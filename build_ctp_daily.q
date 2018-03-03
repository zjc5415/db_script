//-- CONFIG -------------

/dbdir:`:/home/workspace/q/qtest/kdb_ctp_tick
dbdir:`:d:/db/cta/ctptick

/inputdir:`:/home/workspace/q/qtest/ctp_tick
/inputdir:`:/home/tick_data/ctp_tick_on_aliyun_cleaned
/inputdir:`:/home/tick_data/ctp_tick_on_jishu_cleaned
inputdir:`:d:/tick_20170219_20170227

// the number of bytes to read at once, used by .Q.fsn
chunksize:`int$100*2 xexp 20;

// compression parameters
/ .z.zd:17 2 6

//-- END OF CONFIG ------

// maintain a dictionary of the db partitions which have been written to by the loader
partitions:()!()

// maintain a list of files which have been read
filesread:()

// the column names that we want to read in
//columnnames:`sourcetime`sym`price`size`exchange
columnnames:`date_time`inst`tick_count`ask_price1`bid_price1`ask_volume1`bid_volume1`last_price`vol`turn_over`open_interest`avg_price

// function to print log info
out:{-1(string .z.z)," ",x}

// loader function
loaddata:{[filename;rawdata]
 
 out"Reading in data chunk"; 
 
 // check if we have already read some data from this file
 // if this is the first time we've seen it, then the first row
 // contains the header information, so we want to load it accounting for that
 // in both cases we want to return a table with the same column names
 data:$[filename in filesread; 
     [flip columnames!("ZSJEEIIEIFEE";enlist",")0:rawdata;
          filesread,::filename];
      columnnames xcol ("ZSJEEIIEIFEE";enlist",")0:rawdata];

 out"Read ",(string count data)," rows"; 
 
 // enumerate the table - best to do this once
 out"Enumerating";
 data:.Q.en[dbdir;data];  
 
 // write out data to each date partition
 {[data;date]
 // sub-select the data to write
  towrite:select from data where date=`date$date_time;
  
 // generate the write path
  writepath:.Q.par[dbdir;date;`$"ctp_tick/"];
  
  key_tab:@[{select date_time,inst from get x};writepath;([]date_time:();inst:())];
   $[count key_tab;
  	[dups:exec i from towrite where ([]date_time;inst) in key_tab;];
  	dups:()];
   $[count dups;
     [out"Removed ",(string count dups)," duplicates from ctp_tick table";
     towrite:select from towrite where not i in dups];
     out"No duplicates found"];
  
  out"Writing ",(string count towrite)," rows to ",string writepath; 
 // splay the table - use an error trap
  .[upsert;(writepath;towrite);{out"ERROR - failed to save table: ",x}]; 
  
 // make sure the written path is in the partition dictionary
  partitions[writepath]:date;
 
  }[data] each exec distinct date_time.date from data;
 } 

// set an attribute on a specified column
// return success status
setattribute:{[partition;attrcol;attribute] .[{@[x;y;z];1b};(partition;attrcol;attribute);0b]}

// set the partition attribute (sort the table if required)
sortandsetp:{[partition;sortcols]
 
 out"Sorting and setting `p# attribute in partition ",string partition;
 
 // attempt to apply an attribute.
 // the attribute should be set on the first of the sort cols
 parted:setattribute[partition;first sortcols;`p#];
 
 // if it fails, resort the table and set the attribute
 if[not parted;
    out"Sorting table";
    0N!sortcols;
    0N!partition;
    sorted:.[{x xasc y;1b};(sortcols;partition);{out"ERROR - failed to sort table: ",x; 0b}];
   // check if the table has been sorted
    if[sorted;
       // try to set the attribute again after the sort
       parted:setattribute[partition;first sortcols;`p#]]];
 
 // print the status when done
 $[parted; out"`p# attribute set successfully"; out"ERROR - failed to set attribute"];
 }

finish:{
 // re-sort and set attributes on each partition
 sortandsetp[;`inst`date_time] each key partitions;
 }

// load all the files from a specified directory
loadallfiles:{[dir]
  
 // get the contents of the directory
 filelist:key dir:hsym dir;
 
 // create the full path
 filelist:` sv' dir,'filelist;
 
 // Load each file in chunks
 {out"**** LOADING ",(string x)," ****";
  .Q.fsn[loaddata[x];x;chunksize]} each filelist;
 
 // finish the load
 finish;
 }

loadallfiles[inputdir]


/fpath:`:/home/tick_data/ctp_tick_on_aliyun_cleaned/a1511_cleaned
/fpath:`:/home/tick_data/ctp_tick_on_jishu_cleaned/SR611_cleaned
/fpath:`:/home/bin/ctp_market_daily/quote/CTP20161124

/ fpath: ` sv inputdir,`$"CTP",(ssr[;".";""] string .z.d)
/ file_exist: not ()~key fpath
/ if [file_exist;
/ 	out"**** LOADING ",(string fpath)," ****";
/     .Q.fsn[loaddata[fpath];fpath;chunksize]]; 

\\
h

 
