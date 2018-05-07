/ kdb+ partitioned database maintenance
\d .os
WIN:.z.o in`w32`w64
pth:{p:$[10h=type x;x;string x];if[WIN;p[where"/"=p]:"\\"];(":"=first p)_ p}
cpy:{system$[WIN;"copy /v /z ";"cp "],pth[x]," ",pth y}
del:{system$[WIN;"del ";"rm "],pth x}
ren:{system$[WIN;"move ";"mv "],pth[x]," ",pth y}
here:{hsym`$system$[WIN;"cd";"pwd"]}
\d .



allcols:{[tabledir]get tabledir,`.d}

dbdir:`:d:/db
table:`trade
t:select from tbl
allpaths:{[dbdir;table]
 files:key dbdir;
 if[any files like"par.txt";:raze allpaths[;table]each hsym each`$read0(`)sv dbdir,`par.txt];
 files@:where files like"[0-9]*";
 (`)sv'dbdir,'files,'table
}
ap:allpaths[`:d:/db;`tbl]


enum:{[tabledir;val]if[not 11=abs type val;:val];`sym set$[type key p:` sv tabledir,`sym;get p;0#`];e:`sym?val;.[p;();:;sym];e}

ren1table:{[old;new]stdout"renaming ",(string old)," to ",string new;.os.ren[old;new];}

add1table:{[dbdir;tablename;table]
 stdout"adding ",string tablename;
 @[tablename;`;:;.Q.en[dbdir]table];}

stdout:{-1 raze[" "sv string`date`second$.z.P]," ",x;}
validcolname:{(not x in `i,.Q.res,key`.q)and x = .Q.id x}

//////////////////////////////////////////////////////////////////////////////////////////////////////////
// * public

thisdb:`:. / if functions are to be run within the database instance then use <thisdb> (`:.) as dbdir

tbl

addtable:{[dbdir;tablename;table] / addtable[`:.;`trade;([]price...)]
 add1table[dbdir;;table]each allpaths[dbdir;tablename];}

allpaths[`:d:/db;`tbl]

tbl:gen_tbl[100]
addtable[`:d:/db;`ptbl;tbl]

tbl
cn:`dt
cn
parse "select dt.year,ti from tbl"

?[tbl;();0b;(enlist(`year))!enlist(`dt.year)]
par_col:`dt
par:2016.01.01
parse "select from tbl where dt=par"
?[tbl;enlist(=;par_col;par);0b;()]
count select from ptbl 
\l .

dbdir:"d:/db"
tablename:"tbl"
data:tbl
par_col:`dt
par:2016.01.01

raze dbdir,"/",string(par),"/",tablename

.Q.par[`:d:/db;2016;`trade]
pupserttable_no_duplicate:{[dbdir;tablename;data;par_col;key_cols;log_path]
    pars:?[tbl;();();par_col];
    {[data;par]
        towrite:?[data;enlist(=;par_col;par);0b;()]
        par_dbdir:raze dbdir,"/",string(par),"/",tablename;
        upserttable_no_duplicate[par_dbdir;tablename;data;key_cols;log_path];

  
 // make sure the written path is in the partition dictionary
  partitions[writepath]:date;
 
    }[data] each distinct pars
 } 



parse "exec dt from tbl"
a:?[tbl;();();`dt]
distinct a
parse "exec distinct dt from tbl"
?[tbl;();();?[:`dt]]
