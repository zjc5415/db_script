/ kdb+ splayed database maintenance
//https://github.com/KxSystems/kdb/blob/master/utils/dbmaint.md

WIN:.z.o in`w32`w64
pth:{p:$[10h=type x;x;string x];if[WIN;p[where"/"=p]:"\\"];(":"=first p)_ p}
cpy:{system$[WIN;"copy /v /z ";"cp "],pth[x]," ",pth y}
del:{system$[WIN;"del ";"rm "],pth x}
ren:{system$[WIN;"move ";"mv "],pth[x]," ",pth y}
here:{hsym`$system$[WIN;"cd";"pwd"]}
nullof: {[item] enlist[item] 1}
log_path:"d:/db.log"
//x:"d:/db/dblib.log";y:string "output me"
dblog:{[x;y]log_str:raze[[" "sv string`date`second$.z.P]," ",y];-1 log_str;hlog: hopen hsym `$x;(neg hlog) log_str;hclose hlog;}

//newtable[dbdir;tablename;tbl]
//dbdir:"d:/db";tablename:"quote";tbl:.Q.en[`:.]([]date:0#0Nt;sym:0#`;price:0#0n;size:0#0)  
newtable:{[dbdir;tablename;tbl]   
    hsym[`$dbdir,"/",tablename,"/"] set tbl;
    system "l .";   //reload
};
//upserttable[dbdir;tablename;tbl]
//todo: check disk db exist
//todo: check meta tbl same as disk db
upserttable:{[dbdir;tablename;tbl]
    hsym[`$dbdir,"/",tablename,"/"] upsert .Q.en[hsym `$dbdir;] tbl;
    system "l .";   //reload
};
//update part of a row, 
/ upsertdict:{[dbdir;tablename;val_dict]
/     db:hsym[`$dbdir,"/",tablename];
/     0#get db
/     (flip 0#get `:d:/db/t)
/ }

//todo: checkt type of val same as entry in disk db
//todo: issues about lock, test multithread modify
updateentry:{[dbdir;tablename;row;col;val;log_path]
    db:hsym[`$dbdir,"/",tablename];
    db_col:hsym[`$dbdir,"/",tablename,"/",col];   
    col_d:hsym[`$dbdir,"/",tablename,"/",".d"];
    if[0=count exec i from db;dblog[log_path;"updateentry failed, table[",tablename,"] is empty"];:-1];
    if[(0=count key db_col) or (not (`$col) in get col_d);dblog[log_path;"updateentry failed, ",(string db_col)," not exist"];:-1];       
    
    col_type:value "type exec first ",col," from ",tablename;
    if[col_type<>type first val;dblog[log_path;"updateentry failed,type"];:-1];    
    db_col set @[get db_col;row;:;val];        
    system "l .";   //reload
    :0;
};

addcol:{[dbdir;tablename;col;default_value;log_path]
    db_path:hsym[`$dbdir,"/",tablename];
    col_path: hsym[`$dbdir,"/",tablename,"/",col];
    col_d:hsym[`$dbdir,"/",tablename,"/",".d"];
    
    if[(`$col) in get[col_d];dblog[log_path;"add col[",col,"] to table[",tablename,"] failed, table exist"];:`];
    col_path set (1+exec max i from db_path)#default_value;
    col_d set get[col_d] union `$col;
    dblog[log_path;"add col[",col,"] to table[",tablename,"]"];
    system "l .";   //reload
};

//todo: check if reload is needed after delete
delete_col:{[dbdir;tablename;col;log_path]    
    col_d:hsym[`$dbdir,"/",tablename,"/",".d"];    
    if[not (`$col) in get[col_d];dblog[log_path;"delete col[",col,"] from table[",tablename,"] failed, table not exist"];:`];
    del (dbdir,"/",tablename,"/",col);        
    col_d set get[col_d] except `$col;
    dblog[log_path;"delete col[",col,"] from table[",tablename,"]"];
    system "l .";   //reload
};

