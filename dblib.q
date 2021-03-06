/ kdb+ splayed database maintenance
//https://github.com/KxSystems/kdb/blob/master/utils/dbmaint.md

WIN:.z.o in`w32`w64;
pth:{p:$[10h=type x;x;string x];if[WIN;p[where"/"=p]:"\\"];(":"=first p)_ p};
cpy:{system$[WIN;"copy /v /z ";"cp "],pth[x]," ",pth y};
del:{system$[WIN;"del ";"rm "],pth x};
ren:{system$[WIN;"move ";"mv "],pth[x]," ",pth y};
here:{hsym`$system$[WIN;"cd";"pwd"]};
nullof: {[item] enlist[item] 1};
/ log_path:"d:/db.log";
//x:"d:/db/dblib.log";y:string "output me"
dblog:{[x;y]log_str:raze[[" "sv string`date`second$.z.P]," ",y];-1 log_str;hlog: hopen hsym `$x;(neg hlog) log_str;hclose hlog;};
enum:{[dbdir;val]       $[not 10=abs type val;:val;val:`$val];    p:hsym[`$dbdir,"/","sym"];    `sym set$[type key p;get p;0#`];    e:`sym?val;    .[p;();:;sym];    e};
allcols:{[dbdir;tablename]d_path:hsym[`$dbdir,"/",tablename,"/",".d"];get d_path}
havetable:{[dbdir;tablename]    $[count key hsym `$dbdir,"/",tablename;:1;:0];}
//newtable[dbdir;tablename;tbl]
//dbdir:"d:/db";tablename:"quote";tbl:.Q.en[`:.]([]date:0#0Nt;sym:0#`;price:0#0n;size:0#0)  
newtable:{[dbdir;tablename;tbl]hsym[`$dbdir,"/",tablename,"/"] set .Q.en[hsym `$dbdir] tbl;system "l .";};
//upserttable[dbdir;tablename;tbl]
// 支持空表
// 表存在则append，不存在则新建
// 异常情况举例：tbl某列的类型与原有的表不一致则该列append失败，其他列正常更新。
//todo: check disk db exist
//todo: check meta tbl same as disk db
//upserttable:{[dbdir;tablename;tbl]hsym[`$dbdir,"/",tablename,"/"] upsert .Q.en[hsym `$dbdir;] tbl;system "l ."; };
//todo: checkt type of val same as entry in disk db
//todo: issues about lock, test multithread modify
/ updateentry:{[dbdir;tablename;row;col;val;log_path] db:hsym[`$dbdir,"/",tablename];db_col:hsym[`$dbdir,"/",tablename,"/",col];col_d:hsym[`$dbdir,"/",tablename,"/",".d"];    if[0=count exec i from db;dblog[log_path;"updateentry failed, table[",tablename,"] is empty"];:-1];    if[(0=count key db_col) or (not (`$col) in get col_d);dblog[log_path;"updateentry failed, ",(string db_col)," not exist"];:-1];    col_type:value "type exec first ",col," from ",tablename;    if[col_type<>type first val;dblog[log_path;"updateentry failed,type"];:-1];        db_col set @[get db_col;row;:;val];            system "l .";    :0;};
updateentry:{[dbdir;tablename;row;col;val;log_path]    db:hsym[`$dbdir,"/",tablename];    db_col:hsym[`$dbdir,"/",tablename,"/",col];    col_d:hsym[`$dbdir,"/",tablename,"/",".d"];        if[any not row in exec i from db;dblog[log_path;"updateentry failed, row index"];:-1];        if[(0=count key db_col) or (not (`$col) in get col_d);dblog[log_path;"updateentry failed, ",(string db_col)," not exist"];:-1];        col_type:value ("type exec first ",col," from `",string db);        $[col_type=-20h;        [$[(type first "")<>type first val;[dblog[log_path;"updateentry failed,type"];:-1];val:enum[dbdir;val]]];        [if[col_type<>type first val;dblog[log_path;"updateentry failed,type"];:-1]]];            db_col set @[get db_col;row;:;val];    system "l .";    :0;    };
/ addcol:{[dbdir;tablename;col;default_value;log_path]    db_path:hsym[`$dbdir,"/",tablename];    col_path: hsym[`$dbdir,"/",tablename,"/",col];    col_d:hsym[`$dbdir,"/",tablename,"/",".d"];        if[(`$col) in get[col_d];dblog[log_path;"add col[",col,"] to table[",tablename,"] failed, table exist"];:`];    col_path set (1+exec max i from db_path)#default_value;    col_d set get[col_d] union `$col;    dblog[log_path;"add col[",col,"] to table[",tablename,"]"];    system "l .";};
addcol:{[dbdir;tablename;col;default_value;log_path]        db_path:hsym[`$dbdir,"/",tablename];        col_path: hsym[`$dbdir,"/",tablename,"/",col];        d_path:hsym[`$dbdir,"/",tablename,"/",".d"];            if[(`$col) in get[d_path];dblog[log_path;"add col[",col,"] to table[",tablename,"] failed, col exist"];:`];        num:count get hsym[`$ dbdir,"/",tablename,"/",string first allcols[dbdir;tablename]];    default_value:enum[dbdir;default_value];        .[col_path;();:;num#default_value];    @[db_path;`.d;,;`$col];    dblog[log_path;"add col[",col,"] to table[",tablename,"]"];        system "l .";};
//todo: check if reload is needed after delete
delete_col:{[dbdir;tablename;col;log_path]        col_d:hsym[`$dbdir,"/",tablename,"/",".d"];        if[not (`$col) in get[col_d];dblog[log_path;"delete col[",col,"] from table[",tablename,"] failed, table not exist"];:`];    del (dbdir,"/",tablename,"/",col);            col_d set get[col_d] except `$col;dblog[log_path;"delete col[",col,"] from table[",tablename,"]"];    system "l .";  :0 };

// partition就是一个splayedtable
//setattribute[`:d:/db/quote;`code;`p#]     //succeed
setattribute:{[partition;attrcol;attribute] .[{@[x;y;z];1b};(partition;attrcol;attribute);0b]}
//      set the partition attribute (sort the table if required)
/ sortandsetp[`:d:/db/product;`code`dlmonth;log_path]   //succeed
/ sortandsetp[`:d:/db/quote;`contract`date;log_path]    //succeed
sortandsetp:{[dbdir;tablename;sortcols;log_path]        partition:hsym[`$dbdir,"/",tablename];    sortcols:`$sortcols;    parted:setattribute[partition;first sortcols;`p#];     if[not parted;            0N!sortcols;        0N!partition;        sorted:.[{x xasc y;1b};(sortcols;partition);{dblog[log_path;"ERROR - failed to sort table: ",string partition]; 0b}];                if[sorted;parted:setattribute[partition;first sortcols;`p#]]];         $[parted; dblog[log_path;"`p# attribute set successfully"]; dblog[log_path;"ERROR - failed to set attribute"]];};  // 设置按date排序，所有select结果将保持排序
//@[`:d:/db/quote;`date;`s#] //succeed
//@[`:d:/db/product;`contract;`s#] //succeed

upserttable:{[dbdir;tablename;tbl__;log_path]        writepath:hsym[`$dbdir,"/",tablename,"/"];    0N!writepath;       to_upsert:$[0<count key writepath;((0#select from writepath) upsert .Q.en[hsym `$dbdir;] tbl__);.Q.en[hsym `$dbdir;] tbl__]; .[upsert;(writepath;to_upsert);{dblog[log_path;"failed to upsert table: ",x]}];    system "l ."; };
upserttable_no_duplicate:{[dbdir;tablename;tbl__;key_cols;log_path]    if[0=havetable[dbdir;tablename];upserttable[dbdir;tablename;tbl__;log_path];:`];    kc:`$key_cols;    k1:?[hsym `$dbdir,"/",tablename;();0b;(kc)!(kc)];    k2:?[tbl__;();0b;(kc)!(kc)];    uk:k2 except k1;    $[(asc cols uk)~(asc cols tbl__);to_upsert:uk;to_upsert:lj[uk;kc xkey tbl__]];    upserttable[dbdir;tablename;to_upsert;log_path];};
pupserttable:{[dbdir;tablename;tbl__;par_col;log_path]        pars:?[tbl__;();();`$par_col];    pars:distinct asc pars;    i:0;n:count pars;    while[i<n;            towrite:?[tbl__;enlist(=;`$par_col;pars[i]);0b;()];        par_tablename:raze string(pars[i]),"/",tablename;          upserttable[dbdir;par_tablename;![towrite;();0b;enlist`$par_col];log_path];         i+:1;    ];    .Q.chk hsym `$dbdir };  
pupserttable_no_duplication:{[dbdir;tablename;tbl__;par_col;key_cols;log_path]        pars:?[tbl__;();();`$par_col];    pars:distinct asc pars;    i:0;n:count pars;        while[i<n;            towrite:?[tbl__;enlist(=;`$par_col;pars[i]);0b;()];        par_tablename:raze string(pars[i]),"/",tablename;          upserttable_no_duplicate[dbdir;par_tablename;![towrite;();0b;enlist`$par_col];key_cols;log_path];        sortandsetp[dbdir;par_tablename;key_cols;log_path]        i+:1;    ];    .Q.chk hsym `$dbdir };
sortdb:{[partition;sortcols;log_path]    sorted:.[{x xasc y;1b};(sortcols;partition);{dblog[log_path;"ERROR - failed to sort table: ",string partition]; 0b}];            sorted    }

list_dir:{reverse hsym[x],.Q.dd'[hsym x;key hsym x]};

//  db_root_str:"d:/db_fa_dev"
//  tbl_name_str:"to_dfzq_sur_10"
//  delete_par_table[db_root_str;tbl_name_str]
delete_par_table:{[db_root_str;tbl_name_str]    db_root:hsym `$db_root_str;    tbl_name:`$tbl_name_str;        file_list : raze list_dir each allpaths[db_root;tbl_name];    {if[not ()~key x;hdel x]} each file_list;            value ("delete ",tbl_name_str," from `.");    system "l .";    }

//  db_root_str:"d:/db_fa_dev"
//  tbl_name_str:"to_dfzq_sur_10"
//  date_str:"2018.06.29"
//  delete_par_table_by_date[db_root_str;tbl_name_str;date_str]
delete_par_table_by_date:{[db_root_str;tbl_name_str;date_str]    db_root:hsym `$db_root_str;    tbl_name:`$tbl_name_str;      date_sym:`$date_str;    dir_to_delete:` sv (db_root;date_sym;tbl_name);    file_list : raze list_dir each dir_to_delete;    {if[not ()~key x;hdel x]} each file_list;    .Q.chk db_root;    value ("delete ",tbl_name_str," from `.");    system "l .";    }


swin:{[f;w;s] f each { 1_x,y }\[w#s[0];s]}    / utils for sliding windows, fill with s[0]
swin2:{x'/[0^(y-1)('[;]/[(flip;reverse;prev\)])'z]}        /swin2[cor;3;(29 10 54;1 3 9)]

