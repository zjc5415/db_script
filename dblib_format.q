/
同一个db目录下，splayed和partitioned table不能重名
\
/ kdb+ splayed database maintenance
//https://github.com/KxSystems/kdb/blob/master/utils/dbmaint.md

WIN:.z.o in`w32`w64
pth:{p:$[10h=type x;x;string x];if[WIN;p[where"/"=p]:"\\"];(":"=first p)_ p}
cpy:{system$[WIN;"copy /v /z ";"cp "],pth[x]," ",pth y}
del:{system$[WIN;"del ";"rm "],pth x}
ren:{system$[WIN;"move ";"mv "],pth[x]," ",pth y}
here:{hsym`$system$[WIN;"cd";"pwd"]}
nullof: {[item] enlist[item] 1}
is_debug_mode:1b

system "l d:/db_script/dbmaint.q"

/gen_tbl[10]
gen_tbl:{[n]
    ([]date:(2016.01.01)+n?150; ti:asc n?24:00:00; sym:n?`ibm`aapl; qty:n?1000)}
    
//x:"d:/tmp.log";y:"output me"
dblog:{[x;y]
    log_str:raze[[" "sv string`date`second$.z.P]," ",y];
    -1 log_str;
    hlog: hopen hsym `$x;(neg hlog) log_str;
    hclose hlog}

enum:{[dbdir;val]
    $[not 10=abs type val;:val;val:`$val];
    p:hsym[`$dbdir,"/","sym"];
    `sym set$[type key p;get p;0#`];
    e:`sym?val;
    .[p;();:;sym];
    e}
    
allcols:{[dbdir;tablename]
    d_path:hsym[`$dbdir,"/",tablename,"/",".d"];
    get d_path}
    
havetable:{[dbdir;tablename]
    $[count key hsym `$dbdir,"/",tablename;:1;:0]}

//newtable[dbdir;tablename;tbl]
//dbdir:"d:/db";tablename:"quote";tbl:.Q.en[`:.]([]date:0#0Nt;sym:0#`;price:0#0n;size:0#0)  
newtable:{[dbdir;tablename;tbl]hsym[`$dbdir,"/",tablename,"/"] set .Q.en[hsym `$dbdir] tbl;system "l .";}

/
todo: checkt type of val same as entry in disk db
todo: issues about lock, test multithread modify
dbdir:"/home/quser/db_pa/2018.07.02"
log_path:"/home/quser/db.log"
tablename:"p"
row:0,1
val:1.0,1.0
col:"bonus_cash"    
updateentry[dbdir;tablename;row;col;val;log_path]   
\
updateentry:{[dbdir;tablename;row;col;val;log_path]
    db:hsym[`$dbdir,"/",tablename];
    db_col:hsym[`$dbdir,"/",tablename,"/",col];
    col_d:hsym[`$dbdir,"/",tablename,"/",".d"];
    if[any not row in exec i from db;dblog[log_path;"updateentry failed, row index"];:-1];
    if[(0=count key db_col) or (not (`$col) in get col_d);dblog[log_path;"updateentry failed, ",(string db_col)," not exist"];:-1];
    col_type:value ("type exec first ",col," from `",string db);
    $[col_type=-20h;        
        [$[(type first "")<>type first val;[dblog[log_path;"updateentry failed,type"];:-1];val:enum[dbdir;val]]];
        [if[col_type<>type first val;dblog[log_path;"updateentry failed,type"];:-1]]];
    db_col set @[get db_col;row;:;val];
    system "l .";:0}

addcol:{[dbdir;tablename;col;default_value;log_path]
    db_path:hsym[`$dbdir,"/",tablename];
    col_path: hsym[`$dbdir,"/",tablename,"/",col];
    d_path:hsym[`$dbdir,"/",tablename,"/",".d"];
    if[(`$col) in get[d_path];
        dblog[log_path;"add col[",col,"] to table[",tablename,"] failed, col exist"];:`];
    num:count get hsym[`$ dbdir,"/",tablename,"/",string first allcols[dbdir;tablename]];
    default_value:enum[dbdir;default_value];
    .[col_path;();:;num#default_value];
    @[db_path;`.d;,;`$col];
    dblog[log_path;"add col[",col,"] to table[",tablename,"] done"];
    system "l ."}

//todo: check if reload is needed after delete
delete_col:{[dbdir;tablename;col;log_path]
    col_d:hsym[`$dbdir,"/",tablename,"/",".d"];
    if[not (`$col) in get[col_d];
        dblog[log_path;"delete col[",col,"] from table[",tablename,"] failed, table not exist"];:`];
    del (dbdir,"/",tablename,"/",col);
    col_d set get[col_d] except `$col;
    dblog[log_path;"delete col[",col,"] from table[",tablename,"] done"];
    system "l .";:0}

// partition就是一个splayedtable
//setattribute[`:d:/db/quote;`code;`p#]     //succeed
setattribute:{[partition;attrcol;attribute] .[{@[x;y;z];1b};(partition;attrcol;attribute);0b]}

/
set the partition attribute (sort the table if required)
sortandsetp[`:d:/db/product;`code`dlmonth;log_path]   //succeed
sortandsetp[`:d:/db/quote;`contract`date;log_path]    //succeed
@[`:d:/db/quote;`date;`s#] //succeed
@[`:d:/db/product;`contract;`s#] //succeed
\
sortandsetp:{[dbdir;tablename;sortcols;log_path]        
    partition:hsym[`$dbdir,"/",tablename];
    sortcols:`$sortcols;
    parted:setattribute[partition;first sortcols;`p#];
    if[not parted;
        0N!sortcols;0N!partition;        
        sorted:.[{x xasc y;1b};(sortcols;partition);{dblog[log_path;"failed to sort table: ",string partition]; 0b}];
        if[sorted;parted:setattribute[partition;first sortcols;`p#]]];
    if[not parted;dblog[log_path;"failed to set part attribute for ",tablename]]}  // 设置按date排序，所有select结果将保持排序

/
支持空表
表存在则append，不存在则新建
异常情况举例：tbl某列的类型与原有的表不一致则该列append失败，其他列正常更新,通过meta检查类型
dbdir:"d:/db_test_partition";
table_path:"2016.01.16/ptable";
table_path:"stable";
tbl__:gen_tbl[1];
tbl__:select sym,ti,qty from gen_tbl[1];
log_path:"d:/tmp.log";
upserttable[dbdir;table_path;tbl__;log_path]

dbdir:X;table_path:Y;log_path:V;
\
upserttable:{[dbdir;table_path;tbl__;log_path]
    //注意table_path包含partition的情况，如2016.01.10/ptable,
    //只支持splayed table/partitioned table,且后者tbl__不包含partition那一列
    if[is_debug_mode;
        X::dbdir;Y::table_path;V::log_path;
        0N!"***********upserttable***************";0N!dbdir;0N!table_path;0N!show 2#tbl__;0N!log_path
    ];
    system "l .";   /从磁盘加载数据，保证meta能成功
    table_sym:`$last "/" vs table_path;
    table_type:value ".Q.qp ",string table_sym;
    if[0~table_type;dblog[log_path;"failed to upsert to table_type 0",table_path]];
    if[table_sym in tables[];         //类型检查，注意列的顺序没有关系
        type_new:select c,t from (`c xasc meta tbl__);
        type_old:select c,t from (`c xasc meta table_sym);
        if[1b~table_type;
            type_old:delete from type_old where c=`date];  //partitioned table需要删除date列，todo:按其他字段分区是
        if[not type_new~type_old;
            dblog[log_path;"meta mismatch:",table_path];:`]];
    writepath:hsym `$dbdir,"/",table_path,"/";
    .[upsert;(writepath;.Q.en[hsym `$dbdir;] tbl__);{dblog[log_path;"failed to upsert ",table_path,":",x];:`}];
    dblog[log_path;"upsert to ",table_path," done"];
    system "l ."}

/
dbdir:"d:/db_test_partition";
tablename:"ptable";
tbl__:gen_tbl[10];
par_col:"date";
log_path:"d:/tmp.log";
pupserttable[dbdir;tablename;tbl__;par_col;log_path]
\     
pupserttable:{[dbdir;tablename;tbl__;par_col;log_path]
    if[is_debug_mode;0N!"***********pupserttable***************";0N!dbdir;0N!tablename;0N!show 2#tbl__;0N!par_col;0N!log_path];
    pars:?[tbl__;();();`$par_col];
    pars:distinct asc pars;
    i:0;n:count pars;
    while[i<n;            
        towrite:?[tbl__;enlist(=;`$par_col;pars[i]);0b;()];
        par_tablename:raze string(pars[i]),"/",tablename;
        upserttable[dbdir;par_tablename;![towrite;();0b;enlist`$par_col];log_path];
        i+:1];
    .Q.chk hsym `$dbdir;
    system "l ."}

/
dbdir:"d:/db_test_partition";
tablename:"ptable";
tbl__:gen_tbl[10];
key_cols:enlist("sym");
log_path:"d:/tmp.log";
upserttable_no_duplicate[dbdir;tablename;tbl__;par_col;log_path]
tablename:X;tbl__:Y;key_cols:Z;dbdir:W;log_path:V;
\     
upserttable_no_duplicate:{[dbdir;tablename;tbl__;key_cols;log_path]
    if[is_debug_mode;
        X::tablename;Y::tbl__;Z::key_cols;W::dbdir;V::log_path;
        0N!"***********upserttable_no_duplicate***************";0N!dbdir;0N!tablename;0N!show 2#tbl__;0N!key_cols;0N!log_path];
    if[0=havetable[dbdir;tablename];upserttable[dbdir;tablename;tbl__;log_path];:`];
    kc:`$key_cols;    
    k1:?[hsym `$dbdir,"/",tablename;();0b;(kc)!(kc)];
    k2:?[tbl__;();0b;(kc)!(kc)];
    uk:k2 except k1;
    $[(asc cols uk)~(asc cols tbl__);to_upsert:uk;to_upsert:lj[uk;kc xkey tbl__]];
    upserttable[dbdir;tablename;to_upsert;log_path];}
    
pupserttable_no_duplication:{[dbdir;tablename;tbl__;par_col;key_cols;log_path]
    if[is_debug_mode;0N!"***********pupserttable_no_duplication***************";0N!dbdir;0N!tablename;0N!show 2#tbl__;0N!par_col;0N!key_cols;0N!log_path];
    pars:?[tbl__;();();`$par_col];
    pars:distinct asc pars;
    i:0;n:count pars;
    while[i<n;
        towrite:?[tbl__;enlist(=;`$par_col;pars[i]);0b;()];
        par_tablename:raze string(pars[i]),"/",tablename;
        upserttable_no_duplicate[dbdir;par_tablename;![towrite;();0b;enlist`$par_col];key_cols;log_path];
        sortandsetp[dbdir;par_tablename;key_cols;log_path];
        i+:1];
    .Q.chk hsym `$dbdir}
    
sortdb:{[partition;sortcols;log_path]    
    .[{x xasc y;1b};(sortcols;partition);{dblog[log_path;"failedd to sort table: ",string partition]; 0b}]}

list_dir:{reverse hsym[x],.Q.dd'[hsym x;key hsym x]}

//  db_root_str:"d:/db_fa_dev"
//  tbl_name_str:"to_dfzq_sur_10"
//  delete_par_table[db_root_str;tbl_name_str]
delete_par_table:{[db_root_str;tbl_name_str]    db_root:hsym `$db_root_str;    tbl_name:`$tbl_name_str;        file_list : raze list_dir each allpaths[db_root;tbl_name];    {if[not ()~key x;hdel x]} each file_list;            value ("delete ",tbl_name_str," from `.");    system "l .";    }

// todo
//修复wind发行日大于交割日的合约
//to_update:select i,contract_issuedate:(lastdelivery_date.date-365)+lastdelivery_date.time from product where contract_issuedate>lastdelivery_date
//updateentry[dbdir;"product";to_update`x;"contract_issuedate";to_update`contract_issuedate;log_path]

//修复wind改名合约
/ updateentry[dbdir;"product";exec i from product where code=`WS;"code";"WH";log_path]
/ updateentry[dbdir;"product";exec i from product where code=`RO;"code";"OI";log_path];
/ updateentry[dbdir;"product";exec i from product where code=`ER;"code";"RI";log_path]
/ updateentry[dbdir;"product";exec i from product where code=`ME;"code";"MA";log_path]
/ updateentry[dbdir;"product";exec i from product where code=`TC;"code";"ZC";log_path]
/ updateentry[dbdir;"product";exec i from product where code=`WT;"code";"PM";log_path]

/ updateentry[dbdir;"quote";exec i from quote where code=`WS;"code";"WH";log_path]
/ updateentry[dbdir;"quote";exec i from quote where code=`RO;"code";"OI";log_path];
/ updateentry[dbdir;"quote";exec i from quote where code=`ER;"code";"RI";log_path]
/ updateentry[dbdir;"quote";exec i from quote where code=`ME;"code";"MA";log_path]
/ updateentry[dbdir;"quote";exec i from quote where code=`TC;"code";"ZC";log_path]
/ updateentry[dbdir;"quote";exec i from quote where code=`WT;"code";"PM";log_path]

/ updateentry[dbdir;"warehouse_receipt";exec i from warehouse_receipt where code=`WS;"code";"WH";log_path];
/ updateentry[dbdir;"warehouse_receipt";exec i from warehouse_receipt where code=`RO;"code";"OI";log_path];
/ updateentry[dbdir;"warehouse_receipt";exec i from warehouse_receipt where code=`ER;"code";"RI";log_path];
/ updateentry[dbdir;"warehouse_receipt";exec i from warehouse_receipt where code=`ME;"code";"MA";log_path];
/ updateentry[dbdir;"warehouse_receipt";exec i from warehouse_receipt where code=`TC;"code";"ZC";log_path];
/ updateentry[dbdir;"warehouse_receipt";exec i from warehouse_receipt where code=`WT;"code";"PM";log_path];

// 行情表修复
/ .quote.refine:{    //remove null settle, ffill close for quote
/     :raze {t:select from quote where contract=x,not null settle;t[`close]:fills t[`close];:t }each distinct exec contract from quote;
/ };
/ q:.quote.refine[]
/ upserttable["d:/db";"q";q]
/ \l .
