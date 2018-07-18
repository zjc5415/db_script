//http://code.kx.com/q4m3/14_Introduction_to_Kdb+/
/ //////////////////////////////////////////////////////////////////////////////append table to disk db
/ reordercols0:{[tabledir;neworder]
/  if[not((count ac)=count neworder)or all neworder in ac:allcols tabledir;'`order];
/  stdout"reordering columns in `",string tabledir;
/  @[tabledir;`.d;:;neworder]}
/ 
/ rename_col:{[tabledir;oldname;newname]
/  if[(oldname in ac)and not newname in ac:allcols tabledir;
/   stdout"renaming ",(string oldname)," to ",(string newname)," in `",string tabledir;
/   .os.ren[` sv tabledir,oldname;` sv tabledir,newname];@[tabledir;`.d;:;.[ac;where ac=oldname;:;newname]]]}
/ 
/ oldname:"sym";newname:"mys"
/ rename_col:{[dbdir;tablename;oldname;newname]
/     if[((`$oldname) in ac)and not (`$newname) in ac:allcols[dbdir;tablename]];
/ }
tablename:"tbl";
dbdir:"d:/db";
gen_tbl:{[n]
    ([]dt:(2016.01.01)+n?150; ti:asc n?24:00:00; sym:n?`ibm`aapl; qty:n?1000)
};

upserttable:{[dbdir;tablename;tbl__;log_path]    
/     hsym[`$dbdir,"/",tablename,"/"] upsert .Q.en[hsym `$dbdir;] tbl__;
    writepath:hsym[`$dbdir,"/",tablename,"/"];
    0N!writepath;
    .[upsert;(writepath;.Q.en[hsym `$dbdir;] tbl__);{dblog[log_path;"failed to upsert table: ",x]}];
    system "l ."; 
};
test_upserttable:{
    tbl:gen_tbl[100];
    upserttable["d:/db";"tbl1";tbl;log_path];
    tablename:"test"
    tbl__:tbl
};
test_upserttable[]

tablename:X;tbl__:Y;key_cols:Z;dbdir:W;log_path:V;
upserttable_no_duplicate:{[dbdir;tablename;tbl__;key_cols;log_path]
    X::tablename;Y::tbl__;Z::key_cols;W::dbdir;V::log_path;
    if[0=havetable[dbdir;tablename];upserttable[dbdir;tablename;tbl__;log_path];`:];
    kc:`$key_cols;
    k1:?[hsym `$dbdir,"/",tablename;();0b;(kc)!(kc)];
    k2:?[tbl__;();0b;(kc)!(kc)];
    uk:k2 except k1;
    $[(asc cols uk)~(asc cols tbl__);to_upsert:uk;to_upsert:lj[uk;kc xkey tbl__]];
    upserttable[dbdir;tablename;to_upsert;log_path];
};
test_upserttable_no_duplicate:{[n]  // n:record number
    tbl_no:gen_tbl[100];
    key_cols:("dt";"ti");
    upserttable_no_duplicate["d:/db";"tbl_no_dup_";tbl_no;key_cols;log_path];
}
test_upserttable_no_duplicate[100]
count select from tbl_no_dup
\t test_upserttable_no_duplicate[100000] each til 10  // run 1000 times
count select distinct dt,ti from tbl_no_dup


pupserttable:{[dbdir;tablename;tbl__;par_col;log_path]    // 一个db貌似只支持一个类型的分区，如year和date，不能同时在一个db下分区,\l 会提示part错误
    pars:?[tbl__;();();`$par_col];
    pars:distinct asc pars;
    i:0;n:count pars;
    while[i<n;    
        towrite:?[tbl__;enlist(=;`$par_col;pars[i]);0b;()];
        par_tablename:raze string(pars[i]),"/",tablename;  
        upserttable[dbdir;par_tablename;![towrite;();0b;enlist`$par_col];log_path]; //删除par_col，vir col 自动推断，date,year,month,int
        i+:1;
    ];
    .Q.chk hsym `$dbdir     //填充空值
 };  

 test_pupserttable:{
    tbl:gen_tbl[100];
    tbl:update m:dt.year from tbl
    pupserttable["d:/db";"tbl2";tbl;"dt";log_path]; 
    dbdir:"d:/db";
    tablename:"tbl_par_another";
    par_col:"dt";
    log_path:"d:/db.log";
}

guess_virtual_par_col:{[x]
    tp:type(last x);
    $[tp=-14;c:"date";tp=-13;c:"month";tp=-6h;c:"year";c:"int"];
    c
}
$[tp=-14;
guess_virtual_par_col[2016i]
tablename:X;tbl__:Y;key_cols:Z;par_col:W;dbdir:V;log_path:U;
dbdir:"d:/db_cache_wind_rdf"
pupserttable_no_duplication:{[dbdir;tablename;tbl__;par_col;key_cols;log_path]    
    // 一个db貌似只支持一个类型的分区，如year和date，不能同时在一个db下分区,\l 会提示part错误
    // key_cols同时也是sort_cols,且为code,par的形式, par 为date/month/year/int
    // key_cols不包含par_col
    X::tablename;Y::tbl__;Z::key_cols;W::par_col;V::dbdir;U::log_path;
    pars:?[tbl__;();();`$par_col];
    pars:distinct asc pars;
    i:0;n:count pars;    
    while[i<n;    
        towrite:?[tbl__;enlist(=;`$par_col;pars[i]);0b;()];
        par_tablename:raze string(pars[i]),"/",tablename;  
        upserttable_no_duplicate[dbdir;par_tablename;![towrite;();0b;enlist`$par_col];key_cols;log_path]; //删除par_col，vir col 自动推断，date,year,month,int
        sortandsetp[dbdir;par_tablename;key_cols;log_path]
        i+:1;
    ];
    .Q.chk hsym `$dbdir     //填充空值
 }  
test_pupserttable_no_duplication:{
    tbl:gen_tbl[1000];
    pupserttable_no_duplication["d:/db";"trade";tbl;"dt";("sym";"ti");log_path]; 
    dbdir:"d:/db";
    tablename:"tbl";
    par_col:"dt";
    key_cols:enlist "sym";
    log_path:"d:/db.log";
};
delete tbl from `.

delete k1 from `.
disk_tbl:select from (hsym `$dbdir,"/",tablename)
select from tbl
?[disk_tbl;();0b;(kc)!(kc)]
parse "select sym from disk_tbl"
?[disk_tbl;();0b;kc!kc]
parse "select from hsym `$dbdir,\"/\",tablename "
tname 
select from tbl where date=par
\l .
tbl

select from tbl
del_table:{[dbdir;tablename]
    path_str:dbdir,"/",tablename;
    if[1=havetable[dbdir;tablename];del pth[path_str]];
    system "l ."; 
}

del_table["d:/db";"tbl"]
havetable["d:/db";"tbl"]


sortdb:{[partition;sortcols;log_path]
    sorted:.[{x xasc y;1b};(sortcols;partition);{dblog[log_path;"ERROR - failed to sort table: ",string partition]; 0b}];        
    sorted
    }
setattribute[`:d:/db/quote;`code;`p#]

sortdb[`:d:/db_cache_wind_rdf/adjclose;`date`wind_code;log_path]    //ok
setattribute[`:d:/db_cache_wind_rdf/adjclose;`date;`p#]             //ok


\a
select from tbl

batch:{[rt;tn;recs] hsym[`$rt,"/",tn,"/"] upsert .Q.en[hsym `$rt;] recs}
dayrecs:{([] dt:x; ti:asc 100?24:00:00; sym:100?`ibm`aapl; qty:100*1+100?1000)}
appday:batch["d:/db";"trade";]
appday dayrecs 2015.01.01

recs:dayrecs 2015.01.02
`:d:/db/trade/ upsert .Q.en[`:d:/db] recs

select from trade

////////////////////////////////////////////////////////////////////////////////update entry by row,col
//http://code.kx.com/q4m3/14_Introduction_to_Kdb+/                                 14.2.7 Manual Operations on a Splayed Directory
//http://www.kdbfaq.com/kdb-faq/what-is-a-virtual-column.html
//http://code.kx.com/wiki/DotQ/DotQDotind
`:d:/db/t/ set ([] ti:09:30:00 09:31:00; p:101.5 33.5)
//update entry by condition
`:/db/t/p set .[get `:d:/db/t/p; where 09:31:00=get `:d:/db/t/ti; :;42.0]
//update entry by virtual index
`:/db/t/p set .[get `:d:/db/t/p; where 0= exec i from  `:d:/db/t; :;44.0]

updateentry:{[dbdir;tablename;row;col;val]  
    db:hsym[`$dbdir,"/",tablename];
    db_col:hsym[`$dbdir,"/",tablename,"/",col];    
    db_col set .[get db_col; where 0=exec i from db;:;val];    
};
//dbdir:"d:/db";tablename:"t";row:0;col:"p";val:100.0;
updateentry[dbdir;tablename;row;col;val]
get `:d:/db/t
db_col:`:d:/db/t/p
db_col set @[get db_col; 0 1;:;100.0];    
get db_col
path set @[value path;where 0=(value path) mod 2;neg]

newtable[dbdir;"new_talbe";.Q.en[`:.]([]date:0#0Nt;sym:0#`;price:0#0n;size:0#0)]

havetable:{[dbdir;tablename]    $[count key hsym `$dbdir,"/",tablename;:1;:0];}
havetable[dbdir;"quote"]

$[count key `:d:/db/duct;0N!1;0N!0]
////////////////////////////////////////////////////////////////////////////////add/delete column
dbdir:"d:/db";tablename:"t";row:0;col:"p";val:100.0;log_path:"d:/db.log";tbl:.Q.en[`:.]([]date:0#0Nt;sym:0#`;price:0#0n;size:0#0)  
`:d:/db/t/ set .Q.en[`:d:/db]([] ti:09:30:00 09:31:00; p:101.5 33.5; sym:`ag`au)

addcol[dbdir;"t";"new_col";"col0";log_path]
delete_col[dbdir;"t";"pp";log_path]
updateentry[dbdir;"t";0;"p";6.0;log_path]                   //update one float
updateentry[dbdir;"t";0 1;"p";7.0;log_path]                 //update two float
updateentry[dbdir;"t";0 1;"p";1.0 2.0;log_path]             //update two float
updateentry[dbdir;"t";0;"new_col";"tow";log_path]        //update one symbol
updateentry[dbdir;"t";0 1;"new_col";"tow";log_path]        //update two symbol

get `:d:/db/t/p
get `:d:/db/t/new_col
get `:d:/db/t

get `:d:/db/t
@[get `:d:/db/t/p;0;:;0.0]      //succeed
@[get `:d:/db/t/sym;0;:;`A]     //`A need enum
`:d:/db/t/p set @[get `:d:/db/t/p;();:;1.0]     //failed
`:d:/db/t/p set .[get `:d:/db/t/p;();:;1.0]     //succeed
.[`:d:/db/t/p;();:;0.0] //succeed   修改一列
@[db_path;`.d;,;`$col];     //succeed append一列
@[`:d:/db/t;`p;:;1.0 2.0]   //succeed 修改整列
@[`:d:/db/t;`p;:;1.0]   //succeed 修改整列
.[`:d/db/t/p;0;:;5.0]   //failed 
@[`:d/db/t/p;0;:;5.0]   //failed
`:d:/db/t/sym set .[get `:d:/db/t/sym;();:;`au] //succeed


val:"b"
enum:{[dbdir;val]        
    $[not 10=abs type val;:val;val:`$val];
    p:hsym[`$dbdir,"/","sym"];
    `sym set$[type key p;get p;0#`];
    e:`sym?val;
    .[p;();:;sym];
    e
}
enum["d:/db";"c"]           //succeed

allcols:{[dbdir;tablename]d_path:hsym[`$dbdir,"/",tablename,"/",".d"];get d_path}
allcols[dbdir;"t"]

get `:d:/db/sym
get `:d:/db/t/.d
get `:d:/db/t
`:d:/db/t/.d set `ti`p`sym
`:d:/db/t/.d set `ti`p`sym`new_col4`new_col6    //手动恢复.d文件，提前是记得列名字
dbdir:"d:/db";
default_value:"new_sym4";col:"new_col6";
addcol[dbdir;tablename;col;default_value;log_path]
addcol:{[dbdir;tablename;col;default_value;log_path]    
    db_path:hsym[`$dbdir,"/",tablename];    
    col_path: hsym[`$dbdir,"/",tablename,"/",col];    
    d_path:hsym[`$dbdir,"/",tablename,"/",".d"];        
    if[(`$col) in get[d_path];dblog[log_path;"add col[",col,"] to table[",tablename,"] failed, col exist"];:`];    
    num:count get hsym[`$ dbdir,"/",tablename,"/",string first allcols[dbdir;tablename]];
    default_value:enum[dbdir;default_value];    
    .[col_path;();:;num#default_value];
    @[db_path;`.d;,;`$col];
    dblog[log_path;"add col[",col,"] to table[",tablename,"]"];    
    system "l .";
};

addcol[dbdir;tablename;"pp";0.0;log_path]
addcol[dbdir;tablename;"s";"A";log_path]
get `:d:/db/t

tablename:"prod";col:0 1;col:"product";val:"A"
updateentry[dbdir;"prod";0 1;"product";"A";log_path]
////////////////////////////////////////////////////////////////////////////////.Q.ind
`:/db/2015.01.01/t/ set ([] c1:1 2 3; c2:1.1 2.2 3.3)
`:/db/2015.01.03/t/ set ([] c1:4 5; c2:4.4 5.5)
\l /db
select from t where date within 2015.01.01 2015.01.03

.Q.ind[t;1 3]
.Q.ind[t;enlist 2]

//test using prod
prod:product_info_t

upserttable[dbdir;"prod";prod]
get `:d:/db/prod
delete_col[dbdir;"prod";"change_limit";log_path]
addcol[dbdir;"prod";"change_limit";0.0;log_path]
c:count get `:d:/db/prod
updateentry[dbdir;"prod";til c ;"change_limit";c#6.0;log_path]

row:0;col:"new_col";val:"A"
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
    system "l .";
    :0;
    };

    value type exec first sym from t    
    select sym from t
    select 
    get `:d:/db/t
    get `:d:/db/t/new_col
    get `:d:/db/t/sym
    
col:"p"
tbl
select from db

any not row in exec i from tbl

type exec first sym from `:d:/db/t
type exec new_col from `:d:/db/t

type `
prod:get `:d:/db/prod
meta prod
type (exec first product from `:d:/db/prod)
.Q.en val
 `:d:/db/prod
symlist:get`:d:/db/sym;


//test sort column and set atrribute, sort by date, part by code
@[`:d:/db/product;`contract;`s#]     //succeed
meta product

`:d:/db/q/ set .Q.en[`:d:/db]([] ti:09:30:00 09:31:00 09:31:00; p:101.5 33.5 35.0; sym:`ag`au`a)
\l .
select from q
updateentry[dbdir;"q";0;"p";1.0;log_path]
meta get `:d:/db/q
`p xasc `:d:/db/q
`ti xasc `:d:/db/q
meta `:d:/db/q

tt:`ti`p xasc select from q
@[tt;`ti`p;`s#]
meta tt


fn1col:{[tabledir;col;fn]
 if[col in allcols tabledir;
  oldattr:-2!oldvalue:get p:tabledir,col;
  newattr:-2!newvalue:fn oldvalue;		
  if[$[not oldattr~newattr;1b;not oldvalue~newvalue];
   stdout"resaving column ",(string col)," (type ",(string type newvalue),") in `",string tabledir;
   oldvalue:0;
   .[(`)sv p;();:;newvalue]]
   ]
   }

// partition就是一个splayedtable
//setattribute[`:d:/db/quote;`code;`p#]     //succeed
setattribute:{[partition;attrcol;attribute] .[{@[x;y;z];1b};(partition;attrcol;attribute);0b]}
//      set the partition attribute (sort the table if required)

sortdb:{[partition;sortcols;log_path]
    sorted:.[{x xasc y;1b};(sortcols;partition);{dblog[log_path;"ERROR - failed to sort table: ",string partition]; 0b}];        
    sorted
}

/ sortandsetp["d:/db";"warehouse_receipt";("code";"date");log_path]
/ sortandsetp:{[dbdir;tablename;sortcols;log_path]    
/     partition:hsym[`$dbdir,"/",tablename];
/     sortcols:`$sortcols;
/     parted:setattribute[partition;first sortcols;`p#]; 
/     if[not parted;    // if it fails, resort the table and set the attribute
/         0N!sortcols;
/         0N!partition;
/         sorted:.[{x xasc y;1b};(sortcols;partition);{dblog[log_path;"ERROR - failed to sort table: ",string partition]; 0b}];        
/         if[sorted;parted:setattribute[partition;first sortcols;`p#]]];     
/     $[parted; dblog[log_path;"`p# attribute set successfully"]; dblog[log_path;"ERROR - failed to set attribute"]];    
/  }

 dbdir:X;tablename:Y;sortcols:Z;   
 sortcols:enlist "sym"
sortandsetp:{[dbdir;tablename;sortcols;log_path]   
X::dbdir;Y::tablename;Z::sortcols;     
 partition:hsym[`$dbdir,"/",tablename];    
 sortcols:`$sortcols;    
 parted:setattribute[partition;first sortcols;`p#];     
 if[not parted;        
 sorted:.[{x xasc y;1b};(sortcols;partition);{dblog[log_path;"ERROR - failed to sort table: ",string partition]; 0b}];                
 if[sorted;parted:setattribute[partition;first sortcols;`p#]]];         
 $[parted; dblog[log_path;"`p# attribute set successfully"]; 
 dblog[log_path;"ERROR - failed to set attribute"]];
};  // 设置按sym排序，所有select结果将保持排序

sortandsetp["d:/db";"warehouse_receipt";("code";"date");log_path]
sortandsetp["d:/db";"quote";("code";"date");log_path]
sortandsetp["d:/db";"product";("code";"lastdelivery_date");log_path]

`code`lastdelivery_date xasc select from product
select from product

sortdb[`:d:/db/product;`code`lastdelivery_date;log_path]

pupserttable_auto:{[dbdir;tablename;tbl;par_col;log_path]    // 按year/month/date自动分区，没什么鸟用
    suffix:last("." vs par_col);
    pars:?[tbl;();();`$par_col];
    pars:distinct asc pars;
    i:0;n:count pars;
    while[i<n;    
        towrite:?[tbl;enlist(=;`$par_col;pars[i]);0b;()]
        par_tablename:raze suffix,"/",string(pars[i]),"/",tablename;  
        upserttable[dbdir;par_tablename;tbl;log_path];
        i+:1;
    ];
 } 
 
test_pupserttable_auto:{
    tbl:gen_tbl[100];
    pupserttable["d:/db";"tbl_par";tbl;"dt.date";log_path];
    dbdir:"d:/db";
    tablename:"tbl_par";
    par_col:"dt.year";
    log_path:"d:/db.log";
}
test_delete_par_table:{
    list_dir:{reverse hsym[x],.Q.dd'[hsym x;key hsym x]};
    file_list : raze list_dir each allpaths[`:/home/quser/db_csv;`factorneu20180629_mcy];
    {if[not ()~key x;hdel x]} each file_list;
    system "l .";
}
