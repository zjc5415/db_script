//http://code.kx.com/q4m3/14_Introduction_to_Kdb+/
//////////////////////////////////////////////////////////////////////////////append table to disk db
upserttable:{[dbdir;tablename;tbl]
    hsym[`$dbdir,"/",tablename,"/"] upsert .Q.en[hsym `$dbdir;] tbl;
};

batch:{[rt;tn;recs] hsym[`$rt,"/",tn,"/"] upsert .Q.en[hsym `$rt;] recs}
dayrecs:{([] dt:x; ti:asc 100?24:00:00; sym:100?`ibm`aapl; qty:100*1+100?1000)}
appday:batch["d:/db";"trade";]
appday dayrecs 2015.01.01

recs:dayrecs 2015.01.02
`:d:/db/trade/ upsert .Q.en[`:d:/db] recs

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
db_col set @[get db_col; 0 1;:;2.0 1.0];    
get db_col
path set @[value path;where 0=(value path) mod 2;neg]



////////////////////////////////////////////////////////////////////////////////add/delete column
dbdir:"d:/db";tablename:"t";row:0;col:"p";val:100.0;log_path:"d:/db.log";tbl:.Q.en[`:.]([]date:0#0Nt;sym:0#`;price:0#0n;size:0#0)  
`:d:/db/t/ set ([] ti:09:30:00 09:31:00; p:101.5 33.5)
addcol[dbdir;tablename;"pp";0.0;log_path]
delete_col[dbdir;tablename;"pp";log_path]
get hsym `$dbdir,"/",tablename
updateentry[dbdir;tablename;0 1;col;2.0 2.0;log_path]


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

