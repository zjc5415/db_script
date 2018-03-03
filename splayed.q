//test dbmaint.q
`:/db/t/ set .Q.en[`:/db;] ([] s1:`a`b`c; v:10 20 30; s2:`x`y`z)
dbdir:`:d:/db
tabledir:`:d:/db/t
allcols[tabledir]

add1col:{[tabledir;colname;defaultvalue]
add1col[tabledir;`col_added;0f]
delete1col[tabledir;`col_added]
find1col[tabledir;`s2]
rename1col[tabledir;`s1;`col_renamed]


newtable[dbdir;`tablename;tbl]
tbl:.Q.en[`:.]([]date:0#0Nt;sym:0#`;price:0#0n;size:0#0)
`:d:/db/quote set tbl

listcols[dbdir;`quote]
allpaths[dbdir;`quote]

"{[dbdir;table]
 files:key dbdir;
 if[any files like"par.txt";:raze allpaths[;table]each hsym each`$read0(`)sv dbdir,`par.txt];
 files@:where files like"[0-9]*";(`)sv'dbdir,'files,'table}"


add1table[`:/db/quote;`new_table;0#tbl]
add1table
addtable[dbdir;`quote;.Q.en[`:.]([]date:0#0Nt;sym:0#`;price:0#0n;size:0#0)]
tbl


addcol[dbdir;"t";`added_col2;0f]

addcol:{[dbdir;table;colname;defaultvalue] / addcol[`:/data/taq;`trade;`noo;0h]
    if[not validcolname colname;'(`)sv colname,`invalid.colname];
    add1col[;colname;enum[dbdir;defaultvalue]]each allpaths[dbdir;table];
}
allpaths:{[dbdir;table]
 files:key dbdir;
 if[any files like"par.txt";:raze allpaths[;table]each hsym each`$read0(`)sv dbdir,`par.txt];
 files@:where files like"[0-9]*";(`)sv'dbdir,'files,'table
}

files@:where files like"[0-9]*";(`)sv'dbdir,'files,'tbl
(`)sv 'dbdir

tbl:`t
add1table:{[dbdir;tablename;table]
    stdout"adding ",string tablename;
    @[tablename;`;:;.Q.en[dbdir]0#table];
}
@[`added_tbl;`;:;.Q.en[`:/db/new_tbl]0#tbl]


get tabledir

//http://code.kx.com/q4m3/14_Introduction_to_Kdb+/#143-partitioned-tables
`:d:/db/t/ set ([] v1:10 20 30; v2:1.1 2.2 3.3)
`:/db/t2/ upsert ([] v1:10 20 30; v2:1.1 2.2 3.3)
.[`:/db/t3/; (); ,; ([] v1:10 20 30; v2:1.1 2.2 3.3)]

get `:d:/db/t/.d

`:/db/t/ set .Q.en[`:/db;] ([] s1:`a`b`c; v:10 20 30; s2:`x`y`z)
select from `:/db/t
`:/db/t/ upsert .Q.en[`:/db;] ([] s1:`d`e; v:40 50; s2:`u`v)
`:/db/t upsert .Q.en[`:/db;] enlist `s1`v`s2!(`f;60;`t)
`:/db/t upsert .Q.en[`:/db;] flip `s1`v`s2!flip ((`g;70;`r);(`h;80;`s))

//update value on splayed table
`:/db/t1/ set ([] ti:09:30:00 09:31:00; p:101.5 33.5)
`:/db/t1/p set .[get `:/db/t1/p; where 09:31:00=get `:/db/t1/ti; :;42.0]
\l :/db/t1
select from t1

//add new column on splayed table
`:/db/t3/ set ([] ti:09:30:00 09:31:00; p:101.5 33.5)
`:/db/t3/s set (count get `:/db/t3/ti)#`
`:/db/t3/.d set get[`:/db/t3/.d] union `s
select from `:/db/t3