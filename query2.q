t:tables[]
files@:where files like"[0-9]*"
t@:where not t like "con_*"
tables[]
select from tables[]
key `:/home
t@where t like "*su*"
tables[] where tables[] like "*su*"
cols dfzq_sur
f:exec factor from select distinct factor from factor_ic
f@where f like "dfzq_sur_no_seasonal_shift"
select from factor_qr_10 where factor=`dfzq_sur_no_seasonal_shift,index=`000000.SH,category=`all
tables[]@where tables[] like "*neu*"
tables[]
select from cov_dfzq_sur
select from factor_ic_chenxu
select from DW_ROE_Enhance_20180803_neutral
select from factor_pre_000000.SH where date.year=2018
cols factor_pre_000000.SH
select from dfzq_sue_with_profit_notice where date=2018.07.31

select date,asset,NET_CASH_FLOWS_OPER_ACT_TOGR_yangkun-S_FA_OCFTOOR_macy from df_2018
select date,asset,(YOY_OPER_REV_yangkun*100)-S_FA_YOY_OR_macy from df_2018
select date,asset,CASHRATIO_yangkun,S_FA_CASHTOLIQDEBT_macy from df_2018

\l .
tables[]

select dfzq_con_ep_fy1 from dfzq_con_ep_fy1 where date=2010.04.30
select from factor_ic_chenxu where date>2018.06.01

tables[]
select from gg_con_eps where date in (2010.04.30,2011.04.29,2012.04.27,2013.04.26,2014.04.30,2015.04.30,2016.04.29,2017.04.28,2018.04.27)
select from con_eps where date in (2010.04.30,2011.04.29,2012.04.27,2013.04.26,2014.04.30,2015.04.30,2016.04.29,2017.04.28,2018.04.27)
{[dt]lj[select from con_eps where date=dt;2!select from gg_con_eps where date=dt]}[2018.05.31]
cols dfzq_sue_with_profit_notice
select from dfzq_sue_with_profit_notice where date=2018.01.31

tables[]
select from dfzq_sue_with_profit_notice where i<10
select from DW_ROE_Enhance_20180803 where date=2010.01.29
\l .
tables[]
select from factor_ic_yinh where category=`all

count df
select from df where i < 100
select from df where con_year=2018,i<100,con_date>`$"2018-01-01"
df[`con_date][0]
select from gg_con_eps where date=2018.04.27
df
tables[]
select from p
select from factor_ic_juxy where category=`all,index=`000300.SH

lj[select from con_np where date=2018.08.31;2!select from gg_con_np where date=2018.08.31]

select from dfzq_con_ep_fy1 where date=2018.08.27

select from to_dfzq_sur_10_10
a:1
.Q.qp a 
.Q.qp ic_dfzq_sur
.Q.qp cov_dfzq_sue_no_profit_notice
\l .
tables[]
tables[]@where tables[] like "*sue*"

select from gg_con_roe where date>2018.08.27

select from ic_dfzq_sur
select from cov_dfzq_sur where factor_cov>0

meta cov_neu_all20180831
select from cov_neu_all20180831 where factor=`gg_rpt_num

tables[]@where tables[] like "*factor_neu*"
rentable[`:.;`to_neu_all20180831_10;`to_neu_all20180831_10_no_drop]
\l .
delete `ic_neu_all20180831 from `.
delete  to_neu_all20180831_10 from `.
select  from cov_factor_neu_201808

tables[]
select from con_eps where date>2018.09.12

.z.o in`w32`w64
\p

add:{[x;y]
    z:x+y;
    z:x-y;
    z:x+y;
    z
};

add[3;4]
\f
tables[]
\l d:/db_script/dbmaint.q
allpaths[`:d:/db_fa_dev;`to_manager_return_10]
key `:d:/db_fa_dev
select from `to_manager_return_10

X:1
Y:1
dblog:{[x;y]
    X::x;
    Y::y;
    log_str:raze[[" "sv string`date`second$.z.P]," ",y];
    -1 log_str;
    hlog: hopen hsym `$x;(neg hlog) log_str;
    hclose hlog}

string "sdf"
y:"dfsdfsdf"
x:"d:/tmp.log"
dblog[x;y]

tablename:"tbl";
dbdir:"d:/db_test_partition";

gen_tbl:{[n]
    ([]date:(2016.01.01)+n?150; ti:asc n?24:00:00; sym:n?`ibm`aapl; qty:n?1000)}

gen_tbl:{[n]
    ([]date:(2016.01.01)+n?150; ti:asc n?24:00:00; sym:n?`ibm`aapl; qty:n?1000)}
    
gen_tbl2:{[n]
    ([]qty:n?1000;date:(2016.01.01)+n?150; ti:asc n?24:00:00; sym:n?`ibm`aapl)}

df:gen_tbl[1]
pupserttable["d:/db_test_partition";"df";df;"date";"d:/tmp.log"]
/
dbdir:"d:/db_test_partition";
tablename:"stable";
tbl__:gen_tbl[1];
log_path:"d:/tmp.log";
upserttable[dbdir;tablename;tbl__;log_path]
meta tbl__
type tbl__[`ti][0]
delete_par_table[dbdir;tablename]
\l d:/db_script/dbmaint.q
\
upserttable:{[dbdir;tablename;tbl__;log_path]
    if[is_debug_mode;0N!"-------------------";0N!dbdir;0N!tablename;0N!tbl__;0N!log_path];
    if[1=havetable[dbdir;tablename];
        if[not (`c xasc (meta `$tablename))[;`t]~(`c xasc (meta tbl__))[;`t];
            dblog[log_path;"meta mismatch:",tablename];`:]];    /   check meta
    writepath:hsym[`$dbdir,"/",tablename,"/"];  /splayed table
    .[upsert;(writepath;.Q.en[hsym `$dbdir;] tbl__);{dblog[log_path;"failed to upsert ",tablename,":",x]}];
    system "l ."}

    
upserttable_no_duplicate:{[dbdir;tablename;tbl__;key_cols;log_path]
    if[0=havetable[dbdir;tablename];upserttable[dbdir;tablename;tbl__;log_path];`:];
    kc:`$key_cols;    
    k1:?[hsym `$dbdir,"/",tablename;();0b;(kc)!(kc)];
    k2:?[tbl__;();0b;(kc)!(kc)];
    uk:k2 except k1;
    $[(asc cols uk)~(asc cols tbl__);to_upsert:uk;to_upsert:lj[uk;kc xkey tbl__]];
    upserttable[dbdir;tablename;to_upsert;log_path]}

/
dbdir:"d:/db_test_partition";
tablename:"dfs";
tbl__:gen_tbl[10];
par_col:"date";
log_path:"d:/tmp.log";
pupserttable[dbdir;tablename;tbl__;par_col;log_path]
\
pupserttable:{[dbdir;tablename;tbl__;par_col;log_path]
    pars:?[tbl__;();();`$par_col];
    pars:distinct asc pars;
    i:0;n:count pars;
    while[i<n;            
        towrite:?[tbl__;enlist(=;`$par_col;pars[i]);0b;()];
        par_tablename:raze string(pars[i]),"/",tablename;
        upserttable[dbdir;par_tablename;![towrite;();0b;enlist`$par_col];log_path];
        i+:1];
    .Q.chk hsym `$dbdir}

tables[]
\l d:/db_script/test_load_two_functions.q
\l d:/db_script/dbmaint.q