select date,gg_rpt_num from gg_rpt_num
select date,gg_rpt_num from factor
select from gg_rpt_num where date=2011.10.31,stock_code=`000858
select date,stock_code,gg_rpt_num from factor where gg_rpt_num=max gg_rpt_num

10#select from gg_roll

select from gg_dfzq_cov where stock_code=`000001

select from factor where date=2018.05.11,stock_code=`000001

h:hopen `:localhost:10001

h:hopen `:localhost:10001:wj:123456

td:h"exec date from select date from gg_dfzq_cov where stock_code=`000001"
2#td
count td
select from factor where date in td,stock_code=`000001,factor_name=`gg_dfzq_cov

td:exec date from select date from gg_dfzq_cov where stock_code=`000001
tmp

select from factor where date=2010.01.29,stock_code=`002119
distinct select from tmp where date.date in td
delete factor from `.

cols factor
`date xasc select from factor where date>2018.01.01
save `:d:/cta/factor.csv

distinct exec date from factor where date>=2014.01.25

distinct exec date from factor where date>=2015.01.01
ht_comp
3890.50%3366.54

style_cov
cols style_cov /`ExchangeRateSensitivity`Value`Leverage`Growth`Size`Liquidity`ShortTermMomentum`MediumTermMomentum`Volatility
select Volatility from style
style_corr
save `style.csv
load `:d:/db_tmp/style
\v
t:select from tmp
save `t

set 

set `:tmp
\dir
get `:300enhance_fund_list_20180524

yahoo_quote
select deltas from yahoo_quote_unstack
parse "select date,(deltas A)%(prev A) from yahoo_quote_unstack"
select date,(deltas A)%(prev A) from yahoo_quote_unstack
?[`yahoo_quote_unstack;();0b;`date`A!(`date;("%";("-':";`A);(":':";`A)))]

{select date,(deltas x)%(prev x) from yahoo_quote_unstack}[`A]

df_rpt_num_20180511_20180528
h:hopen `::10001:wj:123456
gg_rpt_num_20180511
gg_rpt_num_20180511 : h"select from gg_rpt_num where date=2018.05.11"
lj[gg_rpt_num_20180511;2!df_rpt_num_20180511_20180528]
h"`stock_code xasc select from gg_con_np where date=2018.05.11" 
\v
select from trading_day
traing_day
select from gg_rpt_num where date=2018.05.11
select from gg_rpt_num_unique where date=2018.05.11
rpt_num_comp :{[dt]lj[select from gg_rpt_num where date=dt;2!select from gg_rpt_num_unique where date=dt] }[2018.05.11]
rpt_num_comp:{[dt]lj[select from gg_rpt_num where date=dt;2!select from gg_rpt_num_unique where date=dt] }[2018.04.27]
select date,stock_code,gg_rpt_num-gg_rpt_num_unique from rpt_num_comp

rpt_num_comp:{[dt]lj[select from gg_dfzq_cov where date=dt;2!select from gg_dfzq_cov_unique where date=dt] }[2018.04.27]
select date,stock_code,gg_dfzq_cov-gg_dfzq_cov_unique from rpt_num_comp
2305%1373480

`date xasc select from quote where wind_code=`000001.SZ
select distinct date from gg_rpt_num_unique where date>=2018.01.01

select from gg_rpt_num_unique where date=2018.05.30

select from gg_rpt_num_unique where date=2018.01.02
select from gg_con_np_20180531 where date=2018.01.02

select from gg_con_np
select from gg_con_np_20180531
pricing
industry_label
select from gg_dfzq_cov_unique

pupserttable_no_duplication
sortandsetp

nn : `stock_code xasc select from ht_comp where not null gg_con_eps
nn

select from nn where (gg_con_eps-ht_con_eps)<>0

`stock_code xasc select from ht_comp
`stock_code xasc select from ht_comp where not null gg_con_peg,PEG_exp_m<>gg_con_peg

`stock_code xasc select from ht_comp
`stock_code xasc select from ht_comp where not null gg_con_roe,ROE_exp_m<>gg_con_roe%100

`stock_code xasc select from ht_comp
`stock_code xasc select from ht_comp where not null gg_con_roe,ROE_exp_m<>gg_con_roe%100

select from ht_comp where (stock_code=`000416) or stock_code=`000001

dfb
df
`date`wind_code xasc select from dfb
`date`wind_code xasc select from df

ht_comp
select from ht_comp where date=2018.03.30

pricing:select from pricing
save `:pricing.csv
industry_label:select from industry_label
save `:industry_label.csv

con_roe:select from con_roe
save `:con_roe.csv

mmm_con_deps
select from mmm_con_deps where stock_code=`000001,date<=2017.12.29
select from mmm_con_deps where stock_code=`000001
select from pricing
-10#pricing
-10#industry_label
select eps
mmm_con_deps
\f
tables[]

count select from axioma_group
industry_label
pricing

select from `.

sortdb:{[partition;sortcols;log_path]
    sorted:.[{x xasc y;1b};(sortcols;partition);{dblog[log_path;"ERROR - failed to sort table: ",string partition]; 0b}];        
    sorted
    }
    
setattribute[`:d:/db/quote;`code;`p#]

sortdb[`:d:/db_cache_wind_rdf/2018.01.02;`date`wind_code;log_path]
select from `:d:/db_cache_wind_rdf/2018.01.02/pindex_pool
sortdb[`:d:/db_cache_wind_rdf/2018.01.02/pindex_pool;`wind_code;log_path]
sortdb[`:d:/db_cache_wind_rdf/adjclose;`date`wind_code;log_path]
setattribute[`:d:/db_cache_wind_rdf/adjclose;`date;`p#]
 adjclose
 
 select from adjclose where date=2018.01.02
 select from con_roe where date=2018.01.02
 count select from stock_status
 
 select distinct date  from con_roe where date.month=2018.01m
 tables[]
 select from stock_status
 select from index_pool where date=2018.06.15,index_name=`000852.SH
 select distinct index_name from index_pool
 index_pool
 select from pindex_pool where date=2018.01.03
  ?[hsym `$dbdir,"/",tablename;();0b;(`wind_code`index_name)!(`wind_code`index_name)]
key_cols

sortdb[`:d:/db_cache_wind_rdf/2018.01.02;enlist`wind_code;log_path]
z.zd
 dblog[log_path;"ERROR - failed to set attribute"]
\f
tables[]
select from gg_rpt_num
log_path:"/home/quser/db.lg"
test_upserttable:{
    tbl:gen_tbl[100];
    upserttable["/home/quser/db_gogoal_factor";"tbl1";tbl;log_path];
};
dbdir:"/home/quser/db_gogoal_factor"
tablename:"tbl2"
tbl__:tbl
par_col:"dt"
key_cols:enlist "ti"
pupserttable_no_duplication:{[dbdir;tablename;tbl__;par_col;key_cols;log_path]    
    // 一个db貌似只支持一个类型的分区，如year和date，不能同时在一个db下分区,\l 会提示part错误
    // key_cols同时也是sort_cols,且为code,par的形式, par 为date/month/year/int
    // key_cols不包含par_col
    X::tablename;Y::tbl__;Z::key_cols;W::par_col;
    pars:?[tbl__;();();`$par_col];
    pars:distinct asc pars;
    i:0;n:count pars;    
    while[i<n;    
        towrite:?[tbl__;enlist(=;`$par_col;pars[i]);0b;()];
        par_tablename:raze string(pars[i]),"/",tablename;  
/         upserttable_no_duplicate_par_[dbdir;par_tablename;![towrite;();0b;enlist`$par_col];key_cols;pars[i];log_path]; //删除par_col，vir col 自动推断，date,year,month,int
        upserttable_no_duplicate[dbdir;par_tablename;![towrite;();0b;enlist`$par_col];key_cols;log_path]; //删除par_col，vir col 自动推断，date,year,month,int
        sortandsetp[dbdir;par_tablename;key_cols;log_path]
        i+:1;
    ];
    .Q.chk hsym `$dbdir     //填充空值
 }  
 
 upserttable_no_duplicate:{[dbdir;tablename;tbl__;key_cols;log_path]
    X::tablename;Y::tbl__;Z::key_cols;
    if[0=havetable[dbdir;tablename];upserttable[dbdir;tablename;tbl__;log_path];`:];
    kc:`$key_cols;
    k1:?[hsym `$dbdir,"/",tablename;();0b;(kc)!(kc)];
    k2:?[tbl__;();0b;(kc)!(kc)];
    uk:k2 except k1;
    $[(cols uk)~(cols tbl__);to_upsert:uk;to_upsert:lj[uk;kc xkey tbl__]];
    upserttable[dbdir;tablename;to_upsert;log_path];
};

(cols uk)~(cols tbl__)
(`a`b)~(`a`b)

count select from dfzq_con_ep_fy1 where null dfzq_con_ep_fy1
tables[]

tbl:select from index_pool where date=last date
count tbl

tbl
.z.zd:(17;2;6)

.[upsert;(writepath;.Q.en[hsym `$dbdir;] tbl__);{dblog[log_path;"failed to upsert table ",writepath]}];

.[upsert;(`:d:/db_cache_wind_rdf/test/;.Q.en[`:d:/db_cache_wind_rdf] tbl）;]
`:d:/db_tmp/test/ upsert .Q.en[`:d:/db_tmp] tbl
`:d:/db_cache_wind_rdf/t/ set ([] v1:10 20 30; v2:1.1 2.2 3.3)
system "cd"
get `:d:/db_tmp
10#tbl
select distinct index_name from index_pool

pupserttable_no_duplication

tmp

tablename:X;tbl__:Y;key_cols:Z;par_col:W;
log_path:"/home/quser/db.log"
dbdir:"/home/quser/db_factor_neutral"
pupserttable_no_duplication:{[dbdir;tablename;tbl__;par_col;key_cols;log_path]    
    // 一个db貌似只支持一个类型的分区，如year和date，不能同时在一个db下分区,\l 会提示part错误
    // key_cols同时也是sort_cols,且为code,par的形式, par 为date/month/year/int
    // key_cols不包含par_col
    X::tablename;Y::tbl__;Z::key_cols;W::par_col;
    pars:?[tbl__;();();`$par_col];
    pars:distinct asc pars;
    i:0;n:count pars;    
    while[i<n;    
        towrite:?[tbl__;enlist(=;`$par_col;pars[i]);0b;()];
        par_tablename:raze string(pars[i]),"/",tablename;  
/         upserttable_no_duplicate_par_[dbdir;par_tablename;![towrite;();0b;enlist`$par_col];key_cols;pars[i];log_path]; //删除par_col，vir col 自动推断，date,year,month,int
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

a:![towrite;();0b;enlist`$par_col]
meta a

b:select from con_roe_neutral
meta b

tablename:X;tbl__:Y;key_cols:Z;par_col:W;
dbdir:"d:/db_cache_wind_rdf"
pupserttable_no_duplication:{[dbdir;tablename;tbl__;par_col;key_cols;log_path]    
    // 一个db貌似只支持一个类型的分区，如year和date，不能同时在一个db下分区,\l 会提示part错误
    // key_cols同时也是sort_cols,且为code,par的形式, par 为date/month/year/int
    // key_cols不包含par_col
    X::tablename;Y::tbl__;Z::key_cols;W::par_col;
    pars:?[tbl__;();();`$par_col];
    pars:distinct asc pars;
    i:0;n:count pars;    
    while[i<n;    
        towrite:?[tbl__;enlist(=;`$par_col;pars[i]);0b;()];
        par_tablename:raze string(pars[i]),"/",tablename;  
/         upserttable_no_duplicate_par_[dbdir;par_tablename;![towrite;();0b;enlist`$par_col];key_cols;pars[i];log_path]; //删除par_col，vir col 自动推断，date,year,month,int
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

(asc `a`b)~(asc `b`a)

select date, factor_cov from factor_cov where factor=`con_roe_neutral,factor_group=`000300.SH
select from con_pe_neutral where date.year=2018

system "pwd"
tables[]

from factor_cov
period
count select from gg_con_rating where date.year=2016
tables[]
select from factor_to_5 where i=0
select date,rank_ic from factor_ic where factor=`con_roe,index=`000000.SH,category=`all

a:10#select from factor_cov_1
delete from `.
count select from factor_ic
tables[]
\f
select from gg_dfzq_cov
cols factor_to_5
select from factor_to_5 where date=2018.07.02

select (avg rank_ic)%(dev rank_ic) from select rank_ic from factor_ic where category=`all,period=`1M,index=`000300.SH

select max date from con_roe

 cols factor_cov_1
 
 f:{reverse hsym[x],.Q.dd'[hsym x;key hsym x]}
hdel each f``:/home/quser/db_factor_dev
\pwd
hsym `:/home/quser/db_factor_dev/neutral_return
f`:/home/quser/db_factor_dev
.Q.dd'[`:/home/quser/db_factor_dev]
{x}each tables[]
.Q.qp con_eps
con_eps ? tables[]
0#select from factor_cov
select date, factor_cov,index from factor_cov where date>2010.01.29,factor=`mmm_con_eps,index=`000000.SH
select date,rank_ic from factor_ic where factor=`mmm_con_eps
select from factor_ic where factor=`mmm_con_eps
select date,rank_ic from factor_ic where factor=`mmm_con_roe,index=`000906.SH,category=`all,period=`1M
ic : exec rank_ic from select rank_ic from factor_ic where factor=`mmm_con_roe,index=`000906.SH,category=`all,period=`1M
ir :(avg ic) %dev ic
count ic
tables[]

0#select from factor_qr_5
0#select from factor_to_5

select from XY_DevFactor
select from mmm_con_roe where date=2018.05.31
tables[]
\l .
0#select from factorneu20180629_mcy

 f:{reverse hsym[x],.Q.dd'[hsym x;key hsym x]}
hdel each f``:/home/quser/db_factor_dev
.Q.dd[`:/home/quser/db_csv;`aa]
key hsym[`:/home/quser/db_csv]
allpaths[`:/home/quser/db_csv;`factorneu20180629_mcy]
hdel each f `:/home/quser/db_csv/2007.01.31/factorneu20180629_mcy
 `:/home/quser/db_csv/2007.01.31/factorneu20180629_mcy
.Q.dd' [hsym key `:/home/quser/db_csv/2007.01.31/factorneu20180629_mcy

allpaths[`:/home/quser/db_csv;`factorneu20180629_mcy]
{x} each (f each allpaths)
 fls : raze f each allpaths[`:/home/quser/db_csv;`factorneu20180629_mcy]
 hdel fls[1]
 key fls[2]
{if[not ()~key x;hdel x]} each fls
 \l .
 tables[]
 .Q.qp tfzq_neutral_25_20180629
tables[]

.[upsert;(writepath;.Q.en[hsym `$dbdir;] tbl__);{dblog[log_path;"failed to upsert table: ",x]}];
get `:/home/quser/db_csv/2007.01.31/factorneu20180629_mcy/
get `:/home/quser/db_csv/2007.02.28/factorneu20180629_mcy/
tp:meta tbl__
tp
tables[]
select abs(rank_ic) from factor_ic
0#select from factor_ic
select from factor_ic where factor=`con_roe,index=`000000.SH,category=`all
dev exec rank_ic from select rank_ic from factor_ic where factor=`con_roe,index=`000000.SH,category=`all
tables[]
-1#select from tfzq_neutral_25_20180629_symm_orth
tables[]
select from factor_ic where category=`all

10#np
parse "select date from np"
distinct ?[np;();0b;(enlist`type)!(enlist `type)]
select from np where wind_code=`000001.SZ,stype in (`408002000`408003000)
select from np_yoy where wind_code=`601169.SH

a:select from np_single where wind_code=`000001.SZ
`rdate xasc a
asc distinct np[`rdate]
select from df_m where wind_code=`000001.SZ

select from factor_ic where index=`000000.SH,factor=`con_roe,category=`all,period=`1M

distinct df[`wind_code]
select from df where wind_code=`000001.SZ

select from df where wind_code=`A18045.SH

`wind_code xasc select from df where null np

select from df_q where wind_code=`000001.SZ
select from df_qs where wind_code=`000001.SZ
select from df_yoy where wind_code=`000001.SZ

tables[]
select from factorneu20180629_mcy where date=2018.06.29
select from XY_DevFactor where date in (2007.01.31,2007.02.28)
select from dfzq_sue_fillna_yes
tables[]

tables[]
exec distinct factor from select from factor_ic
select from factor_ic where factor=`AMORT_INTANG_ASSETS
tables[]
select date,stock_code,AMORT_INTANG_ASSETS from factorneu20180629_mcy where date=2018.06.29
['XY_EP_SQ', 'XY_LotteryMomentum_1M', 'XY_Volume20D_240D',       'XY_VolumeCV_20D']
select date,rank_ic from factor_ic where factor=`XY_VolumeCV_20D
tables[]
\l .

tables[]
10#select from dfzq_sue_ver3
cols select from dfzq_sue_fillna_yes_ver2
cols 10#select from dfzq_sue_fillna_yes
asc distinct select factor from factor_ic
\l .
system "l ."
tables[]
?[factor_ic;
parse "select date,rank_ic from factor_ic where factor=`dfzq_sue0_fillna_no"
rank_ic:select date,rank_ic from factor_ic where date>2010.01.01,factor=`dfzq_sue0_fillna_no,index=`000000.SH,category=`all,period=`1M
rank_ic
rank_ic:select date,rank_ic from factor_ic where date>2010.01.01,factor=`BETA_24M,index=`000000.SH,category=`all,period=`1M
(avg rank_ic[`rank_ic])%(dev rank_ic[`rank_ic])
qret:select from factor_qr_5 where date>2010.01.01
select distinct factor from factor_qr_5
select date,dfzq_sue0_fillna_no from dfzq_sue_fillna_no where date=2007.03.30

select date,q1,q2,q3,q4,q5 from factor_qr_5 where date>2010.01.01,factor=`dfzq_sue0_fillna_no,index=`000000.SH,category=`all,period=`1M

select date,dfzq_sue_with_seasonal_shift_ver5 from dfzq_sue_ver5
tables[]
select date,sym,SUE1_from_dfzq_wangxingxing from surprise_from_dfzq_wangxingxing where date=2018.05.31
select date,wind_code,dfzq_sue_no_seasonal_shift_ver4 from dfzq_sue_ver4 where date=2018.05.31

select from surprise_from_dfzq_wangxingxing where date=2018.05.31
tables[]
select from dfzq_sue_ver_8 where wind_code=`000001.SZ
\l .
cols select from dfzq_sue_ver_8
tables[]
select from tfzq_neutral_25_20180629_symm_orth where date.year=2018
tables[]
select from factorneu20180629_mcy where date=2018.07.31
select distinct date from factorneu20180629_mcy 

select from factorneu20180629_mcy where date=2018.06.29
select date,stock_code, S_DQ_ADJOPEN, S_DQ_ADJHIGH from factorneu20180629_mcy where date=2018.06.29
count cols factorneu20180731

tables[]
\l .
\l .
select from factor_pre_000000.SH where date>2018.06.01
select from factor_ic where date=2018.06.29,period=`1M,index=`000000.SH,category=`all,factor in (`ACCT_PAYABLE`ACCT_PAYABLE_20180731)
a:select stock_code, ACCT_PAYABLE from factorneu20180629_mcy where date=2018.06.29
b:select stock_code, ACCT_PAYABLE_20180731 from factorneu20180731 where date=2018.06.29
S_DQ_TURN_2018073
ic:select from factor_ic where date within 2012.12.31 2018.06.29,factor=`dfzq_sue_with_seasonal_shift_8,index=`000000.SH,category=`all,period=`1M
ic
ic2:select from ic where date within 2012.12.31 2018.06.29
sqrt(12)*(avg exec rank_ic from ic)%(dev exec rank_ic from ic)

tables[]
cols dfzq_sur
select from dfzq_sue_no_profit_notice where wind_code=`000001.SZ
