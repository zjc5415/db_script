select from .d.data where date=2019.09.27,wind_code=`002938.SZ
select from factor_
select from cfactor_main
`:b5_r.csv 0: csv 0: select from b5_r
select from iow where index=`000852.SH


select from ft where id=235,datetime.date within 2019.09.10 2019.10.15, wind_code like "688*"

exec date from select  `$string max date by date.month from b5s_gama

select distinct date from b5s_gama

select from po300 where alpha=`GMV
meta b5_r
tables[]
select from b5_r where date>2019.10.01
select from b5_sr where date>2019.01.01
select from b5_p where date>2010.01.01
select from b5f_cov where date=2019.10.15
select from b5f_gama where date>2019.10.01
select from b5s_sigma where date>2019.01.01
select from b5s_gama where date>2010.01.01
select from b5s_gama_sm where date>2010.01.01

exec `$string max qdate by qdate.month from tds where qdate within 2019.01.01 2019.10.01

(select from b5s_gama) lj (1!select from b5s_gama_sm)lj(select from b5f_gama)

select date,adjusted_rsquare,r2:252 mavg adjusted_rsquare from b5_p where date>2010.01.01


(exec distinct date.date from quote) except (exec distinct date from cfactor_main)


2!`date xasc select from b5_sr where date within 2014.08.11 2015.08.19

exec distinct date from cfactor_main
select from cfactor_main where date=2007.01.05

 cfactor_main from `.

select from cfactor_main
select from cfactor_main where code=`B,date>2009.11.01
select from quote where code=`B,date>=2009.11.13
select from product where code=`B
select from cfactor_main where date<2008.01.01,code=`A

(select from cfactor_mom_basis)ij(2!select date,code,settle:settle*adj from cfactor_main)ij(2!select date,code,pool from cfactor_pool where pool)

select from product where code=`TA
select .z.D-min contract_issuedate.date by code from product

select days:2007.01.04-min contract_issuedate.date by code from product

select min contract_issuedate.date by code from product


tables[]
delete 
select sdev sigma_sh,max sigma_sh by date from b5s_sigma
delete b5s_sigma,b5s_gama,b5s_gama_sm from `.

select from b5f_cov
select from b5_r
tables[]
select from b5s_sigma where date=2019.09.11
select from b5s_gama where date=2019.09.11
d
select from quote where code=`IF
`:d:/cta/tmp.csv 0: csv 0: select from cfactor_cash_flow where code=`AU,date>2018.12.31
select from cfactor_oiavg
delete cfactor_oiavg from `.
select oi wavg open, oi wavg high,oi wavg low,oi wavg close,sum(oi),oi wavg settle,sum(volume),sum(amt)  by date,code from quote 
select from quote where code=`AL
cols quote
`date xasc 
(select size:sum(oi*close) by `date`code from quote where date>=2014.06.13,date<=2014.09.10)

select max oi*close by `date`code from quote
select max oi*close by date,code from quote

lj(1!select contract,multiplier from product)

select from cfactor_cash_flow

`date xdesc select from cfactor_tec where code=`AU
delete cfactor_main from `.
select from quote where code=`IF,date within 2019.09.18 2019.09.23
select from cfactor_main where code=`IF
first exec contract  from quote where date=2011.12.01,code=`FU,(oi+volume)>=max(oi+volume)
select from product
tables[]
select from cfactor_roll_return_reg
select max date from quote
first exec contract from quote where date=2018.06.27,code=`FU,(0^oi+0^volume)>=max(0^oi+0^volume)
select from quote where date=2018.06.27,code=`FU
`date xdesc select from cfactor_main where code=`IM
select from quote where code=`FU
se
select from quote where date=2014.09.08
count select distinct code  from quote where date=2014.09.05
count select distinct code  from quote where date=2014.09.11
select from tds where qdate within 2014.09.05 2014.09.09
select from quote where code=`FU,date=2011.12.01
select from product where code=`RI
select from quote where date=2019.10.09
/delete date from splayed table
{0N!x;`:neu2/ upsert select from neu where date.year=x,date<2019.09.24} each exec distinct date.year from neu where date<2019.09.24

select distinct date from quote
select from cfactor_main where code=`AG
select distinct date from quote where code=`A
select from cfactor_main where date=2007.01.23

select from factor_cne5 where date=2019.09.27
select from factor_cne5_desc where date=2019.09.27
delete cfactor_main from `.


tables[]
select max date from b5_r
select from b5_sr where null sr
b5_sr:`date xasc b5_sr

select from factor_cne5 where date=2018.08.23,wind_code=`000629.SZ
b5_sr:delete from b5_sr where null sr
select from b5_sr where wind_code=`000629.SZ
$[not `b5_sr in tables[];0b;(exec asc distinct qdate from tds where qdate within 2017.01.13 20180123)~(exec asc distinct date from b5_sr where date within 2017.01.13 20180123)]
select from b5s_gama_sm
select from b5f_gama
select max date from factor_cne5 
select min date from b5_sr 
delete b5s_sigma from `.
select from b5f_cov
select from b5f_gama
select from b5s_sigma
select from factor_cne5 where wind_code=`002450.SZ

tables[]@where tables[] like "b5*"
parse  "`:b5_r/ set b5_r"
b5_r:`date xasc b5_r

select date,sums(COUNTRY) from b5_r
tables[]
delete from `b5s_sigma
delete b5f_cov,b5f_sigma from `.
delete b5_cov_nw_eigen from `.
delete b5s_h,b5s_p,b5s_r,b5s_sr,b5s_risk,b5s_sigma from `.
select from b5_p
select date,wind_code,r,fmv,citics1 from .d.data where date>2019.01.01
delete b5_r from `.
delete from `b5_r where date=2019.09.27
select from  b5_r 
$[not `factor_cne5 in tables[];0b;2018.01.02 in exec distinct date from factor_cne5]
select wind_code,SIZE,NLSIZE,VOL,LIQUIDITY,RSTR,EY,GROWTH,LEVERAGE,BP from factor_cne5 where date=2019.09.26
select max date from factor_cne5
select from b5_r
tables[]
`$string 2019.01.01
$[not `b5s_risk in tables[];0b;(exec asc distinct qdate from tds where qdate within 2019.09.27 2019.09.27)~(exec asc distinct date from b5s_risk where date within 2019.09.27 2019.09.27)]
select from b5s_sigma
(exec distinct qdate from tds where qdate within 2019.09.01 2019.09.12)~(exec distinct date from b5s_risk)
$[not `b5s_risk in tables[];enlist();`()$exec distinct date from b5s_risk]
tables[]
select from b5_sr where wind_code=`000418.SZ
select max date from factor_cne5
select from b5_r
tables[]
select from a3_return where date.month=2019.09m
select min date from b5_cov
select from b5_h where date=2019.03.25
select from b5_cov where date=2019.03.27
select from b5_gama where date=2019.03.25

any not `b5_gama`b5_cov in tables[]

(b5_cov_nw_eigen in talbes) | not 2019.09.27 in exec distinct date from b5_cov_nw_eigen
not b5_cov_nw_eigen in tables[]
tables[]
b5_cov:select from b5_cov_nw_eigen
cols b5_cov_nw_eigen
{[x;y]([]rsquare:enlist x;adjusted_rsquare:enlist y)}[1;2]
select date,sums MKT from b5_r

select date,adjusted_rsquare, mr2:60 mavg adjusted_rsquare from b5_p
tables[]

select from factor_cne5_desc

parse "delete b5_r,b5_sr from `."

delete b5_r,b5_sr,b5_p,b5_h from `.

b5_h
`d$enlist .z.P
select max date from eod
2!select date,wind_code,citics1,fmv,r1 from .d.data where date=2019.09.26,pool1
cols .d.data

select from tds

til (1990.12.03+100)
nature_daty:(exec first qdate from tds) +/: til (exec (max qdate)-min qdate from tds)

([]qdate:nature_daty)lj `qdate xkey select from tds


(2!select from factor_cne5_desc where date=2019.09.26)ij(2!select date,wind_code,pool1 from .d.data where date=2019.09.26,pool1)

select from a3_return where date=2019.09.26
select from factor_cne5 where date=2019.09.26

select max Growth ,min Growth ,avg Growth by date  from a3_factor where date>2019.01.01

t:((select cn:count wind_code by date from .d.data)lj(select cn1:count wind_code by date from .d.data where pool1))
select date, cn,cn1,cn1%cn from t
update pool1:(not suspend)&(not sta)&(list_days>252)&(not wind_code like "688*")&(citics1<>29)&(fmv252_30)&(amount252_30) from `.d.data 
d
tables[]
cols factor_cne5_desc
select from factor_cne5
select from factor_cne5_desc where date=2019.01.02,wind_code=`000018.SZ
\l
(select from factor_cne5_desc where date=2019.09.26)ij(2!select date,wind_code,pool1 from .d.data where date=2019.09.26,pool1)

select date,wind_code,fmv,fmv252 from .d.data where date=2019.09.26
update pool:(not suspend)&(not st)&(list_days>120)&(not wind_code like "688*")&(citics1<>29) from `.d.data  /当天非st，上市大于252天，非停牌，非科创板,中信行业已分类, 过去252天停牌少于174，停牌少于174，退市

select from windst

update fmv252_30:fmv252>{(asc x) floor .3*count x} fmv by date from `.d.data

{(asc x) floor .3*count x}   100?100

select S_INFO_WINDCODE,REPORT_PERIOD,ACTUAL_ANN_DT,STATEMENT_TYPE,NET_PROFIT_EXCL_MIN_INT_INC from windi where STATEMENT_TYPE in (`408001000`408004000`408005000),ACTUAL_ANN_DT within 2019.01.01 2019.09.24

select from windc where S_INFO_WINDCODE=`000333.SZ
count windc
cols windb
select from a3_return where date>2019.09.19
select from fa_neu_w where date>2019.09.2

`:d:/cta/tmp.csv 0: csv 0: select from a3_return where date>2019.01.01

select from factor_xroe where date=2019.09.24
select date,q1,q10 from fa_neu_w where factor=`fwdret
cols fa_ne
{0N!x;`:neu2/ upsert select from neu where date.year=x,date<2019.09.24} each exec distinct date.year from neu where date<2019.09.24
select from neu where date=2019.09.24
select max date from neu2
select from n
count neu2
count neu
delete neu from `.
neu: select from neu where date<2019.09.20

`:neu2/ set neu

select from factor_xroe where date=2019.09.20

select from windst where ENTRY_DT>=2018.09.06,S_TYPE_ST<>`S

select date,wind_code,adjclose:close*adj%100,st,suspend from eod where date within 2018.09.06 2019.09.20, list_days>180,not wind_code in (exec S_INFO_WINDCODE from windst where ENTRY_DT within 2018.09.06 2019.09.20,S_TYPE_ST<>`S)
cols eod

select distinct S_TYPE_ST from windst
delete windst from `.
meta windst

select date,wind_code,close,adj,mv,st,suspend from eod where date within 2019.09.20- 2019.09.20, list_days>180



delete windb from `.

{[xdate;yn;zn]start_date:first -yn#exec qdate from tds where qdate<xdate;tmp:select date,wind_code,adjclose:close*adj%100,mv,st,suspend from eod where date within start_date,xdate, list_days>zn;
select update r:{(( xprev x)%x)-1} close*adj  by wind_code from tmp;}

count windb
select from windi where OPDATE>2019.09.01

2019.09.20-247
-101#select from .d.data

select from tds where qdate
first -247#exec qdate from tds where qdate<2019.09.20
l:1,2,3,4


flip (`id`uid`date`start_time`end_time`amt`twap_cost`vwap_cost)!("IIIVVFFF";enlist",") 0: `:W:/zhangjc/cost.csv

.Q.fs[{0N!("IIIVVFFFF";enlist",") 0:x}] `:W:/zhangjc/cost.csv

slip:("IIIVVFFF";enlist",") 0:`:d:/cta/cost1.csv

`:slip set slip

(2!select from fjh_neu1)ij(2!select date,wind_code,fwdret from .m.data where pool,date < (exec max date from .m.data))
get slip
2#select date,wind_code,fwdret from .m.data 

select from fa_neu_m_300 where factor=`Sent,date>2018.01.01

select max q10 by date from fa_neu_m_300 
`:d:/cta/a3.csv 0: csv 0: select from a3_return where date>2019.08.31


`citics1 xdesc (2!select date,wind_code,IMOM1_11M from neu where date=2019.08.30)ij(2!select date,wind_code, fwdret, citics1,mv,w300 from .m.data where date=2019.08.30,citics1=4,w300>0)
cols neu
select distinct date from neu
tables[]
select from factor_ht_ns where NS=0w

select from .m.data where pool, date=2019.09.18
2!select date,wind_code,fwdret from .m.data where pool,w300>0,date < exec max date

select date,wind_code,fwdret from  .m.data where pool,w300>0,date < exec max date

(2!select from neu) ij (2!select date,wind_code,fwdret from  .m.data where pool,w300>0,date < exec max date)

select date,wind_code,fwdret from  (.m.data) where w300>0,date < exec max date
1#select  date,wind_code,fwdret from .m.data where w300>0,pool,(date<exec max date)
select from .m.data where date< max date

select distinct date from neu where date within 2018.03.22 2018.08.15

upserttable_no_duplicate

select from po300

select max date from neu

select avg rank_ic ,sdev rank_ic by factor from fa_neu_m_300 where date>2019.01.01

(select sdev VOL20 by date from neu) ij(1!select date,factor,ic,rank_ic,long_short,q1,q10 from fa_neu_m_300 where date>2019.01.01,factor=`NP_SQ_YMY_GSTD8)

select date,factor,ic,rank_ic,long_short,q1,q10 from fa_neu_m_300 where date>2018.01.01,factor=`EP_ROLL_TFW_fill

se

d:(select date,wind_code,NP_SQ_YMY_GSTD8 from neu where date=2019.05.31)ij(2!select date,wind_code,fwdret from  .m.data where pool,w300>0,date=2019.05.31)
`:d:/cta/d.csv 0: csv 0: d

`d/

d
e:select date,wind_code,weight from po300 where alpha=`NP_SQ_YMY_GSTD8,date=2019.05.31
d:d lj 2!e
(cols neu)@where (cols neu) like "*EP*"

`:d:/cta/1.csv 0: csv 0: select date,NP_SQ_YMY_GSTD8,XROE,ROE_SQ from neu where wind_code=`002311.SZ
select from po300 where alpha=`NP_SQ_YMY_GSTD8
select from mf where date=2019.05.31
po300:delete  from po300
po300
select from cmp

(`date`wind_code xkey select from po300 where alpha=`IMOM1_11M,date=2019.08.30) ij 2!select date,wind_code,citics1 from eod where date=2019.08.30,citics1=0
(`date`wind_code xkey select date,wind_code,IMOM1_11M from neu where date=2019.08.30) ij 2!select date,wind_code,citics1 from eod where date=2019.08.30,citics1=0
(`date`wind_code xkey select  from neu where date=2019.08.30) ij 2!select date,wind_code,citics1 from eod where date=2019.08.30,citics1=0

select from neu where wind_code=`002142.SZ

select from po300
delete po500 from `.
select from po300 where alpha=`DEP

.m.date@where .m.date within 2019.01.01 2019.10.01

(value .m.date)@where .m.date within 2019.01.01 2019.10.01

2!(select date,wind_code,share from fp where date in (2019.08.30), id=136)lj(2!select date,wind_code,close from eod where date in (2019.08.30))

(2!select date,wind_code, fwdret, citics1,mv,w300 from .m.data where date=2019.08.30,w300>0)lj(2!select date,wind_code,weight from po300 where date=2019.08.30,alpha=`IMOM1_11M)
select from fp where wind_code=`002961.SZ
select from po300
meta fp
po300:select from po300
delete from `po300 where alpha=`IMOM1_11M

1#select from .m.data

delete po300 from `.

`:po300/ set  po300
select from fp where date>2019.09.24,id=182
select from po300

select max date from .m.data
select from fp where date=2019.09.17,id=235,wind_code=`002962.SZ
`date xdesc select sum(price*volume) by id,datetime.date,direction from ft where datetime.date>=2019.09.01,id=136

alpha
select from .m.data where date=2019.08.30,wind_code=`603486.SH
select sum(weight) from po500
select from po500
meta po300
select from po300

delete po500 from `.
select from po500
(select from a3_factor where date=(2019.08.30))lj        (2!select date,wind_code,wb:weight from iow where date in (2019.08.30),index=`000905.SH)

(select from a3_factor where date=2019.05.31) lj  2!select date,wind_code,weight from iow where  date=2019.05.31,index=`000905.SH
select from iow where i=max(i)
cols .m.data
select max date from neu




select min date from factor_xroe
cols neu
`long_short xdesc select date,factor,ic,rank_ic,long_short,rank_ac from fa_neu_m where date.month=2019.07m
`date`factor`long_short xdesc select date,factor,ic,rank_ic,long_short,rank_ac from fa_neu_m where factor=`IMOM1_11M
`ir xdesc select avg rank_ic,ir:sqrt(12)*(avg rank_ic)%(sdev rank_ic),avg q1,avg q10,avg long_short,avg rank_ac by factor from fa_neu_m
-3exec max date except max date from .m.data
(-3#(exec distinct date from .m.data))[0]
first -3#.m.date
first -2#.m.date
select from factor_xroe where date=2019.09.10
select max date from neu

select from neu where date=2019.09.19

select from factor_dfzq_sue where date=2019.09.19


select distinct date from neu
cols factor_ht_quanlity
tables[]
2!select date,wind_code,fwdret from .m.data where pool,date = exec max date except max date from .m.data

`factor`date xasc select from fa_neu_w where date>2019.08.01

select max date from eod
select max date from .d.data

select from fh
select from .d.data where date=2019.09.09,w50>0

select max date from iow where index=`000016.SH

select from iow where date=2019.09.09,index=`000016.SH

`:d:/cta/t.csv 0: csv 0: select from trade where datetime.date=2019.09.12
select from trade

(select qdate:date,wind_code,list_days from eod where date within 2019.09.01 2019.09.10)lj `qdate xkey select from tds

`date xasc select date,ic,rank_ic,qrank_ic,rank_ac from fa_neu_w where factor=`EGP_TFW_fill
select from fa_neu_w

`date xasc select  from fa_neu_w where factor=`NP_SQ

select from tds
meta tds
tds



tmp:`NP_SQ`date xasc (2!select date,wind_code,NP_SQ_neu:NP_SQ,gstd8_neu:NP_SQ_YMY_GSTD8 from neu)ij(2!select date,wind_code,NP_SQ,gstd8:NP_SQ_YMY_GSTD8 from factor_ht_quanlity)ij(2!select date,wind_code,citics1,logmv,fwdret,adjclose:adj*close from .m.data  where pool_300,date=2019.08.30)
`NP_SQ xasc select from tmp where citics1=0x16

tables[]
cols factor_ht_quanlity
select from a3_factor where date=`20190722
tables[]
select from ind where date=`20190722
 from a4_factor where date=`20190722
(cols a4_factor)  _ cols a3_factor

cols a4_factor
select distinct gics from a4_factor 

select date,Size from a4_return where date>`20190601
cols eod

select from m


tables[]

select from index_open_weight where date in (`20190729,`20190726)

`date xdesc select from index_open_weight where date in (`20190701)

10#select st from eod

select from eod where list_days >10
meta eod

(2!select date,wind_code,weight from index_open_weight where date in (`20190701),index=`000852.SH) ij 2!select from eod where date in `20190701,list_days>186

2!select from eod where date in `20190701,st=1b


select date,wind_code,close from eod where date>=`20190101, date<=`20190729,not  st,not suspend,list_days>180

status

select date, wind_code,deltas close  from eod

select date, wind_code,close, prev  close from eod fby wind_code


update pct:100*(deltas close)%close by wind_code from eod

`wind_code xasc update pct: (deltas close) by wind_code from  select date,wind_code,close,adjfactor from eod where date in (`20190102,`20190103)


`wind_code xasc update pct: {(0N,1_deltas x)%x} close*adjfactor by wind_code from  select date,wind_code,close,adjfactor from eod where date in (`20190102,`20190103,`20190104)


`wind_code xasc update pct: {((1_deltas x),0N)%x} close*adj by wind_code from  select date,wind_code,close,adj from eod where date within 2019.01.02 2019.01.04
count eod

`wind_code xasc update pct: {((1_x,0N)%x)-1} close*adjfactor by wind_code from  select date,wind_code,close,adj from eod where date in (`20190102,`20190103,`20190104)

`wind_code xasc update pct: {((2_x,2#0N)%x)-1} close*adjfactor by wind_code from  select date,wind_code,close,adj from eod where date in (`20190102,`20190103,`20190104)

meta eod
select from eod where status=0
a: -1h
type -1i
select distinct status from eod



`wind_code xasc update pct: {((1_deltas x),0N)%x} close*adj by wind_code from eod where date within 2019.01.02 2019.01.04

update date,wind_code, pct: {((1_deltas x),0N)%x} close*adjfactor by wind_code from eod where date in (`20190102,`20190103,`20190104)

 select date.month from select distinct date from eod

select distinct date from eod fby date.month

select max date by month from (select distinct date,month:date.month from eod) 


select date,wind_code, fwdret:{((1_deltas x),0N)%x} close*adj,citics1,log(fmv) from eod  where (date in exec max date by date.month from (select distinct date from eod where date within 2006.01.01 2007.01.01 )) ,not st,not suspend,list_days>180

select date,wind_code,y:fwdret,size:fmv,`$"value":1%pettm from .m.train where date within 2006.02.28 2006.03.31

4 xrank 30 10 40 20 90

select date, wind_code, fmv>(count(fmv)*0.3  by date from eod
select fmv30:min(fmv) by date from eod

select fmv30:{(asc x) floor .3*count x} fmv by date from eod

{(asc x)[count(x)]}[1,2,3,4]
{count(x)*}[1,2,3,4]

{(asc x) floor .3*count x}[1,2,3,4]

`date xkey .m.status

rank

2!select from .m.pool
tmp:-10#select from .m.data 
tmp
select from tmp where not wind_code  like "68808*"



a:select date:max(date),sumst:sum(st),sumsuspend:sum(suspend),sumup:sum(status=0),sumdown:sum(status=2),min(list_days) by date.month,wind_code from select date,wind_code,st,suspend,status,list_days from eod
a
select from a where sumdown>5
(string `688001.SZ) like "688*"
"i"$10
(2!select from .w.data) ij 2!select from .m.data

`date xdesc select count wind_code by date from .m.data
`wind_code`date xasc .m.data

select date, close*adj,suspend from eod where date within 2010.05.31 2010.12.01,wind_code=`000001.SZ

select from .m.data where date=2010.06.30,wind_code=`000001.SZ

select distinct date from eod where date within 2005.01.01 2007.01.01

select date,wind_code,M from mf
meta mf

meta eod
count eod

@[{sd;1b};0;0b]

sortandsetp[dbdir;par_tablename;key_cols;log_path]

sortandsetp["f:/db_cache";"mf";("date";"wind_code");"d:/dblog.txt"]


.[{x xasc y;1b};(`date`wind_code;`:f:/db_cache/eod);{0N!"ERROR - failed to sort table"; 0b}]

`date`wind_code xasc `:f:/db_cache/eod

meta a3_return
sortdb[`:f/db_cache/a3_return;`date;"d:/dblog.txt"]
sortdb[`:d:/db/product;`code`lastdelivery_date;log_path]

meta a3_factor
`date`wind_code xasc `:f:/db_cache/a3_factor


`:a3_return ~ `date xasc `:a3_return
 .[{@[x;y;z];1b};(partition;attrcol;attribute);0b]

{@[x;y;z];1b}[`:a3_return;`date;`s#]

 .[{@[x;y;z];1b};(`:a3_return;`date;`s#);0b]
 .[{@[x;y;z];1b};(`:a3_return;`date;`p#);0b]

.[if 1 1;1]
@[value; 6%7; show]
tables[]
meta mf

select from index_open_weight
delete index_open_weight from `.
select from ind
meta iow
select from iow
meta eod
`date`wind_code xasc `:eod
.[{@[x;y;z];1b};(`:eod;`date`wind_code;`s#);0b]


.[{x xasc y;1b};(sortcols;partition);{dblog[log_path;"ERROR - failed to sort table: ",string partition]; 0b}

.[{x xasc y;1b};(`date;`:a3_return);0b]

select from a3_return

sortandsetp
sortdb
setattribute


sortdb[`:a3_return;`date;"d:/dblog.txt"]
tables[]
select from index_open_weight where wind_code=`600549.SH,date=`20190617

(1!select wind_code,weight from index_open_weight where date=`20190613,index=`000300.SH) ij (1! select wind_code,weight1:weight from index_open_weight where date=`20190617,index=`000905.SH)


:{0N,1_deltas x}

select from a3_factor
meta a3_factor

select from iow

d :2019.08.04
d.month
d.week
d.minite


d:1900.01.01

tables[]

.w
meta .m.data
meta eod


stock_list :read0 `:d:/cta/factor_dev/reference/StockList/20190531.json
stock_list[0]

exec wind_code from eod where date=2019.08.07,((wind_code like "60*") or (wind_code like "30*") or (wind_code like "00*"))

slist:exec wind_code from eod where date=2019.08.07,not wind_code like "688*"
enlist .j.j exec {(-2#x),6#x} string wind_code from eod where date=2019.08.07,not wind_code like "688*"
.z.t

raze "." vs string .z.D

{raze "." vs string x}  each exec distinct  date from eod except 

update string date from (select distinct date from eod)
{8#string x } each key `:d:/cta/factor_dev/reference/Stocklist

select  from tds
date xkey (select distinct date from eod) ij `qdate xkey select from tds

qdate xkey (select qdate:distinct date from eod)

l:exec date from tds where qdate in exec distinct date from eod

less_date:(exec string date from tds where qdate in exec distinct date from eod) except {8#string x } each key `:d:/cta/factor_dev/reference/Stocklist

select from tds where (qdate in exec distinct date from eod),(string date not in  {8#string x } each key `:d:/cta/factor_dev/reference/Stocklist)


{(hsym `$"d:/cta/factor_dev/reference/StockList/",(string x`date),".json") 0: enlist .j.j {(-2#x),6#x} each exec string wind_code from eod where date=x`qdate,not wind_code like "688*" } each  select from tds where (not (string date) in  {8#string x } each key `:d:/cta/factor_dev/reference/Stocklist),(qdate in exec distinct date from eod)

j: {enlist .j.j {(-2#x),6#x} each exec string wind_code from eod where date=x`qdate,not wind_code like "688*" } each  select from tds where (not (string date) in  {8#string x } each key `:d:/cta/factor_dev/reference/Stocklist),(qdate in exec distinct date from eod)

hsym `$"d:/cta/factor_dev/reference/StockList/","20190101",".json"
hsym `$"d:/cta/factor_dev/reference/StockList/",string x`date,".json"

enlist .j.j 
{(-2#x),6#x} each exec string wind_code from eod where date=2019.08.07,not wind_code like "688*" 

exec {string x} wind_code from eod where date=2019.08.07



y:10# select from a3_return

`tbl1 upsert tbl
tables[]
delete fa_m_pool_300 from `.

fa_m_pool_300 upsert select from fa_m_pool_300

tbl
test_tbl

x:"000001.SZ"

7 _ x,6_ x
(-2#x),6#x
6_x
slist_str:.j.j slist
`:d:/cta/1.txt 0: enlist slist_str
 `:d:/cta/factor_dev/reference/StockList/20190807.json 0: enlist .j.j slist


tables[]
(cols factor_bvol20) except `wind_code

select from .m.data where date=2019.07.31
delete fa_m_pool_300 from `.
fa_m_pool_300

select from fa_m_pool_300 where factor=`BVOL20
select from fa_m_pool_300

tables[]
select from factor_haitong_imom_cn3 where date.month = 2019.06m

select from a3_return where date>=2019.08.01
tables[]

{eod:select from eod where date=x;.Q.dpft[`:f:/db_eod;x;`wind_code;`eod] } each (2019.08.13,2019.08.14)

tables[]
select distinct date from haitong_gp_trend

tables[]

select from factor_imom_cn3


select from a3_return

select date,Size, (nsize:(-20 xprev Size)%Size-1),ndate:-20 xprev date from a3_return

update ndate:-20 xprev date,nclose:-20 xprev close, fwdret20d:{((-20 xprev x)%x)-1} close*adj  by wind_code from (select from eod where date>2019.01.01)    /所有日频数据，包括当前月

(`date xkey select date:max(date),sumst:sum(st),sumsuspend:sum(suspend),sumup:sum(status=0),sumdown:sum(status=2),min(list_days) by date,wind_code from select date,wind_code,st,suspend,status,list_days from eod) lj select fmv30:{(asc x) floor .3*count x} fmv by date from eod


(`date xkey select date:max(date),sumst:sum(st),sumsuspend:sum(suspend),sumup:sum(status=0),sumdown:sum(status=2),min(list_days) by date,wind_code from select date,wind_code,st,suspend,status,list_days from eod)


select date,wind_code,sumst:20 msum st  from (select date,wind_code,st,suspend,status,list_days from .d.data) 

select distinct date from factor_dongwu_m
select distinct date from factor_dfzq_sue

select max weight from iow where date=2019.08.15,index=`000905.SH

select from .m.data where date>= exec max date except max date from .m.data 

(2!select date,wind_code,fwdret from .m.data where pool,date >= exec max date except max date from .m.data)
select from neu

select from factor_sw_std_growth
select from eod where date in (2019.07.31,2019.08.16) ,wind_code=`000001.SZ
select from .m.data where date in (2019.07.31,2019.08.16) ,wind_code=`000001.SZ

select from eod where date within 2015.05.12 2016.10.20, wind_code=`002423.SZ,st


(cols a3_return)

1_(cols a3_return)
`:d:/cta/size.csv 0: csv 0:  sums(1!select from a3_return)
parse "select date,sums(Size),sums(Growth) from a3_return"

parse "select date,Size,Growth from a3_return"


 sums(1!select from a3_return)

?[`a3_return;();0b;(cols a3_return)!(`date;(+\;1_(cols a3_return)))]


select min date from a4_factor

select max date from a4_return

tables[]

count select distinct date from factor_tianfeng_con
count select distinct date from factor_rcgo_500

select min date from factor_xroe

l1:exec distinct date from factor_xroe
l2: exec distinct date from factor_haitong_gp_trend
l1 except l2
l
l2
count l1
count l2
count 
count select distinct date from factor_dongwu_m

tables[]

select from iow where index=`000300.SH, date=2019.08.29

select from .d.data where date=2009.06.29,wind_code=`000001.SZ


select from iow where index=`000016.SH,wind_code=`600518.SH

select max(date) from

select from neu

(2!select date,wind_code,fwdret from .m.data where pool,date >= exec max date except max date from .m.data) ij 2!select from neu

tables[]
select from fa_neu_w

exec max date except max date from .w.data

select date from .w.data
.w.date
.m.date

-10#exec max date by date.week from (select distinct date from eod)
select from tds
select distinct qdate.week+1 from tds where qdate within 2019.08.01 2019.09.01

select distinct date.week from (select distinct date from eod)

`ic xasc select date,factor,ic,rank_ic,long_short,qrank_ic from fa_neu_m


`:d:/cta/a3_return.csv 0: csv 0: select from a3_return where date within 2010.01.01 2019.08.26

select date,sums Leverage from a3_return where date within 2010.01.01 2019.08.26
tables[]

(select from factor_dfzq_sue) ij 2!select date,wind_code,mv,citics1 from eod

1#select from a3_factor

select from a3_factor ij 2!select 
select from iow where index=`000016.SH,wind_code=`000002.SZ

select max(date) from eod

select from cmp

count select from cmp
tables[]

select from eod where 

select sum v by date from cmp where date>2019.08.01,contract like "AU*"

parse "select  from cmp where type=1"



type

select max(date) from neu
10#select from neu

select from fa_neu_w