
select distinct fund_id from trade where datetime.date=2019.09.12
select from trade where datetime.date=2019.09.16,fund_id=203
select from trade where datetime.date=2019.09.16,fund_id=136
select from trade where datetime.date=2019.09.16,fund_id=214

select amt:sum(volume*price),vol:sum(volume),c:count(id) by datetime.minute from trade  where datetime.date=2019.09.12,fund_id=214


`factor`date xdesc select date,factor,ic,rank_ic from fa_neu_m where factor=`BVOL20
`ir xdesc select avg rank_ic,ir:sqrt(12)*(avg rank_ic)%(sdev rank_ic),avg q1,avg q10,avg long_short,avg rank_ac by factor from fa_neu_m
-3exec max date except max date from .m.data
(-3#(exec distinct date from .m.data))[0]
first -3#.m.date
first -2#.m.date
select from factor_xroe where date=2019.09.10
select max date from neu

select distinct date from neu
cols factor_ht_quanlity
tables[]
2!select date,wind_code,fwdret from .m.data where pool,date = exec max date except max date from .m.data

`factor`date xasc select from fa_neu_w where date>2019.08.01

select max date from eod
select max date from .d.data

select from .d.data where date=2019.09.09,w50>0

select max date from iow where index=`000016.SH

select from iow where date=2019.09.09,index=`000016.SH

`:d:/cta/t.csv 0: csv 0: select from trade where datetime.date=2019.09.12
select from trade

(select qdate:date,wind_code,list_days from eod where date within 2019.09.01 2019.09.10)lj `qdate xkey select from tds

`date xasc select date,ic,rank_ic,qrank_ic,rank_ac from fa_neu_w where factor=`EGP_TFW_fill

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