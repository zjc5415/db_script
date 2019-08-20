select from a3_factor where date=`20190722
tables[]
select from ind where date=`20190722
 from a4_factor where date=`20190722
(cols a4_factor)  _ cols a3_factor

cols a4_factor
select distinct gics from a4_factor 

select date,Size from a4_return where date>`20190601
cols eod


tables[]

select from index_open_weight where date in (`20190729,`20190726)

`date xdesc select from index_open_weight where date in (`20190701)

10#select st from eod

select from eod where list_days >10
meta eod

(2!select date,wind_code,weight from index_open_weight where date in (`20190701),index=`000852.SH) ij 2!select from eod where date in `20190701,list_days>186

2!select from eod where date in `20190701,st=1b

/去除停牌，st，180天新股
select date,wind_code,close from eod where date>=`20190101, date<=`20190729,not  st,not suspend,list_days>180

status

select date, wind_code,deltas close  from eod

select date, wind_code,close, prev  close from eod fby wind_code


update pct:100*(deltas close)%close by wind_code from eod

`wind_code xasc update pct: (deltas close) by wind_code from  select date,wind_code,close,adjfactor from eod where date in (`20190102,`20190103)

/计算adjacent pct change
`wind_code xasc update pct: {(0N,1_deltas x)%x} close*adjfactor by wind_code from  select date,wind_code,close,adjfactor from eod where date in (`20190102,`20190103,`20190104)

/ 前向收益率
`wind_code xasc update pct: {((1_deltas x),0N)%x} close*adj by wind_code from  select date,wind_code,close,adj from eod where date within 2019.01.02 2019.01.04
count eod

`wind_code xasc update pct: {((1_x,0N)%x)-1} close*adjfactor by wind_code from  select date,wind_code,close,adj from eod where date in (`20190102,`20190103,`20190104)
/ n天连续收益率 
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

/ 选择月底数据
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