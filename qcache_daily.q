/ 同步stocklist，用于原来的因子开发
{(hsym `$"d:/cta/factor_dev/reference/StockList/",(string x`date),".json") 0: enlist .j.j {(-2#x),6#x} each exec string wind_code from eod where date=x`qdate,not wind_code like "688*" } each  select from tds where (not (string date) in  {8#string x } each key `:d:/cta/factor_dev/reference/Stocklist),(qdate in exec distinct date from eod)


/ 日频数据快照，用于因子分析
.d.date:exec distinct date from eod    /日度日期列表
.d.data:update fwdret20d:{((-20 xprev x)%x)-1} close*adj  by wind_code from (select from eod)    /所有日频数据
.d.data:2!.d.data lj 2!select date,wind_code,w300:weight from iow where index=`000300.SH
.d.data:.d.data  lj 2!select date,wind_code,w500:weight from iow where index=`000905.SH
.d.data:.d.data  lj 2!select date,wind_code,w1000:weight from iow where index=`000852.SH
.d.data:.d.data  lj 2!select date,wind_code,w50:weight from iow where index=`000016.SH
update pool:(not suspend)&(not st)&(list_days>120)&(not wind_code like "688*")&(citics1<>29) from `.d.data  /当天非st，上市大于120天，非停牌，非科创板,中信行业已分类
update pool_300:pool&(w300>0) from `.d.data    /300股票池
update pool_500:pool&(w500>0) from `.d.data    /500股票池
update pool_800:pool&(((0^w300)+0^w500)>0) from `.d.data    /800股票池
update pool_1000:pool&(w1000>0) from `.d.data    /1000股票池
update pool_1800:pool&(((0^w300)+(0^w500)+(0^w1000))>0) from `.d.data    /1800股票池
update logmv:log(mv) from `.d.data    / log总市值

/ 月底数据快照，用于因子分析
.m.date:exec max date by date.month from (select distinct date from eod)    /月度日期列表
.m.data:update  fwdret:{((1_deltas x),0n)%x} close*adj  by wind_code from (select from eod where date in .m.date)    /所有月频数据，包括当前月
.m.data:2!.m.data  lj 2!select date,wind_code,w300:weight from iow where index=`000300.SH
.m.data:.m.data  lj 2!select date,wind_code,w500:weight from iow where index=`000905.SH
.m.data:.m.data  lj 2!select date,wind_code,w1000:weight from iow where index=`000852.SH
.m.data:.m.data  lj 2!select date,wind_code,w50:weight from iow where index=`000016.SH
.m.status:(`date xkey select date:max(date),sumst:sum(st),sumsuspend:sum(suspend),sumup:sum(status=0),sumdown:sum(status=2),min(list_days) by date.month,wind_code from select date,wind_code,st,suspend,status,list_days from eod) lj select fmv30:{(asc x) floor .3*count x} fmv by date from eod
.m.data:.m.data lj `date`wind_code xkey .m.status
update pool:(not suspend)&(not st)&(list_days>120)&(not wind_code like "688*")&(citics1<>29) from `.m.data  /当天非st，上市大于120天，非停牌，非科创板,中信行
update pool_300:pool&(w300>0) from `.m.data    /300股票池
update pool_500:pool&(w500>0) from `.m.data    /500股票池
update pool_800:pool&(((0^w300)+0^w500)>0) from `.m.data    /800股票池
update pool_1000:pool&(w1000>0) from `.m.data    /1000股票池
update pool_1800:pool&(((0^w300)+(0^w500)+(0^w1000))>0) from `.m.data    /1800股票池
update logmv:log(mv) from `.m.data    / log总市值
update train: (sumsuspend=0)&(sumst=0)&(sumup<10)&(sumdown<10)&(list_days>120)&(not wind_code like "688*") from `.m.data  /月内无st，上市大于120天，月内无停牌，月内涨停小于 10，月内跌停小于10，非科创板,


/ 周度快照数据，用于因子分析
.w.date: exec max date by date.week from (select distinct date from eod)
.w.data:update  fwdret:{((1_deltas x),0n)%x} close*adj  by wind_code from (select from eod  where date in .w.date)
.w.data:2!.w.data  lj 2!select date,wind_code,w300:weight from iow where index=`000300.SH
.w.data:.w.data  lj 2!select date,wind_code,w500:weight from iow where index=`000905.SH
.w.data:.w.data  lj 2!select date,wind_code,w1000:weight from iow where index=`000852.SH
.w.data:.w.data  lj 2!select date,wind_code,w50:weight from iow where index=`000016.SH
.w.status:(`date xkey select date:max(date),sumst:sum(st),sumsuspend:sum(suspend),sumup:sum(status=0),sumdown:sum(status=2),min(list_days) by date.month,wind_code from select date,wind_code,st,suspend,status,list_days from eod) lj select fmv30:{(asc x) floor .3*count x} fmv by date from eod
.w.data:.w.data lj `date`wind_code xkey .w.status
update pool:(not suspend)&(not st)&(list_days>120)&(not wind_code like "688*")&(citics1<>29) from `.w.data  /当天非st，上市大于120天，非停牌，非科创板,中信行
update pool_300:pool&(w300>0) from `.w.data    /300股票池
update pool_500:pool&(w500>0) from `.w.data    /500股票池
update pool_800:pool&(((0^w300)+0^w500)>0) from `.w.data    /800股票池
update pool_1000:pool&(w1000>0) from `.w.data    /1000股票池
update pool_1800:pool&(((0^w300)+(0^w500)+(0^w1000))>0) from `.w.data    /1800股票池
update logmv:log(mv) from `.w.data    / log总市值
update train: (sumsuspend=0)&(sumst=0)&(sumup<10)&(sumdown<10)&(list_days>120)&(not wind_code like "688*") from `.w.data  /月内无st，上市大于120天，月内无停牌，月内涨停小于 10，月内跌停小于10，非科创板,


