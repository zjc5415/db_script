/ 同步stocklist，用于原来的因子开发
{(hsym `$"d:/cta/factor_dev/reference/StockList/",(string x`date),".json") 0: enlist .j.j {(-2#x),6#x} each exec string wind_code from eod where date=x`qdate,not wind_code like "688*" } each  select from tds where (not (string date) in  {8#string x } each key `:d:/cta/factor_dev/reference/Stocklist),(qdate in exec distinct date from eod)


/ 日频数据快照，用于因子分析
.d.date:select date,qdate,ldate:1 xprev qdate, ndate:-1 xprev qdate from tds    /日度日期列表
.d.data:update fwdret20d:{((-20 xprev x)%x)-1} close*adj,r:{(x%(1 xprev x))-1} close*adj,r1:{((-1 xprev x)%x)-1} close*adj by wind_code from (select from eod)    /所有日频数据
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
update sta:0b from `.d.data
{update sta:sta|(date within x[`s],x[`e]) from `.d.data where wind_code= x[`sym];} each select sym:S_INFO_WINDCODE,s:ENTRY_DT,e: (.z.D^REMOVE_DT)+365 from windst where S_TYPE_ST<>`S ;    /最近365天内出现在windst表中的股票
/ update fmv252:{swin[wavg[2 xexp (reverse til 252)%42;];252;x]} fmv by wind_code from `.d.data         /最近252天加权平均fmv，半衰期为42
/ update amount252:{swin[wavg[2 xexp (reverse til 252)%42;];252;x]} amount by wind_code from `.d.data         /最近252天加权平均amount，半衰期为42
update fmv30:fmv>{(asc x) floor .3*count x} fmv by date from `.d.data
/ update amount30:amount>{(asc x) floor .3*count x} amount by date from `.d.data
update pool1:(not suspend)&(not sta)&(list_days>252)&(not wind_code like "688*")&(citics1<>29)&(fmv30) from `.d.data  /当天非st，上市大于252天，非停牌，非科创板,中信行业已分类,最近252天未出现在windst中，流通市值不在最小的30%分位数



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


/`:hdb_b5_p/ set .Q.en[`:.]0!b5_p
/`:hdb_b5_r/ set .Q.en[`:.]0!b5_r
/`:hdb_b5_sr/ set .Q.en[`:.]0!b5_sr
/`:hdb_b5f_cov/ set .Q.en[`:.]0!b5f_cov
/`:hdb_b5f_gama/ set .Q.en[`:.]0!b5f_gama
/`:hdb_b5s_sigma/ set .Q.en[`:.]0!b5s_sigma
/`:hdb_b5s_gama/ set .Q.en[`:.]0!b5s_gama
/`:hdb_b5s_gama_sm/ set .Q.en[`:.]0!b5s_gama_sm
/delete b5_r,b5_sr,b5_p,b5_h from `.
/delete b5f_gama,b5f_sigma from `.
/delete b5s_h,b5s_p,b5s_r,b5s_sr,b5s_risk,b5s_sigma,b5s_gama,b5s_gama_sm from `.