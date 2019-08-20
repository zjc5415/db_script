
tables[]@where tables[] like"*daily*"

select from qr_neu_test_only_change_winsorize_10 where factor = `YOY_Q_OPER_REV,date>2018.01.01,index=`000000.SH

select from qr_neu_test_only_change_winsorize_10 where factor = `AMORT_INTANG_ASSETS,date>2018.01.01,index=`000000.SH

select date,factor_cov from cov_daily where factor=`ADV_FROM_CUST,index=`000000.SH
cols cov_daily

S_DP

(2!select from ic_neu_daily where factor=`S_DP,index=`000000.SH) lj (2!select date,factor,q10-q1 from qr_neu_daily_10 where factor=`S_DP)

select from  cov_SUE
select from qr_SUE_10 where date>2018.01.01

select from ic_factor_xroe

select from ic_factor_xro

tables[]

select avg q1,avg q10,avg q1-q10 from qr_neu_daily_10 where date>2010.01.01,factor=`S_VAL_LN_MV,index=`000000.SH
select avg q1,avg q10,avg q1-q10 from qr_neu_daily_10 where date>2010.01.01,factor=`M,index=`000000.SH