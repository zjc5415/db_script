//ht_mom_time:("DFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"; enlist ",") 0: `:d:/20180206_mom_time.csv;
//upserttable["d:/db";"ht_mom_time";ht_mom_time]
comp_mom_time:{[xcode]
    to_comp:flip ?[ht_mom_time;();();`date`ht!`date,xcode];
    f:select date,code,mom_time from key_tab where code=xcode,date>=min to_comp[`date];    
    to_comp:update date:`timestamp$date from to_comp;
    :lj[f;1!to_comp];
}
key_tab
comp_mom_time[`ZC]
lj[comp_mom_time[`AL];1!select from factor where code=`RO]

select from product where code=`CY
select from quote where code=`CY
select date,CY from ht_mom_time
delete code from comp_mom_time[`MA]

MA 2014.12.04
`oi xdesc select from quote where code=`MA,date=2014.12.04
select from factor where code=`MA,date>=2014.11.01

//ht_roll_return:("DFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"; enlist ",") 0: `:d:/20180206_roll_return.csv;
//upserttable["d:/db";"ht_roll_return";ht_roll_return]
comp_roll_return:{[xcode]
    to_comp:flip ?[ht_roll_return;();();`date`ht!`date,xcode];
    f:select date,code,roll_return_near_far from factor where code=xcode,date>=min to_comp[`date];    
    to_comp:update date:`timestamp$date from to_comp;
    :lj[f;1!to_comp];
}
a:comp_roll_return[`AL]
comp_roll_return[`MA]

select from factor where code=`ZC
select from product where code=`ZC

//ht_mom_basis:("DFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"; enlist ",") 0: `:d:/20180206_mom_basis.csv;
//upserttable["d:/db";"ht_mom_basis";ht_mom_basis]
comp_mom_basis:{[xcode]
    to_comp:flip ?[ht_mom_basis;();();`date`ht!`date,xcode];
    f:select date,code,mom_basis_near_far from factor where code=xcode,date>=min to_comp[`date];    
    to_comp:update date:`timestamp$date from to_comp;
    :lj[f;1!to_comp];
}
key_tab
comp_mom_basis[`AL]
comp_mom_basis[`]
MA ZC差距较大
B一模一样,但是有大量的nan值
select from trade where code=`B
OI select from quote where code=`OI,date=2012.07.16
OI,RI,WH,ZC开始不一致，可能是合约换名导致的

//ht_mom_warehouse_receipt:("DFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"; enlist ",") 0: `:d:/20180206_mom_warehouse_receipt.csv;
//upserttable["d:/db";"ht_warehouse_receipt";ht_mom_warehouse_receipt]
comp_mom_warehouse_receipt:{[xcode]
    to_comp:flip ?[ht_mom_warehouse_receipt;();();`date`ht!`date,xcode];
    f:select date,code,mom_warehouse_receipt from factor where code=xcode,date>=min to_comp[`date];    
    to_comp:update date:`timestamp$date from to_comp;
    :lj[f;1!to_comp];
}
comp_mom_warehouse_receipt[`C]

select from warehouse_receipt where code=`C,date=2010.05.20
// functinal compare
c:`mom_basis_near_far;
comp_factor[`AL;t;xcode;c]
    f:select date,code,mom_basis_near_far from factor where code=xcode;
    f:flip
    conditions:(=;`code;xcode);       
    ?[factor;conditions;();`date]
    to_comp:flip ?[ht_mom_basis;();();`date`ht!`date,xcode];
    to_comp:update date:`timestamp$date from to_comp;
    :lj[f;1!to_comp];
}
conditions:((=;`code;xcode));       
?[factor;();0b;()]
?[factor;conditions;();`code]

flip ?[key_tab;();();`date`c1`c2!`date`code`open]
x:`adjclose
flip ?[factor;();();(`date`code,x)!`date`code,x]
{flip ?[factor;();();(`date`code,x)!`date`code,x]}`adjclose

    listc:?[key_tab;();();c];      //从 key_tab中选择c列数据，
    key_tab
    ?[key_tab;();();`date`code]
    conditions:((<;`date;min key_tab[`date]);(=;`code;key_tab[`code][0]));       
    ref:(neg n ) sublist ?[t;conditions;();c];                                  //从factor中选择c列的最后n个值，不足n个则保留有效值,ref 可能为空   
    
    
select from quote where code=`AL,date=2012.02.21
factor
select date,adj_dev from factor where code=`AU
ag:select date,adj_dev from factor where code=`AG
ag
ag_adj_yield:select date,adj_yield from factor where code=`AG
ag_adj_yield
sdev -86#exec adj_yield from ag_adj_yield

select from trade
select by product from trade

select datetime,return from pperf where datetime>=2010.01.01

select datetime,return from performance where code=`AU
select from performance where code=`AU

select from performance

select sum cp,sum hp,fee,slip,margin,pos_cost,net_profit,amt,drawdown_amt by datetime from (select by datetime,product from trade)
select sum vol,sum price by datetime from (select by datetime,product from trade)

select datetime,return,pos_cost from pperf
select datetime,return,pos_cost from ht_summary

select datetime, return from pperf
select datetime, return from pperf where datetime>=2010.01.01
select datetime, return from pperf where datetime>=2017.01.01,datetime<=2018.01.01
select datetime, return from pperf where datetime>=2016.01.01,datetime<=2017.01.01
select datetime, return from pperf where datetime.year=

select from perf where code=`AU

select datetime, return from pperf
select from pperf
select from avg_corr
d:exec corr_avg from avg_corr where not null corr_avg
d:exec corr_factor from avg_corr where not null corr_factor
(avg d),(max d),(min d),sdev d
select datetime,return,pos_cost from pperf
select avg pos_cost from pperf

select from quote where code=`IF
select from trade where code=`AG
select from perf where code=`AG
select datetime,return,drawdown from pperf 
select from factor where code=`AG
select from factor where code=`AG
select date,volume from factor where code=`AG

select from score_filtered where date=2000.08.08
select from socre_filtered where 
select from score_filtered where date>=2010.07.01
select from weight_vol
select from weight_vol_mul where date>=2010.07.01
select from amt where date>=2010.07.01

select from factor where code=`WR,date>=2011.03.21
select from quote where code=`WR
select from trade where code=`B
select date,WR from amt where WR>0
select from factor where code=`BU,date>=2014.04.30

select from factor where code=`WR,date=2010.05.17
select from factor where code=`WR,date>=2011.03.23

select date,WR from amt where (abs WR)>0
select date,WH from amt
select from score

lj[`date xcol select from trade where code=`ZC;1!select from factor where code=`ZC]

select from institution_vol_rank

select from china_mutual_fund_nav

select from fund

lj[select `timestamp$date,ZC from ht_mom_basis where date>2014.01.01;1!select from factor where code=`ZC]
lj[select `timestamp$date,ZC from ht_mom_basis where date>2014.01.01;1!select from factor where code=`TC]
select from quote where code=`TC

select from quote

select datetime,return,drawdown from pperf
a:select datetime,pos_cost%amt from pperf
summary[a[`pos_cost]]
pf

select datetime,return from pperf where datetime.year=2018

select from product where (code= `WS  code=`WH)

select from pperf

select from ht_trad

\l d:/db

a:select datetime,return,return_shift:prev return from pperf
b:select datetime,(return)%return_shift from a
b
count exec return from b where return>1
count b
984%1858
prd 1_ exec return from b

select from df


select from product where code in `WT`PM


select from factor where code=`A,date=1999.06.10
select from quote where code=`A,date within 1999.06.06 1999.06.12
select from factor where code=`AG,date>=2012.11.06
select from quote where code=`AG,date>=2012.07.01,oi>=(max;oi) fby date

ag:select from factor where code=`AG,date>=2012.11.06;
ag:select date,code,contract,settle,secondary_contract,secondary_settle from ag;
ag:lj[ag;1!select contract, lasttrade_date from product];
ag:lj[ag;1!select secondary_contract:contract,secondary_lastttrade_date:lasttrade_date from product];
update roll_return:365* ((log settle)-log secondary_settle)%((`date$secondary_lastttrade_date)-(`date$lasttrade_date)) from ag
ag
select from quote where date=2012.11.06,code=`AG

ag1:ag

ag1:update contract:`AG1306 from ag1
ag1:delete settle,lasttrade_date from ag1
ag1:lj[ag1;2!select date,contract,settle from quote where contract=`AG1306]
ag1:lj[ag1;1!select contract,lasttrade_date from product where contract=`AG1306]
update roll_return:365* ((log settle)-log secondary_settle)%((`date$secondary_lastttrade_date)-(`date$lasttrade_date)) from ag1


c1:`AG1812;c2:`AG1806;day:2012.11.06
c1:`AG1301;c2:`AG1212;day:2012.11.06
roll_return[c1;c2;day]
roll_return:{[c1;c2;day]
    p1:first exec settle from quote where contract = c1,date=day;
    p2:first exec settle from quote where contract = c2,date=day;
    t1:first exec lasttrade_date from product where contract = c1;
    t2:first exec lasttrade_date from product where contract = c2;
    ((log p2)-log p1)*365%((`date$t1)-`date$t2)
}

roll_return[`AG1301;`AG1306;2012.11.06]
roll_return[`AG1301;`AG1306;2012.11.07]
`oi xdesc select from quote where code=`AG,date=2012.11.06

roll_return[`AG1301;`AG1212;2012.11.07]
roll_return[`AG1306;`AG1312;2013.02.22]
roll_return[`AG1312;`AG1306;2013.05.06]

roll_return[`AU1812;`AG1806;2018.01.31]
roll_return[`AU1812;`AU1806;2018.02.02]

select date,code,contract from factor where code=`AG
select from factor where code=`AG


key_tab
select date,roll_return_near_far from key_tab where date>=2012.11.06
select from quote where date=2013.05.06,code=`AG
select date,roll_return_near_far from key_tab where date>=2010.01.08
select from key_tab where date>=2010.01.08
`oi xdesc select from quote where code=`AL,date=2010.01.08


期货数据明细：展期收益率：2018.02.02 0.05496
次主力:AG1812,主力AG1806
收盘价：3932 3824
结算价：3931 3823
交割日：2018.12.24 2018.06.25
相差天数：182
((log 3824)-log 3932)*365%185 = -0.0558554
((log 3823)-log 3931)*365%185

select date,mom_basis_near_far from factor where code=`AG,date>=2013.03.06
select date,mom_basis_near_far from factor where code=`AL,date>=2010.07.05

select from warehouse_receipt where code=`AL