load_cta_fee:{[x]
    $[-11h=type x;fpath:hsym x;fpath:hsym `$x];  
    ("SFFFFFFFF"; enlist ",") 0: fpath   
};
product_list
10#daily
10#mx
//trade
load_trade_csv:{[x] //x:"d:/cta/traded" 
    $[-11h=type x;dir:hsym x;dir:hsym `$x];  
    filelist:key dir;    
    trade:raze {[x;y]
        fpath: ` sv x,y;
        prod:`$(string y)[til (string y)?"_"];  //AG_traded.csv--->AG
        d:("DSSFFS"; enlist ",") 0: fpath;
        update product:prod from d
    }[dir] each filelist;
    `datetime xcol trade
};

//performance
load_perf_csv:{[x]    //x:"d:/cta/performance" 
    $[-11h=type x;dir:hsym x;dir:hsym `$x];  
    filelist:key dir;
    perf:raze {[x;y]
        fpath: ` sv x,y;
        prod:`$(string y)[til (string y)?"_"];  //AG_detail.csv--->AG
        d:("DFFFFFFFFFF"; enlist ",") 0: fpath;
        update product:prod from d
    }[dir] each filelist;
    perf:`datetime xcol perf;    //rename first col to datetime
    perf where not perf~ ' prev perf        //drop redundant day
};
load_perf_summary:{[x]
    $[-11h=type x;fpath:hsym x;fpath:hsym `$x];  
    s:("DFFFFFFFFFF"; enlist ",") 0: fpath;
    `datetime xcol s
};

load_amt:{[x]
    $[-11h=type x;dir:hsym x;dir:hsym `$x];  
    amt:("DFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"; enlist ",") 0: dir;
    amt:`datetime xcol amt;
    (`${[x]x[til x?"."]}each string (cols amt)) xcol amt        
};

load_close_c:{[x]   //x:`:d:/cta/mx.csv
    $[-11h=type x;fpath:hsym x;fpath:hsym `$x];  
    d:("DSFFFFFFFFFFFFFFFFF"; enlist ",") 0: fpath;    
    p:exec distinct prod from d;
    data:()!();
    data[`datetime]:exec date from d where prod=p[0];
    r:raze {[x;y] tmp:exec close_c from x where prod=y;tmp1:()!();tmp1[y]:tmp;tmp1 }[d] each exec distinct prod from d;
    flip data,r    
};


fee_tbl:load_cta_fee["d:/cta/cta_fee.csv"];
score:load_amt["d:/cta/score.csv"];
amt:load_amt["d:/cta/amt.csv"];
amt_norm:load_amt["d:/cta/amt_norm.csv"];
trade:load_trade_csv["d:/cta/traded"];
perf:load_perf_csv["d:/cta/performance"];
summary:load_perf_summary["d:/cta/perf_summary.csv"];
close_c:load_close_c["d:/cta/mx.csv"];
c:load_amt["d:/cta/s.csv"];
nw:load_amt["d:/cta/nw.csv"];
w:load_amt["d:/cta/w.csv"];
s:load_amt["d:/cta/s.csv"];

amt
sum(abs(value (1!amt)[2010.07.06]))
10#daily
amt
summary
p:`A
select from trade where product=p
tmp:ej[`datetime;select datetime,pos,avgpx from perf where product=p;select from trade where product=p]
tmp:ej[`datetime;tmp;select datetime,A from amt]

qq:10 #`date`contract`product xcols daily
qq
y:`A9901.DCE
`$(string y)[til (string y)?"."]

select  (string contract) from qq
qq[`contract]:{[x] `$(string x)[til (string x)?"."]}each exec contract from qq
qq[`product]:{[x] `$(string x)[til (string x)?"."]}each exec product from qq
select contract from qq

-10#c
select datetime,WR from c
select datetime,WR from close_c
select datetime,WR from c
select datetime,AU from nw
select datetime,AU from amt_norm
trade
perf
perf_20170406:select from perf where datetime=2017.04.06
perf
trade
select datetime,cp from perf
select datetime, cp from perf where cp=max(cp)

select datetime,return from perf where product=`L
ej[`datetime;select from perf where datetime within 2010.08.02 2010.08.04,product=`L;select from trade where product=`L,datetime within 2010.08.02 2010.08.04]
select from daily where product=`L.DCE,date within 2010.07.25 2010.08.05
select datetime,L from amt where datetime within 2010.07.02 2010.08.05
select from perf where datetime within 2010.07.02 2010.08.04,product=`L
select from trade where product=`L,datetime within 2010.07.02 2010.08.04

select datetime, pos from perf where product=`AG

v:ej[`datetime;select datetime,w:AU from w;select datetime,nw:AU from nw]
v:ej[`datetime;v;select datetime,v:AU from s]

select from perf where datetime<2010.11.15


select from pperf
select from score_filtered where date>=2010.07.01

select datetime, pos_cost%100000000 from pperf
select datetime.date,return,drawdown from pperf

avg select pos_cost%100000000 from pperf
max select pos_cost%100000000 from pperf
min select pos_cost%100000000 from pperf
select from trade where code=`AG
select datetime,return from perf where code=`ZC

select return by code,datetime from perf
select from perf_cross where date.month=2018.01m
sum exec cp from pperf
select from amt where date>=2018
select from perf where code=`A

select from trade where code=`AG
select by datetime from perf where code=`AG
select from weight_vol
select from weight_vol_mul

select from vol_cross
select from score_cross
tmp:select adj_log_return,adj_vol,adj_dev,adj_var,adj_tr from factor where code=`AG
summary exec adj_dev from factor where code=`AL
{[tmp;c]summary  tmp[c]}[tmp] each cols tmp 
summary exec pos_cost%100000000 from pperf

select datetime,return:return-0.56 from pperf where datetime.year=2010


\l d:/db
xcode:`AG;start_date:2012.11.06

parse "select date,AG from amt where not null AG"
?[amt;();0b;`date`AG!`date`AG]

check_perf:{[xcode;start_date]
    c:(enlist (>;`date;start_date));       
    b:();
    a:`date`amt!(`date,xcode);
    t_amt:flip ?[amt;c;b;a];
    
    c:(enlist (>;`date;start_date));       
    b:();
    a:`date`score!(`date,xcode);
    t_score:flip ?[score;c;b;a];
    
    ta:lj[t_amt;1!t_score];
    
    tb:ej[`date;ta;1!`date xcol select from trade where code=xcode,datetime>start_date];
    tc: `date xcol 0!select by datetime from perf where code=xcode;
    tc: select date,avgpx,pos,cp,hp,fee,pos_cost,net_profit from tc;
    ej[`date;tb;0!tc]
}

check_perf[`RB;2010.11.06]


select date,contract,open from quote where code=`AG,date=2012.11.07
select date,contract,open from factor where code=`AG

select from factor where code=`AG

a:select by code from factor
b:lj[a;1!select code,multiplier,pxunit from product]
c:select code,date,contract,settle,pxunit,multiplier from b
c
update amt:settle*multiplier from `c
update slipunit:10000*pxunit%settle from `c
c

idx:exec i from product where code=`JD
//dbdir:"d:/db";tablename:"product";row:0;col:"pxunit";val:1.0;
updateentry[dbdir;"product";idx;"pxunit";1.0;log_path]
select from product where code=`JD

c
select from fee

select from amt
select from product

ta:select from product where code = `A
tb:select from product

keytab:select code,contract from ta
keytab

exec i from tb where ([]code;contract) in keytab
p:parse "exec i from tb where ([]code;contract) in keytab"
p[2]
pl :`code`contract
?[tb;(enlist ("in";("+:";("!";pl;(pl)));`keytab));();`i]

(parse "select from product where code=`A")[2]
?[product;(enlist(>;`code;`A));0b;()]
select from product
,,("=";`code;,`A)
(enlist("=";`code;enlist`A))

select from factor where code=`ZC,date>2013.07.01
 c:(enlist (>;`date;start_date));       
 b:();
 a:`date`amt!(`date,xcode);
 ?[amt;c;0b;()]
 ?[amt;(enlist (>;`date;start_date));0b;()]
 select from amt
 xcode
 parse "select from product where code>xcode"
 
 ?[product;(enlist (>;`code;`xcode));0b;()]
 
 select from trade where code=`AG
 select date,mom_time from factor where code=`A
 select by datetime from perf
 select  from perf
 a:select datetime from select by datetime from perf
 raze {[x;y]lj[x;select datetime,return from perf where code=y];x }[a] each distinct perf[`code]
 select datetime,return from perf where code=`A
 