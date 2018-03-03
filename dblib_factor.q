
.factor.main_contract:{[x;start_date;end_date];data : (select date,contract,oi from quote where code=x,oi>=(max;oi) fby date);     cl :data[`contract];    p:1!select contract,dlmonth from product;    r:cl;    last_mc:cl[0];    i:0;    n:count cl;        while[i<n;         r[i]:last_mc;        if[(i>2) & (cl[i-1]=cl[i-2]) & (cl[i-2]<>last_mc) & (p[cl[i-1]][`dlmonth]>p[last_mc][`dlmonth]);                                    last_mc:cl[i-1];                    ];                i+:1;    ];        data: update main_contract:r from data;    select date,main_contract from data where date>start_date,date<=end_date};
.factor.close:{[key_tab]    key_tab:`date`contract xcol key_tab;    min_date:min key_tab[`date];    max_date:max key_tab[`date];    exec close from ej[`date`contract;key_tab;select date,contract,close from quote where date>=min_date,date<=max_date]};


// schema
.schema.factor:(
    []date:`timestamp$();code:`symbol$();contract:`symbol$();open:`float$();high:`float$();low:`float$();close:`float$();oi:`float$();settle:`float$();volumne:`float$();amt:`float$();
    adjfactor:`float$();adjclose:`float$();
    secondary_contract:`symbol$();secondary_settle:`float$();
    nearest_contract:`symbol$();nearest_settle:`float$();
    near_contract:`symbol$();near_settle:`float$();
    far_contract:`symbol$();far_settle:`float$() );
factor:.schema.factor;

/ upserttable[dbdir;"factor";factor]

// 计算[start_date end_date]闭区间的所有因子值,注意赋值顺序，
.factor.wrap:{[xcode;start_date;end_date]
    key_tab:.factor.main[xcode;start_date;end_date];
    key_tab^:.factor.adjfactor[key_tab];      
    key_tab^:.factor.nearest_contract[key_tab]; 
    key_tab^:.factor.near_far[key_tab];   
    key_tab^:.factor.mom_time[key_tab;`factor;`adjclose;40];
};




// 计算xcode在[start_date end_date]的主力合约，并与行情表左连接
// 注意start_date之前的因子数据必须是全部计算好的，增量计算因子时都需要注意此条件。
// 返回类型：table(98h),可能为空表

.factor.main:{[xcode;start_date;end_date];     //    
    init_trading_day:min exec date from quote where code=xcode; //最早交易日    
    yc:exec -3# contract from select [-3] contract from quote where date<start_date,code=xcode,oi>=(max;oi) fby date;     //  最大持仓的合约 
    if[any null yc;yc:-3#exec contract from quote where date=init_trading_day,code=xcode,oi>=max(oi)];                    //  无历史数据
    c_y:yc[2];c_yy:yc[1];c_yyy:yc[0];   //昨天  前天 大前天最大持仓的合约 
    
    if[null mc:last exec contract from select [-1] contract from factor where date<start_date,code=xcode;mc:c_y];  //当前（上一个）主力合约
  
    r:();   //to return    
    n:count key_tab:(select date,code,contract from quote where date>=start_date,date<=end_date,code=xcode,oi>=(max;oi) fby date);     //  最大持仓的合约
    i:0;
    p:1!select contract,dlmonth from product where code=xcode;
    while[i<n;
        if[(c_yy=c_yyy) & (c_yy<>mc) & p[c_yy][`dlmonth]>p[mc][`dlmonth];mc:c_yy];
        r,:mc;
        c_yyy:c_yy;c_yy:c_y;c_y:key_tab[i][`contract];
        i+:1;
    ];
     
    key_tab: update contract:r from key_tab;    
    :lj[key_tab;3!select from quote where date>=start_date,date<=end_date,code=xcode];    
};



// 计算主力合约复权因子和复权价格
//复权因子(T)=复权因子(T-1)*旧主力合约前收盘价/新主力合约前收盘价
//主力合约复权收盘价(T)=主力合约收盘价*复权因子(T)
// 返回主力合约复权因子和复权收盘价
// 返回类型：table(98h),可能为空表
.factor.adjfactor:{[key_tab]          
    xcode:key_tab[`code][0];
    ref: select [-1] date,contract,close, adjfactor from factor where date<min key_tab[`date],code=xcode;   //前值        
    if[0=count ref;ref:select [1] date,contract,close,adjfactor:1.0 from key_tab];
    tbl:ref,select date,contract,close,adjfactor:1.0 from key_tab;
    t:lj[fills update contract_shift:next contract from tbl;2!select date,contract_shift:contract,close_shift:close from quote where date>=min tbl[`date],date<=max tbl[`date],code=xcode];
    adj:1 _prev exec adjfactor from update adjfactor:adjfactor*close%close_shift from t where contract_shift<>contract;
    adj:1.0 *\ adj;    
    ([]adjfactor:adj;adjclose:adj*exec close from key_tab)    
};

// 最近月,包含交割月, 
// 当前月还在交易则返回当前月，否则返回下月
// 返回:最近月和结算价
// 返回值类型：key_tab 为空，或者key不存在则返回空的list(0h)，否则返回table(98h)
.factor.nearest_contract:{[key_tab]     
     nc:raze{
     k:`lasttrade_date xasc select contract,lasttrade_date from product where code=x[`code],(`month$lasttrade_date)>=`month$x[`date];
     $[0=count exec settle from quote where contract=k[0][`contract],date=x[`date];c:k[1][`contract];c:k[0][`contract]];     
     :c
     }each select date,code from key_tab;
     :select nearest_contract:contract,nearest_settle:settle from  lj[select date,contract:nc from key_tab;2!select date,contract,settle from quote where date>=min key_tab[`date],date<=max key_tab[`date],code=key_tab[`code][0]];
};

// 持仓量第大的合约
// 返回合约和结算价
// 返回值类型：key_tab 为空，或者key不存在则返回空的list(0h)，否则返回table(98h)
.factor.first_contract:{[key_tab]  
    sc:raze { (`oi xdesc select contract,oi from quote where date=x[`date],code=x[`code])[`contract][0] }each select date,code from key_tab;
    select first_contract:contract,first_settle:settle from lj[select date,contract:sc from key_tab;2!select date,contract,settle from quote where date>=min key_tab[`date],date<=max key_tab[`date],code=key_tab[`code][0]]
};

// 持仓量第二大的合约
// 返回合约和结算价
// 返回值类型：key_tab 为空，或者key不存在则返回空的list(0h)，否则返回table(98h)
.factor.secondary_contract:{[key_tab]  
    sc:raze { (`oi xdesc select contract,oi from quote where date=x[`date],code=x[`code])[`contract][1] }each select date,code from key_tab;
    select secondary_contract:contract,secondary_settle:settle from lj[select date,contract:sc from key_tab;2!select date,contract,settle from quote where date>=min key_tab[`date],date<=max key_tab[`date],code=key_tab[`code][0]]
};

// 计算最近且持仓量最大的两个合约,用于期限结构因子
.factor.near_far:{[key_tab]
    nf:raze {2#`oi xdesc select date,contract,oi,settle from quote where date=x[`date],code=x[`code] }each select date,code from key_tab;
    nf:lj[nf;1!select contract,lasttrade_date from product];    
    n:select near_contract:contract,near_settle:settle,near_lasttrade_date:lasttrade_date from nf where  lasttrade_date=(min;lasttrade_date) fby date;
    f:select far_contract:contract,far_settle:settle,far_lasttrade_date:lasttrade_date from nf where  lasttrade_date=(max;lasttrade_date) fby date;   
    :update roll_return_near_far:365*((log near_settle)-log far_settle)%((`date$far_lasttrade_date)-`date$near_lasttrade_date) from n^f;    
};


// 计算c列的动量，(close[0]-close[n])%close[n]
// c为列名，symbol
// t为参考的表格，symbol
// n为回看周期
// 先计算close[n]列，并与close[0]列合并，在计算动量

.factor.mom_time:{[key_tab;t;c;n]
    listc:?[key_tab;();();c];      //从 key_tab中选择c列数据，
    conditions:((<;`date;min key_tab[`date]);(=;`code;key_tab[`code][0]));       
    ref:(neg n ) sublist ?[t;conditions;();c];                                  //从factor中选择c列的最后n个值，不足n个则保留有效值,ref 可能为空   
    ref,:listc;                                                                      //合并新旧数据
    ref:(count key_tab) sublist ((n+(count key_tab) -(count ref))#first ref),ref;
    :([]mom_time:(listc-ref)%ref);
};



// 计算展期收益率 期限结构为主力m（主力定义为连续两天持仓量最大，第三天收盘切换，第四天生效）和次主力s（持仓量第二大）
// ((log pm) - log ps)*365%(ns-nm)
/ .factor.roll_return:{[key_tab]
/     m:lj[select contract,settle from key_tab;1!select contract,lasttrade_date from product];
/     s:lj[select contract:secondary_contract,settle:secondary_settle from key_tab;1!select contract,lasttrade_date from product];
/     :([]roll_return:365*((log exec settle from m) - (log exec settle from s))%((`date$exec lasttrade_date from s) -(`date$exec lasttrade_date from m)));   
/ }

// 计算展期收益率, 持仓量最大和持仓量第二大的两个合约的展期收益
.factor.roll_return:{[key_tab]
    m:lj[select contract:first_contract,settle:first_settle from key_tab;1!select contract,lasttrade_date from product];
    s:lj[select contract:secondary_contract,settle:secondary_settle from key_tab;1!select contract,lasttrade_date from product];
    :([]roll_return:365*((log exec settle from m) - (log exec settle from s))%((`date$exec lasttrade_date from s) -(`date$exec lasttrade_date from m)));   
};
