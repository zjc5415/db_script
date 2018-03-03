// 各个表的索引
/ sortandsetp[`:d:/db/product;`code`dlmonth;log_path]   //succeed
/ sortandsetp[`:d:/db/quote;`contract`date;log_path]    //succeed
/ sortandsetp[`:d:/db/factor;`code`date;log_path]    //succeed
/ sortandsetp[`:d:/db/warehouse_receipt;`code`date;log_path]    //succeed

//schema
.schema.factor:(
    []date:`timestamp$();code:`symbol$();contract:`symbol$();open:`float$();high:`float$();low:`float$();close:`float$();oi:`float$();settle:`float$();volume:`float$();amt:`float$();
    adjfactor:`float$();adjclose:`float$();
    first_contract:`symbol$();first_settle:`float$();
    secondary_contract:`symbol$();secondary_settle:`float$();
    nearest_contract:`symbol$();nearest_settle:`float$();
    near_contract:`symbol$();near_settle:`float$();near_lasttrade_date:`timestamp$();
    far_contract:`symbol$();far_settle:`float$();far_lasttrade_date:`timestamp$();
    roll_return_near_far:`float$();
    mom_time:`float$();
    daily_yield_near:`float$();daily_yield_far:`float$();mom_basis_near_far:`float$();
    mom_warehouse_receipt:`float$();
    adj_yield:`float$();
    adj_ema:`float$();
    adj_dev:`float$();
    adj_var:`float$();
    adj_tr:`float$()
)

dbdir:"d:/db"
upserttable[dbdir;"factor";.schema.factor]
/ @[`:d:/db/factor;`date;`s#] //  第一次写入数据之前设置属性，s-fail

//(meta key_tab)=meta .schema.factor  // check schema
// sliding window, 
sw:{{1 _ x, y}\[x#0;y]}

select from factor where code=`ER
select from product where code=`ER
start_date:exec min date from quote;end_date:exec max date from quote;xcode:`AL;
start_date:2017.01.01;end_date:exec max date from quote;xcode:`AL;
start_date:2010.01.01;end_date:2018.02.21;xcode:`AG;
start_date:1984.01.01;end_date:2018.02.21;xcode:`AL;
start_date:1984.01.01;end_date:2018.02.21;xcode:`A;
start_date:1984.01.01;end_date:2018.02.21;xcode:`ER;
start_date:1984.01.01;end_date:2018.02.21;xcode:`ZC;
// 计算[start_date end_date]闭区间的所有因子值,注意赋值顺序，
.factor.wrap:{[xcode;start_date;end_date]
    key_tab:.factor.main[xcode;start_date;end_date];
    if[0=count key_tab;:key_tab];
    key_tab^:.factor.adjfactor[key_tab];        // 注意使用^合并是，不能有相同的列，否则应该用lj
    key_tab^:.factor.first_contract[key_tab];
    key_tab^:.factor.secondary_contract[key_tab];
    key_tab^:.factor.nearest_contract[key_tab];
    key_tab^:.factor.near_far1[key_tab];        // 持仓量最大的两个合约
    key_tab^:.factor.mom_time[key_tab;`factor;`adjclose;40];
    key_tab^:.factor.mom_basis2[key_tab;120];   // 最近月，远月且持仓量最大
    key_tab^:.factor.mom_warehouse_receipt[key_tab;89];
    key_tab^:.factor.vix[key_tab;86;0.94];    
    :key_tab;
}
key_tab
select from quote where code=`AL,date=2012.02.21
select from key_tab where date=1996.05.16
select date,near_contract,near_settle,near_lasttrade_date,far_contract,far_settle,far_lasttrade_date,roll_return_near_far,mom_basis_near_far from key_tab where date>=2010.01.08
select date,near_contract,near_settle,near_lasttrade_date,far_contract,far_settle,far_lasttrade_date,roll_return_near_far,mom_basis_near_far from factor where date>=2010.01.08,code=`AL
select date,near_contract,near_settle,near_lasttrade_date,far_contract,far_settle,far_lasttrade_date,roll_return_near_far,mom_basis_near_far from factor where date>=2012.11.06,code=`AG
select from quote where code=`AG,date=2012.11.06
select from quote where code=`AL,date=2010.01.08
select date,code,mom_warehouse_receipt from key_tab where date>=2010.01.04,code=`AL
//(meta to_append)=meta .schema.factor
.factor.build:{[start_date;end_date]    
    {[start_date;end_date;xcode]
        0N!xcode;
        to_append:.factor.wrap[xcode;start_date;end_date];
        if[0=count to_append;:`];
        // 按date,code去重
        old:select date,code from factor where code=xcode,date>=start_date,date<=end_date;
        dups:exec i from to_append where ([]date;code) in old;
        if[0<count dups;to_append:select from to_append where not i in dups];  
        //   
        upserttable[dbdir;"factor";to_append]; 
        sortandsetp[`:d:/db/factor;`code`date;log_path]   //succeed
    }[start_date;end_date] each distinct exec code from product where exch<>`CFE;   // CFE有两个仿真合约，AF1802-S/EF1802-S,
}

\t .factor.build[1984.01.01;2084.01.01] / 775589ms

\t .factor.build[2018.02.03;2018.02.21]

// 计算xcode在[start_date end_date]的主力合约，并与行情表左连接
// 主力合约计算方法：连续两天持仓量最大，第三天收盘切换，第四天开盘生效
// 注意start_date之前的因子数据必须是全部计算好的，增量计算因子时都需要注意此条件。
// 返回类型：table(98h),可能为空表

.factor.main:{[xcode;start_date;end_date]
    init_trading_day:min exec date from quote where code=xcode; // 最早交易日    
    yc:exec -3# contract from select [-3] contract from quote where date<start_date,code=xcode,oi>=(max;oi) fby date;     //  最大持仓的合约 
    if[any null yc;yc:-3#exec contract from quote where date=init_trading_day,code=xcode,oi>=max(oi)];                    //  无历史数据
    c_y:yc[2];c_yy:yc[1];c_yyy:yc[0];   // 昨天  前天 大前天最大持仓的合约 
    
    if[null mc:last exec contract from select [-1] contract from factor where date<start_date,code=xcode;mc:c_y];  //当前（上一个）主力合约
  
    r:();   //to return    
    key_tab:(`date xasc select date,code,contract from quote where date>=start_date,date<=end_date,code=xcode,oi>=(max;oi) fby date);     //  最大持仓的合约
    key_tab:0!select by date from key_tab;    //如果最大持仓量的合约有多个，去除重复的保留第一条
    n:count key_tab;
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

key_tab:.factor.main[`AG;2017.01.01;.z.d-3]
xcode:`AG
oldc:`AG1209
newc:`AG1212
oldd:2012.07.25;
newd:2012.07.26
.factor.adjfactor[`AG;key_tab]
(exec last close from quote where contract=newc,date=newd)*(exec last close from quote where contract=oldc,date=oldd)%(exec last close from quote where contract=newc,date=oldd)
(exec last close from quote where contract=oldc,date=newd)
(exec last close from quote where contract=newc,date=oldd)
(exec last close from quote where contract=newc,date=newd)

// 计算主力合约复权因子和复权价格
// 复权因子(T)=复权因子(T-1)*旧主力合约前收盘价/新主力合约前收盘价
// 主力合约复权收盘价(T)=主力合约收盘价*复权因子(T)
// 切换时，复权收益率：(po1/pn1)*pn2/po1 = pn2/pn1， 可见复权收益率是一致的
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
}

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
}

// 持仓量第大的合约
// 返回合约和结算价
// 返回值类型：key_tab 为空，或者key不存在则返回空的list(0h)，否则返回table(98h)
.factor.first_contract:{[key_tab]  
    sc:raze { (`oi xdesc select contract,oi from quote where date=x[`date],code=x[`code])[`contract][0] }each select date,code from key_tab;
    select first_contract:contract,first_settle:settle from lj[select date,contract:sc from key_tab;2!select date,contract,settle from quote where date>=min key_tab[`date],date<=max key_tab[`date],code=key_tab[`code][0]]
}

// 持仓量第二大的合约
// 返回合约和结算价
// 返回值类型：key_tab 为空，或者key不存在则返回空的list(0h)，否则返回table(98h)
.factor.secondary_contract:{[key_tab]  
    sc:raze { (`oi xdesc select contract,oi from quote where date=x[`date],code=x[`code])[`contract][1] }each select date,code from key_tab;
    select secondary_contract:contract,secondary_settle:settle from lj[select date,contract:sc from key_tab;2!select date,contract,settle from quote where date>=min key_tab[`date],date<=max key_tab[`date],code=key_tab[`code][0]]
}

//展期收益率： 持仓量最大的两个合约
.factor.near_far1:{[key_tab]
    nf:raze {2#`oi xdesc select date,contract,oi,settle from quote where date=x[`date],code=x[`code] }each select date,code from key_tab;   //选择oi最大的两个合约, 可能只有一个合约，退市时，ER
    nf:lj[nf;1!select contract,lasttrade_date from product];    
    n:select date,near_contract:contract,near_settle:settle,near_lasttrade_date:lasttrade_date from nf where  lasttrade_date=(min;lasttrade_date) fby date;
    n:select near_contract,near_settle,near_lasttrade_date from n where differ n; // 只有一个合约时会出现重复的    如ER
    f:select date,far_contract:contract,far_settle:settle,far_lasttrade_date:lasttrade_date from nf where  lasttrade_date=(max;lasttrade_date) fby date;   
    f:select far_contract,far_settle,far_lasttrade_date from f where differ f; // 只有一个合约时会出现重复的    如ER
    :update roll_return_near_far:365*((log near_settle)-log far_settle)%((`date$far_lasttrade_date)-`date$near_lasttrade_date) from n^f;    
}
//展期收益率： 最大持仓量合约，远月且持仓量最大的合约
.factor.near_far2:{[key_tab]
    //选择oi最大的合约    
    max_oi:raze {1#`oi xdesc select date,code,contract,oi,settle from quote where date=x[`date],code=x[`code] }each select date,code from key_tab;   
    max_oi:lj[max_oi;1!select contract,lasttrade_date from product];
    //选择远月且oi最大的合约
    next_oi:raze {   tmp:select date,code,contract,oi,settle from quote where date=x[`date],code=x[`code];
        tmp:lj[tmp;1!select contract,lasttrade_date from product];
        1#`oi xdesc select date,contract,oi,settle,lasttrade_date from tmp where lasttrade_date>x[`lasttrade_date]
    }each max_oi;
    next_oi:delete from next_oi where null date;
    to_upsert:select date,contract,oi,settle,lasttrade_date  from max_oi where not date in next_oi[`date];  // 只有一个合约,用max_oi替代
    next_oi:(1!next_oi) upsert 1!to_upsert;
    next_oi:0!next_oi;
    next_oi:`date xasc select from next_oi;     // delele,upsert之后，排序已经乱了
    n:select near_contract:contract,near_settle:settle,near_lasttrade_date:lasttrade_date from max_oi;    
    f:select far_contract:contract,far_settle:settle,far_lasttrade_date:lasttrade_date from next_oi;    
    :update roll_return_near_far:365*((log near_settle)-log far_settle)%((`date$far_lasttrade_date)-`date$near_lasttrade_date) from n^f;    
}

// key_tab主力合约，与远月且持仓量最大的合约
.factor.near_far3:{[key_tab]
    //选择主力合约
    max_oi:select date,code,contract,oi,settle from key_tab;
    max_oi:lj[max_oi;1!select contract,lasttrade_date from product];
    //选择远月且oi最大的合约
    next_oi:raze {   tmp:select date,code,contract,oi,settle from quote where date=x[`date],code=x[`code];
        tmp:lj[tmp;1!select contract,lasttrade_date from product];
        1#`oi xdesc select date,contract,oi,settle,lasttrade_date from tmp where lasttrade_date>x[`lasttrade_date]
    }each max_oi;
    next_oi:delete from next_oi where null date;
    to_upsert:select date,contract,oi,settle,lasttrade_date  from max_oi where not date in next_oi[`date];  // 只有一个合约,用max_oi替代
    next_oi:(1!next_oi) upsert 1!to_upsert;
    next_oi:0!next_oi;
    next_oi:`date xasc select from next_oi;     // delele,upsert之后，排序已经乱了
    
    n:select near_contract:contract,near_settle:settle,near_lasttrade_date:lasttrade_date from max_oi;    
    f:select far_contract:contract,far_settle:settle,far_lasttrade_date:lasttrade_date from next_oi;
    
    :update roll_return_near_far:365*((log near_settle)-log far_settle)%((`date$far_lasttrade_date)-`date$near_lasttrade_date) from n^f;    
}

// 计算c列的动量，(close[0]-close[n])%close[n]
// c为列名，symbol
// t为参考的表格，symbol
// n为回看周期
// 先计算close[n]列，并与close[0]列合并，在计算动量
/ t:`factor
/ .factor.mom_time[key_tab;t;c;40]
.factor.mom_time:{[key_tab;t;c;n]
    listc:?[key_tab;();();c];      //从 key_tab中选择c列数据，
    conditions:((<;`date;min key_tab[`date]);(=;`code;key_tab[`code][0]));       
    ref:(neg n ) sublist ?[t;conditions;();c];                                  //从factor中选择c列的最后n个值，不足n个则保留有效值,ref 可能为空   
    ref,:listc;                                                                      //合并新旧数据
    ref:(count key_tab) sublist ((n+(count key_tab) -(count ref))#first ref),ref;
    :([]mom_time:(listc-ref)%ref);
}



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
}


.quote.refine:{    //remove null settle, ffill close for quote
    :raze {t:select from quote where contract=x,not null settle;t[`close]:fills t[`close];:t }each distinct exec contract from quote;
}

\l d:/db_script/dblib.q

key_tab:.factor.main[`AG;2017.01.01;.z.d-3]
nc:.factor.nearest_contract[key_tab]
sc:.factor.secondary_contract[key_tab]
adj:.factor.adjfactor[key_tab]

tbl:key_tab
tbl[`nearest_contract]:nc
tbl[`secondary_contract]:sc
tbl[`nearest_settle]:exec settle from quote where ([]date;contract) in select date,contract:nearest_contract from tbl
tbl[`secondary_settle]:exec settle from quote where ([]date;contract) in select date,contract:secondary_contract from tbl


tmp:select date,contract,secondary_contract from tbl
r:{roll_return[x[`secondary_contract];x[`contract];x[`date]]}each tmp

key_tab:tbl

// 计算基差动量, 最近月减最大持仓, 近月减远月
// 累乘(1+daily return of near) - 累乘(l+daily return of far)
// n为基差累乘的周期数 n:120
// care:两个合约可能重合，如ZC，前期很多重合，导致基差动量为0
.factor.mom_basis1:{[key_tab;n]
    xcode:key_tab[`code][0];
    // 计算最近月的日收益
    t:(select [neg n+1] date,nearest_contract,nearest_settle from factor where code=xcode,date<min key_tab[`date]),select date,nearest_contract,nearest_settle from key_tab;    
    t:update ref_contract:next nearest_contract from t;    
    t:lj[t;2!select date,ref_contract:contract,ref_settle:settle from quote where code=xcode,date>=min t[`date],date<=max t[`date]];    //合约切换时对齐到上一个合约
    ny:exec 1.0^(nearest_settle%prev ref_settle) from t;    
    // 近月的基差(前n个收益相乘), prd sliding window
    nb:prd each {{1 _ x, y}\[x#1.0;y]}[n;ny];    
                
    // 计算最大持仓量合约的日收益
    t:(select [neg n+1] date,far_contract:first_contract,far_settle:first_settle from factor where code=xcode,date<min key_tab[`date]),select date,far_contract:first_contract,far_settle:first_settle from key_tab;    
    t:update ref_contract:next far_contract from t;    
    t:lj[t;2!select date,ref_contract:contract,ref_settle:settle from quote where code=xcode,date>=min t[`date],date<=max t[`date]];
    fy:exec 1.0^(far_settle%prev ref_settle) from t;
    // 远月的基差(前n个收益相乘), prd sliding window
    fb:prd each {{1 _ x, y}\[x#1.0;y]}[n;fy];     
    
    :(neg count key_tab)#([]daily_yield_near:ny;daily_yield_far:fy;mom_basis_near_far:nb-fb);    // 只去最后需要的数据
};

// 计算基差动量, 最近月合约，除最近月外最大持仓的合约, 近月减远月，
// 累乘(1+daily return of near) - 累乘(l+daily return of far)
// n为基差累乘的周期数 n:120
// care:两个合约可能重合，如ZC，前期很多重合，导致基差动量为0
.factor.mom_basis2:{[key_tab;n]
    xcode:key_tab[`code][0];
    // 计算最近月的日收益
    t:(select [neg n+1] date,nearest_contract,nearest_settle from factor where code=xcode,date<min key_tab[`date]),select date,nearest_contract,nearest_settle from key_tab;
    t:update ref_contract:next nearest_contract from t;    
    t:lj[t;2!select date,ref_contract:contract,ref_settle:settle from quote where code=xcode,date>=min t[`date],date<=max t[`date]];    //合约切换时对齐到上一个合约
    ny:exec 1.0^(nearest_settle%prev ref_settle) from t;    
    // 近月的基差(前n个收益相乘), prd sliding window
    nb:prd each {{1 _ x, y}\[x#1.0;y]}[n;ny];    
                
    //选择远月且oi最大的合约,可能只有一个合约
    t[`code]:xcode;
    next_oi:raze {   tmp:select date,code,contract,oi,settle from quote where date=x[`date],code=x[`code],contract<>x[`nearest_contract];
        tmp:lj[tmp;1!select contract,lasttrade_date from product];
        1#`oi xdesc select date,contract,oi,settle,lasttrade_date from tmp where lasttrade_date>x[`lasttrade_date]
    }each t;
    // 如果只有一个合约，则使用最近月替代    
    tf:(select date,nearest_contract,nearest_settle from t)^select date,far_contract:contract,far_settle:settle from next_oi;
    tf:update far_contract:nearest_contract,far_settle:nearest_settle from tf where null far_contract;    
    
    tf:update ref_contract:next far_contract from tf;        
    tf:lj[tf;2!select date,ref_contract:contract,ref_settle:settle from quote where code=xcode,date>=min tf[`date],date<=max tf[`date]];
    fy:exec 1.0^(far_settle%prev ref_settle) from tf;
    // 远月的基差(前n个收益相乘), prd sliding window
    fb:prd each {{1 _ x, y}\[x#1.0;y]}[n;fy];     
    
    :(neg count key_tab)#([]daily_yield_near:ny;daily_yield_far:fy;mom_basis_near_far:nb-fb);    // 只去最后需要的数据
};

// 计算基差动量, 近月减远月, 近月远月的确认见near_far
// 累乘(1+daily return of near) - 累乘(l+daily return of far)
//  n为基差累乘的周期数 n:120
.factor.mom_basis_near_far:{[key_tab;n]
    xcode:key_tab[`code][0];
    // 计算近月的日收益
    t:(select [neg n+1] date,near_contract,near_settle from factor where code=xcode,date<min key_tab[`date]),select date,near_contract,near_settle from key_tab;    
    t:update ref_contract:next near_contract from t;    
    t:lj[t;2!select date,ref_contract:contract,ref_settle:settle from quote where code=xcode,date>=min t[`date],date<=max t[`date]];    //合约切换时对齐到上一个合约
    ny:exec 1.0^(near_settle%prev ref_settle) from t;    
    // 近月的基差(前n个收益相乘), prd sliding window
    nb:prd each {{1 _ x, y}\[x#1.0;y]}[n;ny];           
     
    // 计算远月的日收益
    t:(select [neg n+1] date,far_contract,far_settle from factor where code=xcode,date<min key_tab[`date]),select date,far_contract,far_settle from key_tab;    
    t:update ref_contract:next far_contract from t;    
    t:lj[t;2!select date,ref_contract:contract,ref_settle:settle from quote where code=xcode,date>=min t[`date],date<=max t[`date]];
    fy:exec 1.0^(far_settle%prev ref_settle) from t;
    // 远月的基差(前n个收益相乘), prd sliding window
    fb:prd each {{1 _ x, y}\[x#1.0;y]}[n;fy];     
    
    :(neg count key_tab)#([]daily_yield_near:ny;daily_yield_far:fy;mom_basis_near_far:nb-fb);    // 只去最后需要的数据
};

// 计算复权价格每个窗口的 对数收益及其ema/dev/var,窗口期n:86，ema衰减系数为c:0.94
// tr:true average:max(high-low,abs(high-prev close),abs(low-prev close))
key_tab
key_tab_c
n:86
count adj_yield
.factor.vix:{[key_tab;n;c]    
    xcode:key_tab[`code][0];
    // todo: 合约切换时prev应该使用新合约
    tab_for_adj_tr:(select [-1] date,contract,high,low,open,close from factor where code=xcode,date<min key_tab[`date]),select date,contract,high,low,open,close from key_tab;
    tab_for_adj_tr:fills update contract_next:next contract from tab_for_adj_tr;
    tab_for_adj_tr:lj[tab_for_adj_tr;2!select date,contract_next:contract,close_next:close from quote where date>=min tab_for_adj_tr[`date],date<=max tab_for_adj_tr[`date],code=xcode];
    adj_tr:max flip select (high-low)%close,abs(high-prev close_next)%close,abs(low-prev close_next)%close from tab_for_adj_tr;    
    adj_tr:(neg count key_tab)#adj_tr;
    
    adj_h:exec adjclose from select [neg n] adjclose from factor where code=xcode,date<min key_tab[`date];
    adj_price:adj_h,exec adjclose from key_tab;
    adj_yield:1 _ deltas log (first adj_price),adj_price;
    adj_ema:last flip ema[1-c] each {{1 _ x, y}\[x#0.0;y]}[n;adj_yield];        // 每个窗口的ema
    adj_dev:dev each {{1 _ x, y}\[x#1.0;y]}[n;adj_yield];                       
    adj_var:var each {{1 _ x, y}\[x#1.0;y]}[n;adj_yield];    
    r:(neg count key_tab)#([]adj_yield:adj_yield;adj_ema:adj_ema;adj_dev:adj_dev;adj_var:adj_var);    
    
    :update adj_tr:adj_tr from r;    
}

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

// 计算仓单变化率的动量，参考周期n为90，实际是n:89
.factor.mom_warehouse_receipt:{[key_tab;n]
    xcode:key_tab[`code][0];
    his_w:select [neg n] from warehouse_receipt where code=xcode,date<min key_tab[`date];    
    new_w:lj[select date,code from key_tab;2!select from warehouse_receipt where code=xcode,date>=min key_tab[`date],date<=max key_tab[`date]];    
    w:his_w,new_w;
    new_n:count key_tab;
    shift_w:new_n#((new_n+n-count w)#1#w),w;    
    wd:update shift_warehouse_receipt:shift_w[`warehouse_receipt] from new_w;
    :select mom_warehouse_receipt:(warehouse_receipt-shift_warehouse_receipt)%shift_warehouse_receipt from wd;
}

//compare 

//ht_mom_time:("DFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"; enlist ",") 0: `:d:/20180206_mom_time.csv;
//upserttable["d:/db";"ht_mom_time";ht_mom_time]
comp_mom_time:{[xcode]
    to_comp:flip ?[ht_mom_time;();();`date`ht!`date,xcode];
    f:select date,code,mom_time from factor where code=xcode,date>=min to_comp[`date];    
    to_comp:update date:`timestamp$date from to_comp;
    :lj[f;1!to_comp];
}
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
comp_roll_return[`RB]

//ht_mom_basis:("DFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"; enlist ",") 0: `:d:/20180206_mom_basis.csv;
//upserttable["d:/db";"ht_mom_basis";ht_mom_basis]
comp_mom_basis:{[xcode]
    to_comp:flip ?[ht_mom_basis;();();`date`ht!`date,xcode];
    f:select date,code,mom_basis_near_far from factor where code=xcode,date>=min to_comp[`date];    
    to_comp:update date:`timestamp$date from to_comp;
    :lj[f;1!to_comp];
}

comp_mom_basis[`ZC]
MA ZC差距较大
B一模一样,但是有大量的nan值

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
comp_mom_warehouse_receipt[`AG]

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

select datetime,margin from pperf