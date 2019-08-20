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
    adj_log_return:`float$();
    adj_vol:`float$();
    adj_dev:`float$();
    adj_var:`float$();
    adj_tr:`float$();
    filter_reason:`float$()
)
dbdir:"d:/db"
upserttable[dbdir;"factor";.schema.factor]

\t .factor.build[1984.01.01;2084.01.01] / 1128079
\t .factor.build[2018.02.03;2018.02.21]
/ @[`:d:/db/factor;`date;`s#] //  第一次写入数据之前设置属性，s-fail

//(meta key_tab)=meta .schema.factor  // check schema
// sliding window, 
sw:{{1 _ x, y}\[x#0;y]}

select from factor where code=`ER
select from product where code=`ER
start_date:exec min date from quote;end_date:exec max date from quote;xcode:`AL;
start_date:2017.01.01;end_date:exec max date from quote;xcode:`AG;
start_date:2010.01.01;end_date:2018.02.21;xcode:`AG;
start_date:1984.01.01;end_date:2018.02.21;xcode:`AL;
start_date:1984.01.01;end_date:2018.02.21;xcode:`A;
start_date:1984.01.01;end_date:2018.02.21;xcode:`RI;
start_date:1984.01.01;end_date:2018.02.21;xcode:`ZC;
start_date:1984.01.01;end_date:2018.02.21;xcode:`RB;
// 计算[start_date end_date]闭区间的所有因子值,注意赋值顺序，
.factor.main[xcode;start_date;end_date]
t:.factor.wrap[xcode;start_date;end_date]
.factor.wrap:{[xcode;start_date;end_date]
    key_tab:.factor.main[xcode;start_date;end_date];
    if[0=count key_tab;:key_tab];
    key_tab^:.factor.adjfactor[key_tab];        // 注意使用^合并是，不能有相同的列，否则应该用lj
    key_tab^:.factor.first_contract[key_tab];
    key_tab^:.factor.secondary_contract[key_tab];
    key_tab^:.factor.nearest_contract[key_tab];
    key_tab^:.factor.roll_return3[key_tab];        // 展期收益率，主力，远月且持仓量最大
    key_tab^:.factor.mom_time[key_tab;`factor;`adjclose;40];
    key_tab^:.factor.mom_basis3[key_tab;120];   // 基差动量，最近月，主力，重合用1替代
    key_tab^:.factor.mom_warehouse_receipt[key_tab;89];
    key_tab^:.factor.vix[key_tab;86;0.94];
    
    nb:avg each {{1 _ x, y}\[x#0.0;y]}[20;exec volume from key_tab];    // volume的20日均值
    key_tab[`vol_avg]:nb;
    key_tab[`filter_reason]:0.0;
    key_tab:update filter_reason:filter_reason+1.0 from key_tab where i<180;
    key_tab:update filter_reason:filter_reason+2.0 from key_tab where vol_avg<10000;
    key_tab:delete vol_avg from key_tab;
    
    key_tab:update mom_time:0n,roll_return_near_far:0n,mom_basis_near_far:0n,mom_warehouse_receipt:0n from key_tab where filter_reason>0;   
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



// 计算xcode在[start_date end_date]的主力合约，并与行情表左连接
// 主力合约计算方法：连续两天持仓量最大，第三天收盘切换，第四天开盘生效
// 注意start_date之前的因子数据必须是全部计算好的，增量计算因子时都需要注意此条件。
// 返回类型：table(98h),可能为空表

.factor.main:{[xcode;start_date;end_date]
    init_trading_day:min exec date from quote where code=xcode; // 最早交易日    

    yc:exec -3# contract from select [-3] contract from quote where date<start_date,code=xcode,oi>=(max;oi) fby date;     //  最大持仓的合约 

    if[any null yc;yc:-3#exec contract from quote where date=init_trading_day,code=xcode,oi>=max(oi)];
    c_y:yc[2];c_yy:yc[1];c_yyy:yc[0];   // 昨天  前天 大前天最大持仓的合约     

    if[null mc:last exec contract from select [-1] contract from factor where date<start_date,code=xcode;mc:c_y];  //当前（上一个）主力合约  

    r:();   //to return    
    key_tab:(`date xasc select date,code,contract from quote where date>=start_date,date<=end_date,code=xcode,oi>=(max;oi) fby date);     //  最大持仓的合约

    key_tab:0!select by date from key_tab;    /如果最大持仓量的合约有多个，去除重复的保留第一条

    n:count key_tab;
    0N!n;
    i:0;
    p:1!select contract,dlmonth from product where code=xcode;
    while[i<n;
        if[(c_yy=c_yyy) & (c_yy<>mc) & p[c_yy][`dlmonth]>p[mc][`dlmonth];mc:c_yy];        
        r,:mc;
        c_yyy:c_yy;
        c_yy:c_y;
        c_y:key_tab[i][`contract];
        i+:1;
    ]; 
    0N!count r;
    0N!count key_tab;    
    key_tab: update contract:r from key_tab;    
    :lj[key_tab;3!select from quote where date>=start_date,date<=end_date,code=xcode];        
};
xcode:`AG;start_date:2010.01.01;end_date:2019.08.01
.factor.main[xcode;start_date;end_date]
key_tab:
.factor.main[`AL;2017.01.01;.z.d-3]
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
.factor.roll_return1:{[key_tab]
    nf:raze {2#`oi xdesc select date,contract,oi,settle from quote where date=x[`date],code=x[`code] }each select date,code from key_tab;   //选择oi最大的两个合约, 可能只有一个合约，退市时，ER
    nf:lj[nf;1!select contract,lasttrade_date from product];    
    n:select date,near_contract:contract,near_settle:settle,near_lasttrade_date:lasttrade_date from nf where  lasttrade_date=(min;lasttrade_date) fby date;
    n:select near_contract,near_settle,near_lasttrade_date from n where differ n; // 只有一个合约时会出现重复的    如ER
    f:select date,far_contract:contract,far_settle:settle,far_lasttrade_date:lasttrade_date from nf where  lasttrade_date=(max;lasttrade_date) fby date;   
    f:select far_contract,far_settle,far_lasttrade_date from f where differ f; // 只有一个合约时会出现重复的    如ER
    :update roll_return_near_far:365*((log near_settle)-log far_settle)%((`date$far_lasttrade_date)-`date$near_lasttrade_date) from n^f;    
}
//展期收益率： 最大持仓量合约，远月且持仓量最大的合约
.factor.roll_return2:{[key_tab]
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
.factor.roll_return3:{[key_tab]
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
.factor.roll_return:{[key_tab]
    m:lj[select contract,settle from key_tab;1!select contract,lasttrade_date from product];
    s:lj[select contract:secondary_contract,settle:secondary_settle from key_tab;1!select contract,lasttrade_date from product];
    :([]roll_return:365*((log exec settle from m) - (log exec settle from s))%((`date$exec lasttrade_date from s) -(`date$exec lasttrade_date from m)));   
}

// 计算展期收益率, 持仓量最大和持仓量第二大的两个合约的展期收益
/ .factor.roll_return:{[key_tab]
/     m:lj[select contract:first_contract,settle:first_settle from key_tab;1!select contract,lasttrade_date from product];
/     s:lj[select contract:secondary_contract,settle:secondary_settle from key_tab;1!select contract,lasttrade_date from product];
/     :([]roll_return:365*((log exec settle from m) - (log exec settle from s))%((`date$exec lasttrade_date from s) -(`date$exec lasttrade_date from m)));   
/ }


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

// 计算基差动量, 最近月合约，主力, 近月减远月，
// 累乘(1+daily return of near) - 累乘(l+daily return of far)
// n为基差累乘的周期数 n:120
// care:两个合约可能重合，如ZC，前期很多重合，导致基差动量为1

.factor.mom_basis3[key_tab;n]
.factor.mom_basis3:{[key_tab;n]
    xcode:key_tab[`code][0];
    // 计算最近月的日收益

    t:(select [neg n+1] date,nearest_contract,nearest_settle from factor where code=xcode,date<min key_tab[`date]),select date,nearest_contract,nearest_settle from key_tab;
    t:update ref_contract:next nearest_contract from t;    
    t:lj[t;2!select date,ref_contract:contract,ref_settle:settle from quote where code=xcode,date>=min t[`date],date<=max t[`date]];    //合约切换时对齐到上一个合约
    ny:exec 1.0^(nearest_settle%prev ref_settle) from t;    
    // 近月的基差(前n个收益相乘), prd sliding window

    nb:prd each {{1 _ x, y}\[x#1.0;y]}[n;ny];    
                
    // 选择主力合约
    tf:select date,far_contract:contract,far_settle:settle from key_tab;
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
.factor.vix[key_tab;86;0.94]
n:86
c:0.94
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
    adj_log_return:1 _ deltas log (first adj_price),adj_price;
    adj_ema:last flip ema[1-c] each {{1 _ x, y}\[x#0.0;y]}[n;adj_log_return xexp 2];        // ewma计算波动率

    adj_vol:sqrt adj_ema;
    adj_dev:dev each {{1 _ x, y}\[x#1.0;y]}[n;adj_log_return];                       
    adj_var:var each {{1 _ x, y}\[x#1.0;y]}[n;adj_log_return];    
    r:(neg count key_tab)#([]adj_log_return;adj_vol;adj_dev:adj_dev;adj_var:adj_var);    
    
    :update adj_tr:adj_tr from r;    
};

// 计算仓单变化率的动量，参考周期n为90，实际是n:89
.factor.mom_warehouse_receipt:{[key_tab;n]
    xcode:key_tab[`code][0];
    his_w:select [neg n] from warehouse_receipt where code=xcode,date<min key_tab[`date];    
    new_w:lj[select date,code from key_tab;2!select from warehouse_receipt where code=xcode,date>=min key_tab[`date],date<=max key_tab[`date]];    
    w:his_w,new_w;
    
    w:update warehouse_receipt:0n from w where warehouse_receipt=0; // 仓单库存为0时，设为nan
    
    new_n:count key_tab;
    shift_w:new_n#((new_n+n-count w)#1#w),w;    
    wd:update shift_warehouse_receipt:shift_w[`warehouse_receipt] from new_w;
    
    :select mom_warehouse_receipt:(warehouse_receipt-shift_warehouse_receipt)%shift_warehouse_receipt from wd;
}