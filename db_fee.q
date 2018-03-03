/ ��������֤������(Ԫ/��)	��������֤�𰴽��	��������֤������(Ԫ/��)	��������֤�𰴽��	
/ ��ƽ�����Ѱ�����(Ԫ/��)	��ƽ�����Ѱ����	ƽ�������Ѱ�����(Ԫ/��)	ƽ�������Ѱ����	
/ ��ֵ��ƽ�����Ѱ�����(Ԫ/��)	��ֵ��ƽ�����Ѱ����	��ֵƽ�������Ѱ�����(Ԫ/��)	��ֵƽ�������Ѱ����	
/ ������ƽ�����Ѱ�����(Ԫ/��)	������ƽ�����Ѱ����	����ƽ�������Ѱ�����(Ԫ/��)	����ƽ�������Ѱ����	
/ �ƽ���ӷѰ���ֵ
load_fee:{[x]
    col_names:`gid`market_id`contract`broker_id`update_time`buy_margin_by_vol`buy_margin_by_amt`buy_hedge_margin_by_vol`buy_hedge_margin_by_amt`sell_margin_by_vol`sell_margin_by_amt`sell_hedge_margin_by_vol`sell_hedge_margin_by_amt`buy_arbitragy_margin_by_vol`buy_arbitragy_margin_by_amt`sell_arbitragy_margin_by_vol`sell_arbitragy_margin_by_amt`fee_by_vol`fee_by_amt`ct_by_vol`ct_by_amt`fee_hedge_by_vol`fee_hedge_by_amt`fee_hedge_ct_by_vol`fee_hedge_ct_by_ammt`fee_arbitragy_by_vol`fee_arbitragy_hedge_by_amt`fee_arbitragy_hedge_ct_by_vol`fee_arbitragy_hedge_ct_by_ammt`fee_au_delay_by_amt;
    $[-11h=type x;x:hsym x;x:hsym `$x];
    fee:col_names xcol ("SSSSZFFFFFFFFFFFFFFFFFFFFFFFFF"; enlist ",") 0: x;
    fee:`contract xkey fee;
    fee:select contract,buy_margin_by_amt,sell_margin_by_amt,fee_by_vol,fee_by_amt,ct_by_vol,ct_by_amt from fee;
    fee
};

fee:load_fee[`:d:/cta/fee.csv];
fee:lj[update `$contract:upper string contract from fee;1!select contract,code from product];
fee:select by code from fee;
fee:select from fee where code<>`;
delete contract from `fee;
fee
fee[`CY]:fee[`AG]   //手动补充cy的数据,使用AG的数据替代
upserttable["d:/db";"fee";0!fee];
select from fee

select date,settle from quote where code=`ZC