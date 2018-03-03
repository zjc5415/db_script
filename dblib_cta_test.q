dir:`:D:/cta/future_list;
filelist:key dir:hsym dir;
product_info_t:();
{
    fpath: ` sv dir,x;
    d:("SMFFDDD"; enlist ",") 0: fpath;    
    d[`product]:`$(-4_ string x);
    product_info_t,:d;
} each filelist;

`wind_code`delivery_mouth xasc product_info_t

upserttable[dbdir;"product";product_info_t]
idx:exec i from `:d:/db/product where product=`A.DCE
updateentry[dbdir;"product";idx;"product";"A";log_path]
get `:d:/db/product




//load daily data
daily:();
contract_list:select wind_code,product from product_info_t;
{
    fname:`$((string x[`wind_code]),".csv");
    if[not fname in key `:D:/cta/future_daily;:`];
    fpath: ` sv (`:D:/cta/future_daily),`$((string x[`wind_code]),".csv");    
    d:("DFFFFFFFF"; enlist ",") 0: fpath;
    d[`close]:fills d[`close];
    d[`settle]:fills d[`settle];
    d[`product]:x[`product];
    d[`contract]:x[`wind_code];
    daily,:d;    
} each contract_list;

upserttable[dbdir;"quote";daily]
last select  from  `:d:/db/quote

(meta select from `:d:/db/pyquote where i<10)
meta select from `:d:/db/quote where i<10

select from `:d:/db/product

flip 0#product

cols product
select contract, contract_issuedate, lastdelivery_date from product

count select from quote

-10# select from quote

select distinct code from quote
select date from quote where contract=`$"AF1411-S"

  key_tab:@[{select date_time,inst from get x};writepath;([]date_time:();inst:())];
   $[count key_tab;
  	[dups:exec i from towrite where ([]date_time;inst) in key_tab;];
  	dups:()];
   $[count dups;
     [out"Removed ",(string count dups)," duplicates from ctp_tick table";
     towrite:select from towrite where not i in dups];
dup:{[x;y]
    
    key_tab;select date,contract from x;
    dups:exec i from y where ([]date;contract) in key_tab;
    towrite: select from y where i not in dups;
}
select from product where code=`ER

10# select from quote

//设置属性
{`timestamp xasc `$":./2014.04.20/quote/"}
`date xasc `:d:/db/quote            //succ
`contract xasc `:d:/db/quote        //succ
update `s#date from `:d:/db/quote   //failed
update `p#contract from `:d:/db/quote   //failed
@[`:d:/db/quote;`contract;`p#]  //failed u-fail
@[`:d:/db/quote;`date;`s#] //succeed
@[`:d:/db/quote/;`date;`s#] //succeed

meta get `:d:/db/quote


select from quote where contract=`AG1806
.Q.w[]

select from product

select i from `:d:/db/quote where date>.z.d
select   from  where date>.z.d

save `:d:/db_csv/quote.csv
para
meta tmp
tmp
select from product where contract_issuedate>2017.01.01
select from quote2
cols product

select from product where contract_issuedate>lastdelivery_date
@[tmp;idx;:;0]

select from tmp:select from product where contract_issuedate=0b
@[tmp;contract_issuedate>lastdelivery_date;:;lastdelivery_date]
tmp
.[tmp;`pxunit;:;0.0]

select from quote where date>2018.02.13
select from product where code=`A
select from quote where null settle
select from quote where null close
select from factor where code=`AG
select from warehouse_receipt where code=`AG

select date,corr_avg from avg_corr
min select date,corr_factor from avg_corr

select from factor where code=`ER
distinct select exch from product
select from product where exch=`INE
select from quote where code=`SC
select from factor where code=`SC
