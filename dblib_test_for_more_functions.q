gen_tbl_trade:{[n]
    ([]date:(2016.01.01)+n?10; time:asc n?24:00:00; sym:n?`ibm`aapl`goo; price:n?100;size:n?1000)
};
tbl:gen_tbl_trade[10]
pupserttable_no_duplication["d:/db";"trade";tbl;"date";"sym";log_path]; 
  tmp
allcols first allpaths[`:d:/db;`trade]
 
allcols[`:d:/db/2016.01.02/trade]
 
listcols[`:d:/db;`trade]
 
renamecol[`:d:/db;`trade;`ti;`time]
\l .

rentable[`:d:/db;`trade;`transactions]
\l .

select from trade

reordercols[`:d:/db;`trade;reverse cols trade]

pupserttable["d:/db";"tbl2";tmp;"date";log_path]
tmp[`date][0]

dt
parse "select date.year from tmp"
select from pupsert_t
pupserttable_no_duplication["d:/db";"s";tmp;"date";enlist "sym";"d:/db.log"]
?[tmp;();0b;(enlist `year)!enlist `date.date]

select from upsert_t
select from factor
exec stock_code from select stock_code from factor
select from f1
count select from factor

select date,stock_code,gg_rpt_num from factor where gg_rpt_num>10

select gg_rpt_num from factor where date=2010.01.29

select from gg_rpt_num where date=2010.01.29

(count distinct select from tfzq_con_roll_peg)=count select from tfzq_con_roll_peg

a:distinct select from dfzq_con_ep_fy1
b:select from dfzq_con_ep_fy1
select from a
100#select from factor
select from record

select date, stock_code, gg_con_npg,gg_con_np_change_13w from factor where date>2018.01.01