gen_tbl_trade:{[n]
    ([]date:(2016.01.01)+n?10; time:asc n?24:00:00; sym:n?`ibm`aapl`goo; price:n?100;size:n?1000)
};
tbl:gen_tbl_trade[10]
pupserttable_no_duplication["d:/db";"trade";tbl;"date";"sym";log_path]; 
  
allcols first allpaths[`:d:/db;`trade]
 
allcols[`:d:/db/2016.01.02/trade]
 
listcols[`:d:/db;`trade]
 
renamecol[`:d:/db;`trade;`ti;`time]
\l .

rentable[`:d:/db;`trade;`transactions]
\l .

select from trade

reordercols[`:d:/db;`trade;reverse cols trade]

