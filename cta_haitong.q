\cd d:/db_cta
if[.z.o in`w32`w64;system "l d:/db_script/dblib.q";system "l d:/db_script/dbmaint.q";system "l ."];

tables[]
select from factor

select from ht_mom_basis

select from quote
select from score_cross
select from product
select from trading_day

select  from sue_cgo where date=`all_mean
cols sue_cgo

a:1#select from product1
b:1#select from product
b

\f
upserttable_no_duplicate

a~b

upserttable
log_path

log_path

select from quote where date>2018.01.01

tables[]
score_cross
-1#select from trading_day
-1#select from quote
exec max date from quote
-1#select from warehouse_receipt