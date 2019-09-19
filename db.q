((`contract xasc `contract xkey select from quote where code=`AG,date=2019.08.28 )lj `contract xkey select contract,dlmonth from product) lj `contract xkey select from factor where code=`AG, date=2019.08.28
select from product

select from factor where code=`AG

select from quote where code=`AG,date=2019.08.30
select from quote where contract=`AG2002,date>2019.08.01

select from quote where null open


swin2:{x'/[0^(y-1)('[;]/[(flip;reverse;prev\)])'z]}        /swin2[cor;3;(29 10 54;1 3 9)]

select from quote where code=`TA

`dlmonth xasc select from product where code=`UR

t:(select min date,max date by contract from quote ) lj `contract xkey select from product
t :select from t where exch=`CZC
t

select first contract by code from quote where date=2019.08.29,code=`AG

`contract xasc select contract  from quote where date=2019.08.28,code=`AG

t
t:select sum(oi) by date from quote where code=`AG,date>2019.08.01
t1:select date,(0f,1_deltas oi) from t
t2:select date,(0,1_deltas close)%close from quote where contract=`AG1912,date>2019.08.01

t1 lj 1!t2

gu:(select date,ag:close from quote where contract=`AG1212,date>2011.06.01) lj 1!select date,au:close from quote where contract=`AU1212
select date,ag%20,au,gu:5*ag%au from gu

gu:(select date, ag:close from factor where code=`AG)lj 1!select date,au:close from factor where code=`AU


select from product where contract=`AG1908

select from quote where contract=`AG1602

`d:cta/rb.csv 0: csv 0: 0!select first code, max oi,max volume,max close by contract from quote where code = `RB

1!0!(select sum(oi),sum(volume) by date,code from quote where code in (`AG`AU`RB`I`J))