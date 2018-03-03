meta trade

delete perf_sum from `.

cols summary

select datetime,margin from summary where margin>=max margin
select from summary where datetime within 2017.11.28 2017.12.01
select from pos where datetime within 2017.11.28 2017.12.01

deltacp : select datetime,deltas cp from summary 
select from deltacp where cp>=max cp 2016.11.02

select from trade where datetime=2016.11.02
select from detail where datetime=2016.11.02

select datetime,J from pos

score
