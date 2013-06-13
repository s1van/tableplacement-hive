DROP TABLE q6_forecast_revenue_change;

-- create the target table
create table q6_forecast_revenue_change (revenue double);

-- the query
insert overwrite table q6_forecast_revenue_change 
select 
  sum(CG2.l_extendedprice*CG2.l_discount) as revenue
from 
  lineitem
where 
  CG2.l_shipdate >= '1994-01-01'
  and CG2.l_shipdate < '1995-01-01'
  and CG2.l_discount >= 0.05 and CG2.l_discount <= 0.07
  and CG2.l_quantity < 24;

