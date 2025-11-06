create schema if not exists silver;

create or replace view silver.stg_macro_daily as
select
  lower(trim(source)) as source,
  date,
  value 
from bronze.raw_macro_daily
order by source, date;

create or replace view silver.stg_quotes_daily as
select
  lower(trim(source)) as source,
  upper(trim(symbol)) as symbol,
  date,
  open,
  high,
  low,
  close
from bronze.raw_quotes_daily
order by source, symbol, date;
