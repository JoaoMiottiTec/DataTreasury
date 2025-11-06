create or replace view silver.stg_macro_daily as
select
    source,
    date:: date as date,
    value:: numeric as value
from bronze.raw_macro_daily;

create or replace view silver.stg_quotes_daily as
select
  source,
  symbol,
  date::date as date,
  open::numeric as open,
  high::numeric as high,
  low::numeric  as low,
  close::numeric as close
from bronze.raw_quotes_daily;