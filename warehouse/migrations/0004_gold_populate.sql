with selic as (
  select
    date,
    value as selic
  from silver.stg_macro_daily
  where source = 'bcb_selic'
),

ipca as (
  select
    date,
    value as ipca
  from silver.stg_macro_daily
  where source = 'bcb_ipca'
),

usdbrl as (
  select
    date,
    close as usdbrl
  from silver.stg_quotes_daily
  where symbol = 'USDBRL'
)

insert into gold.mart_macro_daily as t (date, selic, ipca, usdbrl)
select
  d::date as date,
  s.selic,
  i.ipca,
  u.usdbrl
from (
  select date from selic
  union
  select date from ipca
  union
  select date from usdbrl
) dates(d)
left join selic  s on s.date = d
left join ipca   i on i.date = d
left join usdbrl u on u.date = d
on conflict (date) do update
  set selic  = coalesce(excluded.selic,  t.selic),
      ipca   = coalesce(excluded.ipca,   t.ipca),
      usdbrl = coalesce(excluded.usdbrl, t.usdbrl);


insert into gold.mart_prices_daily as t (symbol, date, close)
select
  q.symbol,
  q.date,
  q.close
from silver.stg_quotes_daily q
where q.close is not null
on conflict (symbol, date) do update
  set close = excluded.close;

with recent as (
  select * from silver.stg_macro_daily
  where date >= current_date - interval '30 days'
)
insert into gold.mart_macro_daily as t (date, selic)
select
  date,
  value as selic
from recent
where source = 'bcb_selic'
on conflict (date) do update
  set selic = excluded.selic;
