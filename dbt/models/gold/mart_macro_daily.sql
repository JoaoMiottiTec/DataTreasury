with selic as (
  select
    date,
    value as selic
  from {{ source('silver', 'stg_macro_daily') }}
  where source = 'bcb_selic'
),

ipca as (
  select
    date,
    value as ipca
  from {{ source('silver', 'stg_macro_daily') }}
  where source = 'bcb_ipca'
),

usdbrl as (
  select
    date,
    close as usdbrl
  from {{ source('silver', 'stg_quotes_daily') }}
  where symbol = 'USDBRL'
),

dates as (
  select date from selic
  union
  select date from ipca
  union
  select date from usdbrl
)

select
  d.date::date as date,
  s.selic,
  i.ipca,
  u.usdbrl
from dates d
left join selic  s on s.date = d.date
left join ipca   i on i.date = d.date
left join usdbrl u on u.date = d.date
order by d.date