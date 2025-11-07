create or replace view gold.v_macro_selic_trends as
select
  date,
  selic / 100.0 as selic_percent,
  lag(selic, 1)  over (order by date) as selic_prev,
  (selic - lag(selic, 1) over (order by date)) / 100.0 as daily_delta,
  round(((selic - lag(selic, 1) over (order by date)) / lag(selic, 1) over (order by date)) * 100, 3) as daily_pct,
  round(avg(selic) over (order by date rows between 6 preceding and current row) / 100.0, 3) as selic_7d_avg,
  round(avg(selic) over (order by date rows between 29 preceding and current row) / 100.0, 3) as selic_30d_avg
from gold.mart_macro_daily
where selic is not null;

comment on view gold.v_macro_selic_trends is 'Trend analytics for SELIC with daily, 7d, 30d windows.';
