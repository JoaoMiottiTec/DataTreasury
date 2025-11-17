select
  q.symbol,
  q.date,
  q.close
from {{ source('silver', 'stg_quotes_daily') }} as q
where q.close is not null
order by q.symbol, q.date