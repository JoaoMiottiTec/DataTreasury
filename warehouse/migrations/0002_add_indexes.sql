create index if not exists idx_raw_macro_daily_date
  on bronze.raw_macro_daily (date);

create index if not exists idx_raw_quotes_daily_symbol_date
  on bronze.raw_quotes_daily (symbol, date);
