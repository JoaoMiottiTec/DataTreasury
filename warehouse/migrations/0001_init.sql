create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;

create table if not exists bronze.raw_macro_daily (
  id            bigserial primary key,
  source        text        not null, 
  date          date        not null,
  value         numeric,
  raw           jsonb       not null,
  ingested_at   timestamptz not null default now(),
  constraint uq_raw_macro_daily_source_date unique (source, date)
);

create table if not exists bronze.raw_quotes_daily (
  id            bigserial primary key,
  source        text        not null,
  symbol        text        not null,
  date          date        not null,
  open          numeric,
  high          numeric,
  low           numeric,
  close         numeric,
  raw           jsonb       not null,
  ingested_at   timestamptz not null default now(),
  constraint uq_raw_quotes_daily_source_symbol_date unique (source, symbol, date)
);

create or replace view silver.stg_macro_daily as
  select source, date, value
  from bronze.raw_macro_daily;

create or replace view silver.stg_quotes_daily as
  select source, symbol, date, open, high, low, close
  from bronze.raw_quotes_daily;

create table if not exists gold.mart_macro_daily (
  date   date primary key,
  selic  numeric,
  ipca   numeric,
  usdbrl numeric
);

create table if not exists gold.mart_prices_daily (
  symbol text not null,
  date   date not null,
  close  numeric,
  primary key (symbol, date)
);
