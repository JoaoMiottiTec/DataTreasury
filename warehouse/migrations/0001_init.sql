-- Schemas
create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;

-- Bronze (bruto)
create table if not exists bronze.raw_macro_daily (
  id bigserial primary key,
  source text not null,         -- 'bcb_selic' | 'bcb_ipca' | 'bcb_usdbrl'
  date date not null,
  value numeric,
  raw jsonb not null,
  ingested_at timestamptz default now(),
  unique (source, date)
);

create table if not exists bronze.raw_quotes_daily (
  id bigserial primary key,
  source text not null,         -- 'coingecko' | 'yfinance'
  symbol text not null,         -- 'BTCUSD' | 'ETHUSD' | 'IBOV' | 'PETR4.SA'
  date date not null,
  open numeric, high numeric, low numeric, close numeric,
  raw jsonb not null,
  ingested_at timestamptz default now(),
  unique (source, symbol, date)
);

-- Silver (staging) simples por enquanto
create or replace view silver.stg_macro_daily as
  select source, date, value from bronze.raw_macro_daily;

create or replace view silver.stg_quotes_daily as
  select source, symbol, date, open, high, low, close
  from bronze.raw_quotes_daily;

-- Gold (marts alvo)
create table if not exists gold.mart_macro_daily (
  date date primary key,
  selic numeric,
  ipca numeric,
  usdbrl numeric
);

create table if not exists gold.mart_prices_daily (
  symbol text,
  date date,
  close numeric,
  primary key (symbol, date)
);
