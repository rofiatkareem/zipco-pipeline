-- Dimensions
create table if not exists dim_location (
  location_sk bigserial primary key,
  state text not null,
  city  text not null,
  postal_code text not null,
  unique(state, city, postal_code)
);

create table if not exists dim_office (
  office_sk bigserial primary key,
  office_name  text not null,
  office_phone text,
  unique(office_name, office_phone)
  )

create table if not exists dim_property (
  property_sk bigserial primary key,
  property_type text,
  bedrooms int,
  bathrooms numeric,
  unique(property_type, bedrooms, bathrooms));

create table if not exists dim_date (
  date_sk int primary key,
  date_val date not null unique
);

-- Fact table
create table if not exists fact_listings (
  listing_id text primary key,              -- from API 'id'
  location_sk bigint not null references dim_location(location_sk),
  office_sk   bigint     references dim_office(office_sk),
  property_sk bigint     references dim_property(property_sk),
  full_address text,
  latitude     numeric,
  longitude    numeric,
  -- Optional date FKs if available in your data (listed/removed/lastSeen)
  listed_date_sk   int references dim_date(date_sk),
  removed_date_sk  int references dim_date(date_sk),
  ingested_at timestamptz default now()
);

create index if not exists ix_fact_loc   on fact_listings(location_sk);
create index if not exists ix_fact_off   on fact_listings(office_sk);
create index if not exists ix_fact_prop  on fact_listings(property_sk);