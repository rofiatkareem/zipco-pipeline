-- Location dimension
insert into dim_location(state, city, postal_code)
select distinct state, city, postal_code
from transformed_listing
where state is not null and city is not null and postal_code is not null
on conflict (state, city, postal_code) do nothing;

-- Office dimension
insert into dim_office(office_name, office_phone)
select distinct listing_office_name, listing_office_phone
from transformed_listing
where listing_office_name is not null
on conflict (office_name,office_phone) do nothing;

-- Property dimension (type + beds + baths)
insert into dim_property(property_type, bedrooms, bathrooms)
select distinct property_type, bedrooms, bathrooms
from transformed_listing
on conflict (property_type, bedrooms, bathrooms) do nothing;

-- Date dimension 
insert into dim_date(date_sk, date_val)
select distinct extract(epoch from listed_date::date)::int, listed_date::date
from transformed_listing
where listed_date is not null
on conflict (date_sk) do nothing;

-- Build the fact table
insert into fact_listings(
  listing_id, location_sk, office_sk, property_sk,
  full_address, latitude, longitude, listed_date_sk, removed_date_sk
)
select
  l.id,
  dloc.location_sk,
  doff.office_sk,
  dprop.property_sk,
  l.full_address,
  l.latitude, l.longitude,
  case when l.listed_date is not null
       then extract(epoch from l.listed_date::date)::int end as listed_date_sk,
  case when l.removed_date is not null
       then extract(epoch from l.removed_date::date)::int end as removed_date_sk
from transformed_listing l
join dim_location dloc
  on dloc.state = l.state and dloc.city = l.city and dloc.postal_code = l.postal_code
left join dim_office doff
  on doff.office_name = l.listing_office_name
 and coalesce(doff.office_phone,'') = coalesce(l.listing_office_phone,'')
left join dim_property dprop
  on dprop.property_type = l.property_type
 and coalesce(dprop.bedrooms,-1)  = coalesce(l.bedrooms,-1)
 and coalesce(dprop.bathrooms,-1) = coalesce(l.bathrooms,-1)
on conflict (listing_id) do nothing;