--checking
select *
from dim_office;

--How many property listings exist in each city for each property type?

select dloc.city, dprop.property_type, count(*) as total_listings
from fact_listings f
join dim_location dloc on dloc.location_sk = f.location_sk
left join dim_property dprop on dprop.property_sk = f.property_sk
group by dloc.city, dprop.property_type
order by total_listings desc;

--Which real estate offices have the most listings in each city?
select city, office_name, listings
from (
  select dloc.city, doff.office_name, count(*) as listings,
         dense_rank() over(partition by dloc.city order by count(*) desc) rnk
  from fact_listings f
  join dim_location dloc on dloc.location_sk = f.location_sk
  left join dim_office doff on doff.office_sk = f.office_sk
  group by dloc.city, doff.office_name
) s
where rnk <= 5
order by city, listings desc;

-- How many listings do they have?
-- What is the average number of bedrooms in their listings?
-- How does that compare with the average number of bedrooms for the whole city?
select dloc.city, doff.office_name,
       count(*) as total_listings,
       round(avg(dprop.bedrooms),2) as avg_beds_office,
       (
         select round(avg(dp2.bedrooms),2)
         from fact_listings f2
         join dim_location dl2 on dl2.location_sk = f2.location_sk
         left join dim_property dp2 on dp2.property_sk = f2.property_sk
         where dl2.city = dloc.city
       ) as avg_beds_city
from fact_listings f
join dim_location dloc on dloc.location_sk = f.location_sk
left join dim_office doff on doff.office_sk = f.office_sk
left join dim_property dprop on dprop.property_sk = f.property_sk
group by dloc.city, doff.office_name
having count(*) >= 3
order by dloc.city, total_listings desc;
