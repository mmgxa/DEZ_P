{{ config(materialized='table') }}

with liqdata as 
(
  select *,
  from {{ source('core','liquor') }}
)
select
    -- order info
    cast(date as date) as date,
    city,
    cast(zip_code as integer) as  zip_code,
    cast(county_number as integer) as countynumber,
    county,
    cast(category as integer) as category,
    category_name,
    cast(vendor_number as integer) as vendornumber,
    vendor_name,
    cast(item_number as integer) as itemnumber,
    item_description,
    cast(pack as integer) as pack,
    cast(bottle_volume_ml as integer) as  bottle_volume_ml,
    cast(state_bottle_cost as decimal) as  state_bottle_cost,
    cast(state_bottle_retail as decimal) as  state_bottle_retail,
    cast(bottles_sold as integer) as  bottles_sold,
    cast(sale_dollars as decimal) as  sale_dollars,
    cast(volume_sold_liters as decimal) as  volume_sold_liters,
    cast(volume_sold_gallons as decimal) as volume_sold_gallons,
from liqdata
