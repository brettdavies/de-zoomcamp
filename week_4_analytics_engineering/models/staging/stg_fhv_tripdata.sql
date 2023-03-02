{{ config(materialized='view') }}
 
with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('staging','fhv_trips') }}
  where PUlocationID is not null 
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['PUlocationID', 'pickup_datetime']) }} as tripid,
    cast(PUlocationID as integer) as pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    dispatching_base_num as dispatching_base,
    Affiliated_base_number as affiliated_base,
    cast(case
        when SR_Flag = 1
        THEN TRUE
        ELSE FALSE
        END as boolean) as shared_ride,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime
    
from tripdata
-- where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

limit 100

{% endif %}
