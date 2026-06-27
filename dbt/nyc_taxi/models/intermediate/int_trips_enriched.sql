{{ config(materialized='view') }}

SELECT
    t.*,

    pickup.zone AS pickup_zone,
    pickup.borough AS pickup_borough,
    pickup.service_zone AS pickup_service_zone,

    dropoff.zone AS dropoff_zone,
    dropoff.borough AS dropoff_borough,
    dropoff.service_zone AS dropoff_service_zone

FROM {{ ref('stg_yellow_trips') }} t

LEFT JOIN {{ ref('stg_taxi_zones') }} pickup
    ON t.pickup_location_id = pickup.location_id

LEFT JOIN {{ ref('stg_taxi_zones') }} dropoff
    ON t.dropoff_location_id = dropoff.location_id

WHERE trip_distance > 0
  AND fare_amount > 0
  AND passenger_count > 0
  AND trip_duration_minutes BETWEEN 1 AND 180