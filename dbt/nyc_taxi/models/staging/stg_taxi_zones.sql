{{ config(materialized='view') }}

SELECT

    LocationID AS location_id,

    Borough,

    Zone,

    service_zone

FROM {{ source('raw', 'taxi_zone_lookup') }}