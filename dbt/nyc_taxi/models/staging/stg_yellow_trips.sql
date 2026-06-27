{{ config(materialized='view') }}

SELECT

    VendorID                          AS vendor_id,

    tpep_pickup_datetime              AS pickup_datetime,

    tpep_dropoff_datetime             AS dropoff_datetime,

    passenger_count,

    trip_distance,

    RatecodeID                        AS rate_code_id,

    store_and_fwd_flag,

    PULocationID                      AS pickup_location_id,

    DOLocationID                      AS dropoff_location_id,

    payment_type,

    fare_amount,

    extra,

    mta_tax,

    tip_amount,

    tolls_amount,

    improvement_surcharge,

    total_amount,

    congestion_surcharge,

    Airport_fee                       AS airport_fee,

    datediff(
        'minute',
        tpep_pickup_datetime,
        tpep_dropoff_datetime
    ) AS trip_duration_minutes

FROM {{ source('raw', 'yellow_tripdata') }}