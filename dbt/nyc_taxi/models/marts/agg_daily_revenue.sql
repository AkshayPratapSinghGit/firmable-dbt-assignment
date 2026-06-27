{{ config(materialized='table') }}

SELECT

    CAST(pickup_datetime AS DATE) AS trip_date,

    COUNT(*) AS total_trips,

    SUM(fare_amount) AS total_fare,

    AVG(fare_amount) AS avg_fare,

    SUM(tip_amount) AS total_tips,

    ROUND(
        SUM(tip_amount) * 100.0 / NULLIF(SUM(fare_amount), 0),
        2
    ) AS tip_rate_percentage

FROM {{ ref('fct_trips') }}

GROUP BY 1

ORDER BY 1