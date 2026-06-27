{{ config(materialized='table') }}

WITH monthly_zone_metrics AS (

    SELECT

        DATE_TRUNC('month', pickup_datetime) AS trip_month,

        pickup_location_id,

        pickup_zone,

        pickup_borough,

        COUNT(*) AS total_trips,

        AVG(trip_distance) AS avg_trip_distance,

        AVG(fare_amount) AS avg_fare,

        SUM(total_amount) AS total_revenue

    FROM {{ ref('fct_trips') }}

    GROUP BY
        1,2,3,4

)

SELECT

    *,

    RANK() OVER (

        PARTITION BY trip_month

        ORDER BY total_revenue DESC

    ) AS revenue_rank,

    CASE

        WHEN total_trips > 10000 THEN TRUE

        ELSE FALSE

    END AS high_volume_zone

FROM monthly_zone_metrics