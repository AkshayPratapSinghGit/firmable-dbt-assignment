/*
Snowflake optimisation ideas

- Cluster by pickup_location_id and pickup_datetime
- Materialize ordered trips
- Use result cache
- Use search optimisation
*/

WITH ordered_trips AS (

    SELECT

        pickup_location_id,

        CAST(pickup_datetime AS DATE) AS trip_date,

        pickup_datetime,

        dropoff_datetime,

        LAG(dropoff_datetime) OVER (

            PARTITION BY
                pickup_location_id,
                CAST(pickup_datetime AS DATE)

            ORDER BY pickup_datetime

        ) AS previous_dropoff

    FROM main.fct_trips

)

SELECT

    trip_date,

    pickup_location_id,

    MAX(

        DATEDIFF(
            'minute',
            previous_dropoff,
            pickup_datetime
        )

    ) AS max_gap_minutes

FROM ordered_trips

WHERE previous_dropoff IS NOT NULL

GROUP BY
    trip_date,
    pickup_location_id

ORDER BY
    trip_date,
    pickup_location_id;