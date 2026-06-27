WITH hourly_metrics AS (

    SELECT

        EXTRACT(hour FROM pickup_datetime) AS hour_of_day,

        COUNT(*) AS total_trips,

        AVG(fare_amount) AS avg_fare,

        AVG(
            tip_amount * 100.0 /
            NULLIF(fare_amount,0)
        ) AS avg_tip_percentage

    FROM main.fct_trips

    GROUP BY 1

)

SELECT

    *,

    AVG(total_trips) OVER (

        ORDER BY hour_of_day

        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW

    ) AS rolling_3_hour_avg

FROM hourly_metrics

ORDER BY hour_of_day;