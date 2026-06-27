/*
Business Decision:
Revenue is ranked within each month instead of across the entire year.
This allows fair month-over-month comparison because taxi demand is seasonal.
*/

WITH monthly_revenue AS (

    SELECT

        DATE_TRUNC('month', pickup_datetime) AS trip_month,

        pickup_location_id,

        pickup_zone,

        SUM(total_amount) AS total_revenue

    FROM main.fct_trips

    GROUP BY
        1,2,3

)

SELECT *

FROM (

    SELECT

        *,

        RANK() OVER (

            PARTITION BY trip_month

            ORDER BY total_revenue DESC

        ) AS revenue_rank

    FROM monthly_revenue

)

WHERE revenue_rank <= 10

ORDER BY
    trip_month,
    revenue_rank;