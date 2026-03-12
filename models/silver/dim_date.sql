{{ config(
    materialized='table',
    tags=['silver']
) }}

WITH date_spine AS (
    SELECT EXPLODE(SEQUENCE(
       TO_DATE('2000-01-01'),
       TO_DATE('2030-12-31'),
       INTERVAL 1 DAY
    )) AS full_date
),

public_holidays AS (
    SELECT CAST(holiday_date AS DATE) AS holiday_date
    FROM {{ ref('public_holidays') }}
)

SELECT
   CAST(DATE_FORMAT(ds.full_date, 'yyyyMMdd') AS INT) AS date_skey,
   ds.full_date,
   DAY(ds.full_date) AS day,
   MONTH(ds.full_date) AS month,
   QUARTER(ds.full_date) AS quarter,
   YEAR(ds.full_date) AS year,
   DATE_FORMAT(ds.full_date, 'EEE') AS day_of_week,
   CASE
       WHEN DAYOFWEEK(ds.full_date) NOT BETWEEN 2 AND 6 THEN false
       WHEN ph.holiday_date IS NOT NULL THEN false
       ELSE true
   END AS is_workday,
   DATE_FORMAT(ds.full_date, 'yyyy-MM') AS reporting_month
FROM date_spine ds
LEFT JOIN public_holidays ph ON ds.full_date = ph.holiday_date
