SELECT 
  CASE WHEN RUN_DATE < FLIGHT_DATE THEN 1 ELSE 0 END AS run_date_check,
  CASE WHEN FLIGHT_TIME > 0 THEN 1 ELSE 0 END AS flight_time_check
FROM {{ params.table }}
WHERE RUN_DATE IN (SELECT RUN_DATE FROM {{ params.table }} ORDER BY RANDOM() LIMIT 1)