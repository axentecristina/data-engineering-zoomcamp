--For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', 
--exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?

select 
	count(*) as number_trips 
from green_tripdata_2025_11
WHERE 
	lpep_pickup_datetime >= DATE '2025-11-01' AND 
	lpep_pickup_datetime <  DATE '2025-12-01' and 
	trip_distance<=1;

--Which was the pick up day with the longest trip distance? 
--Only consider trips with trip_distance less than 100 miles (to exclude data errors).
SELECT lpep_pickup_datetime, max(trip_distance) as max_trip
FROM green_tripdata_2025_11
where trip_distance<100
group by lpep_pickup_datetime
order by  max(trip_distance) desc
limit 1;


--Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?
SELECT 
--  gr."PULocationID", 
  tz."Zone",
  SUM(gr.total_amount) AS total_revenue
FROM green_tripdata_2025_11 gr
JOIN taxi_zone_lookup tz
  ON gr."PULocationID" = tz."LocationID"
WHERE gr.lpep_pickup_datetime::date = DATE '2025-11-18'
GROUP BY 
--  gr."PULocationID", 
  tz."Zone"
ORDER BY total_revenue desc;

--For the passengers picked up in the zone named "East Harlem North" in November 2025, 
--which was the drop off zone that had the largest tip?
SELECT 
  doz."Zone",
  max(gr.tip_amount) as max_tip_amount
FROM green_tripdata_2025_11 gr
JOIN taxi_zone_lookup puz
  ON gr."PULocationID" = puz."LocationID"
  JOIN taxi_zone_lookup doz
  ON gr."DOLocationID" = doz."LocationID"
WHERE puz."Zone"='East Harlem North'
group by doz."Zone"
ORDER BY max(gr.tip_amount) desc
limit 1
;