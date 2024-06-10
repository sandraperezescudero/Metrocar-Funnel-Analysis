/*
EXPLORATORY ANALYSIS QUERIES - Queries I've used to explore the data prior generating the aggregrated datasets for Tableau
*/

/* 
Query 1: 
Funnel analysis - Percent of top + Percent of previous

This SQL query calculates various metrics related to the user journey through different stages of the Metrocar funnel:
- Ride Transactions: Determines unique users who have completed ride transactions.
- Totals: Computes counts of users who have downloaded the app, signed up, requested rides, had their ride requests accepted, completed rides, paid for rides, and reviewed rides.
- Funnel Stages: Combines the total counts for each funnel stage into a single table, assigning a step number to each stage.
- Main Query: Calculates conversion rates and percent of top values for each funnel stage, considering both sequential and initial values for comparison purposes.

I've used this query in the exploratory analysis part to understand the customer funnel and identify drop-offs.
*/

WITH ride_transactions AS (
   SELECT DISTINCT rr.user_id
   FROM ride_requests rr
   JOIN transactions tr ON rr.ride_id = tr.ride_id
),
totals AS (
   SELECT
       COUNT(DISTINCT ad.app_download_key) AS total_users_downloaded,
       COUNT(DISTINCT s.user_id) AS total_users_signed_up,
       COUNT(DISTINCT rr.user_id) AS total_users_ride_requested,
       COUNT(DISTINCT CASE WHEN rr.accept_ts IS NOT NULL THEN rr.user_id END) AS total_users_ride_accepted,
       COUNT(DISTINCT CASE WHEN rr.dropoff_ts IS NOT NULL THEN rr.user_id END) AS total_users_ride_completed,
       COUNT(DISTINCT rt.user_id) AS total_users_ride_paid,
       COUNT(DISTINCT r.user_id) AS total_users_ride_reviewed
   FROM app_downloads ad
   LEFT JOIN signups s ON ad.app_download_key = s.session_id
   LEFT JOIN ride_requests rr ON s.user_id = rr.user_id
   LEFT JOIN ride_transactions rt ON rr.user_id = rt.user_id
   LEFT JOIN reviews r ON rr.ride_id = r.ride_id
),
funnel_stages AS (
   SELECT
       0 AS funnel_step,
       'download' AS funnel_name,
       total_users_downloaded AS total_users
   FROM totals


   UNION


   SELECT
       1 AS funnel_step,
       'signup' AS funnel_name,
       total_users_signed_up AS total_users
   FROM totals


   UNION


   SELECT
       2 AS funnel_step,
       'ride_requested' AS funnel_name,
       total_users_ride_requested AS total_users
   FROM totals


   UNION


   SELECT
       3 AS funnel_step,
       'ride_accepted' AS funnel_name,
       total_users_ride_accepted AS total_users
   FROM totals


   UNION


   SELECT
       4 AS funnel_step,
       'ride_completed' AS funnel_name,
       total_users_ride_completed AS total_users
   FROM totals


   UNION


   SELECT
       5 AS funnel_step,
       'payment' AS funnel_name,
       total_users_ride_paid AS total_users
   FROM totals


   UNION


   SELECT
       6 AS funnel_step,
       'review' AS funnel_name,
       total_users_ride_reviewed AS total_users
   FROM totals
)
SELECT *,
   CASE
       WHEN LAG(total_users) OVER (ORDER BY funnel_step) = 0 THEN NULL -- Handle division by zero
       ELSE (total_users::float / LAG(total_users) OVER (ORDER BY funnel_step)) * 100 -- Convert to percentage
   END AS percent_of_previous,
   CASE
       WHEN FIRST_VALUE(total_users) OVER (ORDER BY funnel_step) = 0 THEN NULL -- Handle division by zero
       ELSE (total_users::float / FIRST_VALUE(total_users) OVER (ORDER BY funnel_step)) * 100 -- Convert to percentage
   END AS percent_of_top
FROM funnel_stages
ORDER BY funnel_step;

/* 
Query 2: 
User Funnel Analysis 

This query is used to analyse the progression of users through the different stages of the funnel, providing insights into user engagement and conversion rates.
I've used it during the exploratory analysis to provide insights by identifying drop-off points in the user funnel and evaluating conversion rates between stages.

Here's a breakdown:

- user_funnel CTE (Common Table Expression):
It collects data on different stages of user engagement, such as app downloads, signups, ride requests, ride acceptance, ride completion, payment completion, and reviews.
Each SELECT statement represents a different stage, with a corresponding step name and the count of distinct users or user IDs.
Conditions are applied to include only relevant data, such as filtering out NULL timestamps and considering only approved payment transactions.

- final_tab CTE:
This CTE simply selects all columns from the previous CTE (user_funnel) and orders them by funnel_step.

- Main Query:
It selects all columns from the final_tab CTE.
Utilises the LAG() window function to calculate the drop-off from the previous stage.
Computes the drop-off rate and conversion rate based on the counts from the previous stage.
*/

WITH user_funnel AS(
  SELECT 
      1 AS funnel_step,
      'app downloads' AS step_name,
      COUNT(*) AS user_count
  FROM app_downloads
  UNION
  SELECT 
      2 AS funnel_step,
      'signups' AS step_name,
      COUNT(*) AS user_count
  FROM signups
  UNION
  SELECT
      3 AS funnel_step,
      'ride requests' AS step_name,
      COUNT(DISTINCT user_id) AS user_count
  FROM ride_requests 
  UNION
  SELECT
      4 AS funnel_step,
      'ride accepted' AS step_name,
      COUNT(DISTINCT user_id) AS user_count
  FROM ride_requests
  WHERE accept_ts IS NOT NULL
  UNION
  SELECT
      5 AS funnel_step,
      'ride completed' AS step_name,
      COUNT(DISTINCT user_id) AS user_count
  FROM ride_requests
  WHERE dropoff_ts IS NOT NULL
  UNION
  SELECT
      6 AS funnel_step,
      'payment completed' AS step_name,
      COUNT(DISTINCT r.user_id) AS user_count
  FROM transactions tt
  LEFT JOIN ride_requests r
  	ON tt.ride_id = r.ride_id
  WHERE tt.charge_status = 'Approved'
  UNION 
  SELECT
      7 AS funnel_step,
      'reviews' AS step_name,
      COUNT(DISTINCT user_id) AS user_count
  FROM reviews
),
final_tab AS(
  SELECT *
FROM user_funnel
ORDER BY funnel_step
)
SELECT
     *,
     LAG(user_count,1) OVER() AS drop_off,
     ROUND((1.0 - user_count::numeric/LAG(user_count,1) OVER()),2) AS drop_off_rate,
     ROUND((user_count::numeric/LAG(user_count,1)OVER()),2) AS conversion_rate
FROM final_tab;

/* 
Query 3: 
Ride Funnel Analysis 

This query is used to analyse the progression of ride requests through the different stages of the ride funnel, providing insights into user engagement and conversion rates.
I've used it during the exploratory analysis to provide insights by identifying drop-off points in the ride funnel and evaluating conversion rates between stages.

Here's a breakdown:

- ride_funnel CTE (Common Table Expression):
It gathers data on various stages of the ride process, such as ride requests, acceptance, pickup, dropoff, payment completion, and reviews.
Each SELECT statement represents a different stage, with a corresponding step name and the count of distinct ride IDs.
Conditions are applied to ensure that only relevant data is included, like filtering out NULL timestamps.

- final_tab CTE:
This CTE simply selects all columns from the previous CTE (ride_funnel) and orders them by funnel_step.

- Main Query:
It selects all columns from the final_tab CTE.
Utilises the LAG() window function to calculate the drop-off from the previous stage.
Computes the drop-off rate percentage and conversion rate percentage based on the counts from the previous stage.
*/

WITH ride_funnel AS(
  SELECT
       1 AS funnel_step,
      'ride requests' AS step_name,
      COUNT(DISTINCT ride_id) AS ride_count
  FROM ride_requests
  WHERE request_ts IS NOT NULL
  UNION 
  SELECT
       2 AS funnel_step,
      'ride accepted' AS step_name,
      COUNT(DISTINCT ride_id) AS ride_count
  FROM ride_requests
  WHERE accept_ts IS NOT NULL
  UNION
  SELECT
       3 AS funnel_step,
      'ride pickup' AS step_name,
      COUNT(DISTINCT ride_id) AS ride_count
  FROM ride_requests
  WHERE pickup_ts IS NOT NULL
  UNION
  SELECT
       4 AS funnel_step,
      'ride dropoff' AS step_name,
      COUNT(DISTINCT ride_id) AS ride_count
  FROM ride_requests
  WHERE dropoff_ts IS NOT NULL
  UNION
  SELECT
      5 AS funnel_step,
      'ride payment completed' AS step_name,
      COUNT(DISTINCT r.ride_id) AS ride_count
  FROM transactions tt
  LEFT JOIN ride_requests r
  	ON tt.ride_id = r.ride_id
  WHERE tt.charge_status = 'Approved'
  UNION 
  SELECT
      6 AS funnel_step,
      'ride reviews' AS step_name,
      COUNT(DISTINCT ride_id) AS ride_count
  FROM reviews
),
final_tab AS(
  SELECT *
FROM ride_funnel
ORDER BY funnel_step
)
SELECT
     *,
     LAG(ride_count,1) OVER() AS drop_off,
     (ROUND((1.0 - ride_count::numeric/LAG(ride_count,1) OVER()),2)*100) AS drop_off_rate_percentage,
     (ROUND((ride_count::numeric/LAG(ride_count,1)OVER()),2)*100) AS conversion_rate_percentage
FROM final_tab;


/* 
TABLEAU QUERIES - Aggregated datasets that I've used to generate the visualisations and dashboards in Tableau
*/

/*
- General funnel without filters
*/


WITH funnel_stages AS (
   SELECT
       0 AS funnel_step,
       'download' AS funnel_name,
       COUNT(DISTINCT ad.app_download_key)::bigint AS user_count,
       NULL::int AS ride_count
   FROM app_downloads ad
   LEFT JOIN signups s ON ad.app_download_key = s.session_id
   LEFT JOIN ride_requests rr ON s.user_id = rr.user_id
   GROUP BY funnel_step


   UNION


   SELECT
       1 AS funnel_step,
       'signup' AS funnel_name,
       COUNT(DISTINCT s.user_id)::bigint AS user_count,
       NULL::int AS ride_count
   FROM signups s
   LEFT JOIN ride_requests rr ON s.user_id = rr.user_id
   GROUP BY funnel_step


   UNION


   SELECT
       2 AS funnel_step,
       'ride_requested' AS funnel_name,
       COUNT(DISTINCT rr.user_id) AS user_count,
       COUNT(DISTINCT rr.ride_id) AS ride_count
   FROM ride_requests rr
   WHERE request_ts IS NOT NULL
   GROUP BY funnel_step


   UNION


   SELECT
       3 AS funnel_step,
       'ride_accepted' AS funnel_name,
       COUNT(DISTINCT rr.user_id) AS user_count,
       COUNT(DISTINCT rr.ride_id) AS ride_count
   FROM ride_requests rr
   WHERE accept_ts IS NOT NULL
   GROUP BY funnel_step


   UNION


   SELECT
       4 AS funnel_step,
       'ride_completed' AS funnel_name,
       COUNT(DISTINCT rr.user_id) AS user_count,
       COUNT(DISTINCT rr.ride_id) AS ride_count
   FROM ride_requests rr
   WHERE dropoff_ts IS NOT NULL
   GROUP BY funnel_step


   UNION


   SELECT
       5 AS funnel_step,
       'payment' AS funnel_name,
       COUNT(DISTINCT rr.user_id) AS user_count,
       COUNT(DISTINCT CASE WHEN tr.transaction_id IS NOT NULL THEN rr.ride_id END) AS ride_count
   FROM transactions tr
   LEFT JOIN ride_requests rr ON tr.ride_id = rr.ride_id
   WHERE tr.charge_status = 'Approved'
   GROUP BY funnel_step


   UNION


   SELECT
       6 AS funnel_step,
       'review' AS funnel_name,
       COUNT(DISTINCT r.user_id) AS user_count,
       COUNT(DISTINCT r.ride_id) AS ride_count
   FROM reviews r
   GROUP BY funnel_step
)
SELECT *
FROM funnel_stages
ORDER BY funnel_step;

/*
User funnel dataset - platform, age_range, download_date, user_count, ride_count

This SQL query aims to analyse the user funnel and ride funnel metrics at different stages by joining multiple tables and aggregating the data.

I've used it to generate visualisations and dashboards in Tableau to provide a comprehensive overview of user and ride funnel metrics at different stages, segmented by platform, age range, and download date, enabling stakeholders to understand user behavior and ride progression through various stages of the funnel.

Here's a breakdown:

- Common Table Expression (CTE) 'totals': This part of the query aggregates data related to app downloads, signups, ride requests, transactions, and reviews. It calculates various metrics such as the number of downloads, users signed up, rides requested, accepted, completed, paid, and reviewed, segmented by platform, age range, and download date.

- Common Table Expression (CTE) 'funnel_steps': This CTE represents each funnel step (downloads, signups, ride requested, ride accepted, ride completed, payment, and review) along with its corresponding metrics. Each step is derived from the 'totals' CTE and organized into a structured format for easy analysis.

- Main Query: The main query selects and presents the results with metrics for each funnel step by selecting columns from the 'funnel_steps' CTE. It orders the results by funnel step, platform, age range, and download date to facilitate analysis.
*/

-- CTE to calculate the metrics at the different funnel steps by joining the different tables and aggregating the data
WITH totals AS (
    SELECT
        ad.platform, 
        COALESCE(s.age_range, 'Unknown') AS age_ranges,
        ad.download_ts::date AS download_date,
  			COUNT(DISTINCT ad.app_download_key) AS total_downloads,
        COUNT(DISTINCT s.user_id) AS total_users_signed_up,
        COUNT(DISTINCT rr.user_id) AS total_users_ride_requested,
        COUNT(DISTINCT rr.user_id) FILTER(WHERE rr.accept_ts IS NOT NULL) AS total_users_ride_accepted,
        COUNT(DISTINCT rr.user_id) FILTER(WHERE rr.cancel_ts IS NULL) AS total_users_ride_completed,
        COUNT(DISTINCT rr.user_id) FILTER(WHERE tr.charge_status = 'Approved') AS total_users_ride_paid,
        COUNT(DISTINCT rv.user_id) AS total_users_with_review, 
        COUNT(rr.ride_id) AS total_rides_requested,
  			COUNT(rr.ride_id) FILTER(WHERE rr.accept_ts IS NOT NULL) AS total_rides_accepted,
        COUNT(rr.ride_id) FILTER(WHERE rr.cancel_ts IS NULL) AS total_rides_completed,
        COUNT(rr.ride_id) FILTER(WHERE tr.charge_status = 'Approved') AS total_rides_paid,
        COUNT(rr.ride_id) FILTER(WHERE tr.charge_status = 'Approved') AS total_rides_reviewed
    FROM app_downloads ad
    FULL JOIN signups s ON ad.app_download_key = s.session_id
    FULL JOIN ride_requests rr ON s.user_id = rr.user_id
    FULL JOIN transactions tr ON tr.ride_id = rr.ride_id
    FULL JOIN reviews rv ON rv.ride_id = rr.ride_id
    GROUP BY ad.platform, age_ranges, download_date
),

-- CTE to represent each funnel step with its corresponding metrics
funnel_steps AS (
    SELECT
        0 AS funnel_step,
        'downloads' AS funnel_name,
        total_downloads AS user_count, platform AS platform, age_ranges, download_date, 0 AS ride_count
    FROM totals

    UNION

    SELECT
        1 AS funnel_step,
        'signups' AS funnel_name,
        total_users_signed_up AS user_count, platform AS platform, age_ranges, download_date, 0 AS ride_count
    FROM totals

    UNION

    SELECT
        2 AS funnel_step,
        'ride_requested' AS funnel_name,
        total_users_ride_requested AS user_count, platform AS platform, age_ranges, download_date, total_rides_requested AS ride_count
    FROM totals

    UNION

    SELECT
        3 AS funnel_step,
        'ride_accepted' AS funnel_name,
        total_users_ride_accepted AS user_count, platform AS platform, age_ranges, download_date, total_rides_accepted AS ride_count
    FROM totals

    UNION

    SELECT
        4 AS funnel_step,
        'ride_completed' AS funnel_name,
        total_users_ride_completed AS user_count, platform AS platform, age_ranges, download_date, total_rides_completed AS ride_count
    FROM totals

    UNION

    SELECT
        5 AS funnel_step,
        'payment' AS funnel_name,
        total_users_ride_paid AS user_count, platform AS platform, age_ranges, download_date, total_rides_paid AS ride_count
    FROM totals

    UNION

    SELECT
        6 AS funnel_step,
        'review' AS funnel_name,
        total_users_with_review AS user_count, platform AS platform, age_ranges, download_date, total_rides_reviewed AS ride_count
    FROM totals
    ORDER BY funnel_step
)

-- Main query: Selects and presents the results with metrics for each funnel step by selecting columns from the funnel_steps CTE
SELECT funnel_step, funnel_name, platform, age_ranges, download_date, user_count, ride_count FROM funnel_steps 
ORDER BY funnel_step, platform, age_ranges, download_date;


/*
Ride funnel dataset - platform, age_range, download_date, ride_count

This SQL query is aimed at analysing the funnel of ride requests through various stages (requested, accepted, started, completed, payment completed, reviewed) broken down by platform and age range.

I've used it to generate visualisations and dashboards in Tableau to provide insights into how ride requests progress through different stages of the ride funnel, allowing for analysis of user behavior and ride completion rates segmented by platform and age range.

Here's a breakdown:

- Common Table Expression (CTE) 'totals': This part of the query aggregates data related to ride requests, including user information, timestamps, and various stages of the ride process. It joins multiple tables such as app_downloads, signups, ride_requests, transactions, and reviews to gather comprehensive data.

- Main Query: The main query combines multiple SELECT statements using UNION to generate a single result set. Each SELECT statement represents a stage of the ride process and calculates the count of distinct rides at that stage, segmented by platform and age range.

- GROUP BY clause: Groups the data by platform, age range, and download date to aggregate the ride counts for each segment.

- UNION: Combines the results of each SELECT statement into a single result set.
*/


WITH totals AS (
 	 SELECT
	   ad.app_download_key AS downloads,
 	   ad.platform,
 	   date_trunc('DAY', ad.download_ts) AS download_date,
  	 s.user_id AS user_signed_up, 
  		s.age_range,
    	rr.user_id AS user_requested_ride,
    	rr.ride_id AS ride_requested, 
         CASE WHEN rr.request_ts IS NOT NULL THEN rr.user_id END AS user_request,
    	   CASE WHEN rr.accept_ts IS NOT NULL THEN rr.user_id END AS user_accepted,
    	   CASE WHEN rr.accept_ts IS NOT NULL THEN rr.ride_id END AS rides_accepted,
         CASE WHEN rr.pickup_ts IS NOT NULL THEN rr.user_id END AS user_started,
    	   CASE WHEN rr.pickup_ts IS NOT NULL THEN rr.ride_id END AS rides_started,
    	   CASE WHEN rr.dropoff_ts IS NOT NULL THEN rr.user_id END AS user_completed,
    	   CASE WHEN rr.dropoff_ts IS NOT NULL THEN rr.ride_id END AS rides_completed, 
    	   CASE WHEN tr.charge_status = 'Approved' THEN rr.user_id END AS user_paid, 
    	   CASE WHEN tr.charge_status = 'Approved' THEN tr.ride_id END AS rides_paid, 
       rv.ride_id AS ride_reviewed, 
  	   rv.user_id AS user_reviewed
         FROM app_downloads AS ad 
	 LEFT JOIN signups AS s ON ad.app_download_key = s.session_id
	 LEFT JOIN ride_requests AS rr ON s.user_id = rr.user_id
	 LEFT JOIN transactions AS tr ON rr.ride_id = tr.ride_id
	 LEFT JOIN reviews AS rv ON rr.ride_id = rv.ride_id)
SELECT 
    1 AS funnel_step, 
    'Ride requested' AS funnel_name, 
    platform, 
    age_range, 
    download_date, 
    COUNT(DISTINCT ride_requested) AS ride_count
FROM totals
GROUP BY platform, age_range, download_date
UNION
SELECT 
    2 AS funnel_step, 
    'Ride accepted' AS funnel_name, 
    platform, 
    age_range, 
    download_date, 
    COUNT(DISTINCT rides_accepted) AS ride_count
FROM totals 
GROUP BY platform, age_range, download_date
UNION 
SELECT 
    3 AS funnel_step, 
    'Ride started' AS funnel_name, 
    platform, 
    age_range, 
    download_date, 
    COUNT(DISTINCT rides_started) AS ride_count
FROM totals 
GROUP BY platform, age_range, download_date
UNION
SELECT 
    4 AS funnel_step, 
    'Ride completed' AS funnel_name, 
    platform, 
    age_range, 
    download_date,  
    COUNT(DISTINCT rides_completed) AS ride_count
FROM totals
GROUP BY platform, age_range, download_date
UNION
SELECT 
    5 AS funnel_step, 
    'Ride payment' AS funnel_name, 
    platform, 
    age_range, 
    download_date, 
    COUNT(DISTINCT rides_paid) AS ride_count
FROM totals
GROUP BY platform, age_range, download_date
UNION
SELECT 
    6 AS funnel_step, 
    'Ride review' AS funnel_name, 
    platform, 
    age_range, 
    download_date, 
    COUNT(DISTINCT ride_reviewed) AS ride_count
FROM totals
GROUP BY platform, age_range, download_date;


/*
Ride funnel dataset - platform, age_range, request_hour, ride_count

This SQL query is designed to analyse the distribution of ride requests throughout the day, segmented by various stages of the ride funnel, platform, and age range. 

I've used it to generate visualisations and dashboards in Tableau to provide insights into the distribution of ride requests throughout the day, allowing analysis of user behavior and ride progression at different stages of the process.

Here's a breakdown:

- Common Table Expression (CTE) 'totals': This part of the query gathers data related to ride requests, including user information, request timestamps, ride statuses, transaction details, and reviews. It joins multiple tables such as app_downloads, signups, ride_requests, transactions, and reviews.

- Main Query: This part of the query consists of several SELECT statements combined using UNION ALL to form a single result set. Each SELECT statement represents a stage of the ride process (funnel), such as ride requested, accepted, started, completed, payment completed, and reviewed. Each stage includes the platform, age range, request hour, and the count of rides at that stage.

- WHERE clause: Filters out records where the request hour is not NULL, ensuring that only valid data is included in the analysis.

- GROUP BY clause: Groups the data by platform, age range, and request hour to aggregate ride counts for each segment.

- ORDER BY clause: Orders the final result set by platform, age range, request hour, and funnel step to present the data in a structured manner.
*/

WITH totals AS (
    SELECT
        ad.app_download_key AS downloads,
        ad.platform,
        date_trunc('DAY', rr.request_ts) AS request_date,
        EXTRACT(HOUR FROM rr.request_ts) AS request_hour,
        s.user_id AS user_signed_up, 
        s.age_range,
        rr.user_id AS user_requested_ride,
        rr.ride_id AS ride_requested, 
        CASE WHEN rr.request_ts IS NOT NULL THEN rr.user_id END AS user_request,
        CASE WHEN rr.accept_ts IS NOT NULL THEN rr.user_id END AS user_accepted,
        CASE WHEN rr.accept_ts IS NOT NULL THEN rr.ride_id END AS rides_accepted,
        CASE WHEN rr.pickup_ts IS NOT NULL THEN rr.user_id END AS user_started,
        CASE WHEN rr.pickup_ts IS NOT NULL THEN rr.ride_id END AS rides_started,
        CASE WHEN rr.dropoff_ts IS NOT NULL THEN rr.user_id END AS user_completed,
        CASE WHEN rr.dropoff_ts IS NOT NULL THEN rr.ride_id END AS rides_completed, 
        CASE WHEN tr.charge_status = 'Approved' THEN rr.user_id END AS user_paid, 
        CASE WHEN tr.charge_status = 'Approved' THEN tr.ride_id END AS rides_paid, 
        rv.ride_id AS ride_reviewed, 
        rv.user_id AS user_reviewed
    FROM 
        app_downloads AS ad 
    LEFT JOIN 
        signups AS s ON ad.app_download_key = s.session_id
    LEFT JOIN 
        ride_requests AS rr ON s.user_id = rr.user_id
    LEFT JOIN 
        transactions AS tr ON rr.ride_id = tr.ride_id
    LEFT JOIN 
        reviews AS rv ON rr.ride_id = rv.ride_id
)
SELECT 
    1 AS funnel_step, 
    'Ride requested' AS funnel_name, 
    platform, 
    age_range, 
    request_hour, 
    COUNT(DISTINCT ride_requested) AS ride_count
FROM 
    totals
WHERE
    request_hour IS NOT NULL
GROUP BY 
    platform, 
    age_range, 
    request_hour

UNION ALL

SELECT 
    2 AS funnel_step, 
    'Ride accepted' AS funnel_name, 
    platform, 
    age_range, 
    request_hour, 
    COUNT(DISTINCT rides_accepted) AS ride_count
FROM 
    totals
WHERE
    request_hour IS NOT NULL
GROUP BY 
    platform, 
    age_range, 
    request_hour

UNION ALL

SELECT 
    3 AS funnel_step, 
    'Ride started' AS funnel_name, 
    platform, 
    age_range, 
    request_hour, 
    COUNT(DISTINCT rides_started) AS ride_count
FROM 
    totals
WHERE
    request_hour IS NOT NULL
GROUP BY 
    platform, 
    age_range, 
    request_hour

UNION ALL

SELECT 
    4 AS funnel_step, 
    'Ride completed' AS funnel_name, 
    platform, 
    age_range, 
    request_hour, 
    COUNT(DISTINCT rides_completed) AS ride_count
FROM 
    totals
WHERE
    request_hour IS NOT NULL
GROUP BY 
    platform, 
    age_range, 
    request_hour

UNION ALL

SELECT 
    5 AS funnel_step, 
    'Ride payment' AS funnel_name, 
    platform, 
    age_range, 
    request_hour, 
    COUNT(DISTINCT rides_paid) AS ride_count
FROM 
    totals
WHERE
    request_hour IS NOT NULL
GROUP BY 
    platform, 
    age_range, 
    request_hour

UNION ALL

SELECT 
    6 AS funnel_step, 
    'Ride review' AS funnel_name, 
    platform, 
    age_range, 
    request_hour, 
    COUNT(DISTINCT ride_reviewed) AS ride_count
FROM 
    totals
WHERE
    request_hour IS NOT NULL
GROUP BY 
    platform, 
    age_range, 
    request_hour

ORDER BY
    platform, 
    age_range, 
    request_hour, 
    funnel_step;


/*
Ride Requests and Payments

This SQL query retrieves specific data related to ride requests and their corresponding transactions. 

The purpose of this query is to gather detailed information about ride requests and their associated transactions, enabling analysis on Tableau on various aspects such as ride status.

Here's a breakdown:

- SELECT statement: Specifies the columns to be included in the final result set. It includes various attributes such as user_id, ride_id, driver_id, timestamps for request, acceptance, pickup, dropoff, and cancellation, as well as transaction-related information like purchase_amount_usd and charge_status.

- FROM clause: Specifies the tables from which the data is being retrieved. The primary table is ride_requests (aliased as 'r'), which contains information about ride requests. It's joined with the transactions table (aliased as 'tt') using INNER JOIN based on the common ride_id column.
*/

SELECT
    r.user_id,
    r.ride_id,
    r.driver_id,
    r.request_ts,
    r.accept_ts,
    r.pickup_location,
    r.pickup_ts,
    r.dropoff_location,
    r.dropoff_ts,
    r.cancel_ts,
    tt.purchase_amount_usd,
    tt.charge_status    
FROM ride_requests r
INNER JOIN transactions tt ON r.ride_id = tt.ride_id;



