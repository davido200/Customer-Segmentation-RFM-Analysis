WITH weekly_cohort AS (
    -- Assign each user to a cohort based on the week of their first visit(registration)
    SELECT
        user_pseudo_id,
        DATE_TRUNC(PARSE_DATE('%Y%m%d', MIN(event_date)), WEEK(SUNDAY)) AS cohort_week
    FROM
        turing_data_analytics.raw_events
    GROUP BY
        user_pseudo_id
),
revenue_table AS (
    -- Calculate the revenue per cohort week for the next 12 weeks
    SELECT
        wc.cohort_week,
        --COUNT(DISTINCT wc.user_pseudo_id) AS registrations,
        ROUND(SUM(CASE
                WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK(SUNDAY)) = DATE_ADD(wc.cohort_week, INTERVAL 0 WEEK)
                THEN re.purchase_revenue_in_usd
                ELSE 0
            END) / COUNT(DISTINCT wc.user_pseudo_id), 4) AS week_0,
        ROUND(SUM(CASE
                WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK(SUNDAY)) = DATE_ADD(wc.cohort_week, INTERVAL 1 WEEK)
                THEN re.purchase_revenue_in_usd
                ELSE 0
            END) / COUNT(DISTINCT wc.user_pseudo_id), 4) AS week_1,
        ROUND(SUM(CASE
                WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK(SUNDAY)) = DATE_ADD(wc.cohort_week, INTERVAL 2 WEEK)
                THEN re.purchase_revenue_in_usd
                ELSE 0
            END) / COUNT(DISTINCT wc.user_pseudo_id), 4) AS week_2,
        ROUND(SUM(CASE
                WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK(SUNDAY)) = DATE_ADD(wc.cohort_week, INTERVAL 3 WEEK)
                THEN re.purchase_revenue_in_usd
                ELSE 0
            END) / COUNT(DISTINCT wc.user_pseudo_id), 4) AS week_3,
        ROUND(SUM(CASE
                WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK(SUNDAY)) = DATE_ADD(wc.cohort_week, INTERVAL 4 WEEK)
                THEN re.purchase_revenue_in_usd
                ELSE 0
            END) / COUNT(DISTINCT wc.user_pseudo_id), 4) AS week_4,
        ROUND(SUM(CASE
                WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK(SUNDAY)) = DATE_ADD(wc.cohort_week, INTERVAL 5 WEEK)
                THEN re.purchase_revenue_in_usd
                ELSE 0
            END) / COUNT(DISTINCT wc.user_pseudo_id), 4) AS week_5,
        ROUND(SUM(CASE
                WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK(SUNDAY)) = DATE_ADD(wc.cohort_week, INTERVAL 6 WEEK)
                THEN re.purchase_revenue_in_usd
                ELSE 0
            END) / COUNT(DISTINCT wc.user_pseudo_id), 4) AS week_6,
        ROUND(SUM(CASE
                WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK(SUNDAY)) = DATE_ADD(wc.cohort_week, INTERVAL 7 WEEK)
                THEN re.purchase_revenue_in_usd
                ELSE 0
            END) / COUNT(DISTINCT wc.user_pseudo_id), 4) AS week_7,
        ROUND(SUM(CASE
                WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK(SUNDAY)) = DATE_ADD(wc.cohort_week, INTERVAL 8 WEEK)
                THEN re.purchase_revenue_in_usd
                ELSE 0
            END) / COUNT(DISTINCT wc.user_pseudo_id), 4) AS week_8,
        ROUND(SUM(CASE
                WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK(SUNDAY)) = DATE_ADD(wc.cohort_week, INTERVAL 9 WEEK)
                THEN re.purchase_revenue_in_usd
                ELSE 0
            END) / COUNT(DISTINCT wc.user_pseudo_id), 4) AS week_9,
        ROUND(SUM(CASE
                WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK(SUNDAY)) = DATE_ADD(wc.cohort_week, INTERVAL 10 WEEK)
                THEN re.purchase_revenue_in_usd
                ELSE 0
            END) / COUNT(DISTINCT wc.user_pseudo_id), 4) AS week_10,
        ROUND(SUM(CASE
                WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK(SUNDAY)) = DATE_ADD(wc.cohort_week, INTERVAL 11 WEEK)
                THEN re.purchase_revenue_in_usd
                ELSE 0
            END) / COUNT(DISTINCT wc.user_pseudo_id), 4) AS week_11,
        ROUND(SUM(CASE
                WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK(SUNDAY)) = DATE_ADD(wc.cohort_week, INTERVAL 12 WEEK)
                THEN re.purchase_revenue_in_usd
                ELSE 0
            END) / COUNT(DISTINCT wc.user_pseudo_id), 4) AS week_12
    FROM
        turing_data_analytics.raw_events re
    JOIN
        weekly_cohort wc
    ON
        re.user_pseudo_id = wc.user_pseudo_id
    GROUP BY
        wc.cohort_week
    ORDER BY wc.cohort_week
),
cumulative_revenue AS (
    -- Create an array of weekly revenues
    SELECT
        cohort_week,
        ARRAY[
            week_0, week_1, week_2, week_3, week_4, week_5,
            week_6, week_7, week_8, week_9, week_10, week_11, week_12
        ] AS weekly_revenues
    FROM
        revenue_table
),
cumulative_sum_table AS (
    -- Calculate cumulative sums using array accumulation
    SELECT
        cohort_week,
        
        ARRAY(SELECT ROUND(SUM(weekly_revenues[i]) OVER (ORDER BY i),4) 
              FROM UNNEST(GENERATE_ARRAY(0, 12)) AS i) AS cumulative_revenues
    FROM
        cumulative_revenue
),
cumulative_table AS(
SELECT
    cohort_week,
    ROUND(cumulative_revenues[OFFSET(0)],4) AS week_0,
    ROUND(cumulative_revenues[OFFSET(1)],4) AS week_1,
    ROUND(cumulative_revenues[OFFSET(2)],4) AS week_2,
    ROUND(cumulative_revenues[OFFSET(3)],4) AS week_3,
    ROUND(cumulative_revenues[OFFSET(4)],4) AS week_4,
    ROUND(cumulative_revenues[OFFSET(5)],4) AS week_5,
    ROUND(cumulative_revenues[OFFSET(6)],4) AS week_6,
    ROUND(cumulative_revenues[OFFSET(7)],4) AS week_7,
    ROUND(cumulative_revenues[OFFSET(8)],4) AS week_8,
    ROUND(cumulative_revenues[OFFSET(9)],4) AS week_9,
    ROUND(cumulative_revenues[OFFSET(10)],4) AS week_10,
    ROUND(cumulative_revenues[OFFSET(11)],4) AS week_11,
    ROUND(cumulative_revenues[OFFSET(12)],4) AS week_12
FROM
    cumulative_sum_table
ORDER BY
    cohort_week
)
SELECT*
FROM revenue_table
