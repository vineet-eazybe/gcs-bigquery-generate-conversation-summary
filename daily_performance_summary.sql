-- FIXED MERGE statement for daily_performance_summary table
-- 
-- FIXES APPLIED:
-- 1. Corrected day-of-week mapping (2=Monday, 3=Tuesday, etc. per BigQuery DAYOFWEEK)
-- 2. Added org_id to MERGE ON condition to prevent "multiple source rows" error
-- 3. Added org_id to GROUP BY in daily_aggregates for proper user/org separation
-- 4. All calculations performed in UTC to match UTC-stored working hours
--
-- Last updated: October 2025

MERGE INTO `waba-454907.whatsapp_analytics.daily_performance_summary` AS T
USING (
-- This subquery calculates all daily conversation metrics
WITH
daily_events AS (
SELECT
*,
DATE(message_timestamp) AS activity_date,
EXTRACT(DAYOFWEEK FROM message_timestamp) AS day_of_week
FROM
`waba-454907.whatsapp_analytics.message_events`
),
events_with_daily_lag AS (
SELECT
*,
LAG(direction, 1) OVER (PARTITION BY user_id, chat_id ORDER BY message_timestamp) AS prev_direction,
LAG(message_timestamp, 1) OVER (PARTITION BY user_id, chat_id ORDER BY message_timestamp) AS prev_message_timestamp
FROM
daily_events
),
-- Collect working hours data for each user/org combination
-- CORRECT mapping: BigQuery DAYOFWEEK returns 1=Sunday, 2=Monday, 3=Tuesday, etc.
user_working_hours AS (
SELECT
user_id,
org_id,
MAX(CASE WHEN day_of_week = 2 THEN start_time_utc END) AS monday_start,
MAX(CASE WHEN day_of_week = 2 THEN end_time_utc END) AS monday_end,
MAX(CASE WHEN day_of_week = 3 THEN start_time_utc END) AS tuesday_start,
MAX(CASE WHEN day_of_week = 3 THEN end_time_utc END) AS tuesday_end,
MAX(CASE WHEN day_of_week = 4 THEN start_time_utc END) AS wednesday_start,
MAX(CASE WHEN day_of_week = 4 THEN end_time_utc END) AS wednesday_end,
MAX(CASE WHEN day_of_week = 5 THEN start_time_utc END) AS thursday_start,
MAX(CASE WHEN day_of_week = 5 THEN end_time_utc END) AS thursday_end,
MAX(CASE WHEN day_of_week = 6 THEN start_time_utc END) AS friday_start,
MAX(CASE WHEN day_of_week = 6 THEN end_time_utc END) AS friday_end,
MAX(CASE WHEN day_of_week = 7 THEN start_time_utc END) AS saturday_start,
MAX(CASE WHEN day_of_week = 7 THEN end_time_utc END) AS saturday_end,
MAX(CASE WHEN day_of_week = 1 THEN start_time_utc END) AS sunday_start,
MAX(CASE WHEN day_of_week = 1 THEN end_time_utc END) AS sunday_end
FROM
`waba-454907.whatsapp_analytics.working_hours`
GROUP BY user_id, org_id
),
response_events AS (
SELECT
events.activity_date,
events.user_id,
events.org_id,
events.chat_id,
events.message_timestamp,
events.prev_message_timestamp
FROM
events_with_daily_lag AS events
WHERE
events.direction = 'OUTGOING' 
AND events.prev_direction = 'INCOMING'
AND events.prev_message_timestamp IS NOT NULL
),
daily_aggregates AS (
SELECT
activity_date,
user_id,
org_id,
agent_phone_number,
chat_id AS contact_id,
ARRAY_AGG(STRUCT(direction) ORDER BY message_timestamp ASC LIMIT 1)[OFFSET(0)].direction AS starter_direction,
COUNTIF(direction = 'INCOMING') AS contact_message_count,
COUNTIF(direction = 'OUTGOING') AS agent_message_count,
MIN(IF(direction = 'INCOMING', message_timestamp, NULL)) AS first_contact_message_ts,
MIN(IF(direction = 'OUTGOING', message_timestamp, NULL)) AS first_agent_message_ts
FROM
events_with_daily_lag
WHERE user_id IS NOT NULL
GROUP BY
activity_date, user_id, org_id, chat_id, agent_phone_number
)
-- Final combination of all metrics
SELECT
agg.activity_date,
agg.user_id,
agg.contact_id,
agg.agent_phone_number,
agg.org_id,
IF(agg.starter_direction = 'OUTGOING', 'agent', 'contact') AS conversation_starter_of_day,
agg.agent_message_count,
agg.contact_message_count,
-- Calculate average response time using working hours UDF (only count responses within working hours)
(SELECT 
  AVG(
    CASE 
      WHEN `waba-454907.whatsapp_analytics.calculate_working_seconds_sql`(
        re.prev_message_timestamp,
        re.message_timestamp,
        COALESCE(wh.monday_start, TIME(0, 0, 0)),
        COALESCE(wh.monday_end, TIME(0, 0, 0)),
        COALESCE(wh.tuesday_start, TIME(0, 0, 0)),
        COALESCE(wh.tuesday_end, TIME(0, 0, 0)),
        COALESCE(wh.wednesday_start, TIME(0, 0, 0)),
        COALESCE(wh.wednesday_end, TIME(0, 0, 0)),
        COALESCE(wh.thursday_start, TIME(0, 0, 0)),
        COALESCE(wh.thursday_end, TIME(0, 0, 0)),
        COALESCE(wh.friday_start, TIME(0, 0, 0)),
        COALESCE(wh.friday_end, TIME(0, 0, 0)),
        COALESCE(wh.saturday_start, TIME(0, 0, 0)),
        COALESCE(wh.saturday_end, TIME(0, 0, 0)),
        COALESCE(wh.sunday_start, TIME(0, 0, 0)),
        COALESCE(wh.sunday_end, TIME(0, 0, 0))
      ) > 0
      THEN `waba-454907.whatsapp_analytics.calculate_working_seconds_sql`(
        re.prev_message_timestamp,
        re.message_timestamp,
        COALESCE(wh.monday_start, TIME(0, 0, 0)),
        COALESCE(wh.monday_end, TIME(0, 0, 0)),
        COALESCE(wh.tuesday_start, TIME(0, 0, 0)),
        COALESCE(wh.tuesday_end, TIME(0, 0, 0)),
        COALESCE(wh.wednesday_start, TIME(0, 0, 0)),
        COALESCE(wh.wednesday_end, TIME(0, 0, 0)),
        COALESCE(wh.thursday_start, TIME(0, 0, 0)),
        COALESCE(wh.thursday_end, TIME(0, 0, 0)),
        COALESCE(wh.friday_start, TIME(0, 0, 0)),
        COALESCE(wh.friday_end, TIME(0, 0, 0)),
        COALESCE(wh.saturday_start, TIME(0, 0, 0)),
        COALESCE(wh.saturday_end, TIME(0, 0, 0)),
        COALESCE(wh.sunday_start, TIME(0, 0, 0)),
        COALESCE(wh.sunday_end, TIME(0, 0, 0))
      )
    END
  )
FROM response_events re 
LEFT JOIN user_working_hours wh ON re.user_id = wh.user_id AND re.org_id = wh.org_id
WHERE re.user_id = agg.user_id AND re.chat_id = agg.contact_id AND re.activity_date = agg.activity_date
) AS avg_agent_response_time_seconds,
-- Calculate time to first response using working hours UDF
IF(
agg.first_agent_message_ts > agg.first_contact_message_ts,
(SELECT 
  `waba-454907.whatsapp_analytics.calculate_working_seconds_sql`(
    agg.first_contact_message_ts,
    agg.first_agent_message_ts,
    COALESCE(wh.monday_start, TIME(0, 0, 0)),
    COALESCE(wh.monday_end, TIME(0, 0, 0)),
    COALESCE(wh.tuesday_start, TIME(0, 0, 0)),
    COALESCE(wh.tuesday_end, TIME(0, 0, 0)),
    COALESCE(wh.wednesday_start, TIME(0, 0, 0)),
    COALESCE(wh.wednesday_end, TIME(0, 0, 0)),
    COALESCE(wh.thursday_start, TIME(0, 0, 0)),
    COALESCE(wh.thursday_end, TIME(0, 0, 0)),
    COALESCE(wh.friday_start, TIME(0, 0, 0)),
    COALESCE(wh.friday_end, TIME(0, 0, 0)),
    COALESCE(wh.saturday_start, TIME(0, 0, 0)),
    COALESCE(wh.saturday_end, TIME(0, 0, 0)),
    COALESCE(wh.sunday_start, TIME(0, 0, 0)),
    COALESCE(wh.sunday_end, TIME(0, 0, 0))
  )
FROM user_working_hours wh
WHERE wh.user_id = agg.user_id AND wh.org_id = agg.org_id
),
NULL
) AS time_to_first_response_seconds
FROM
daily_aggregates agg
) AS S
ON T.activity_date = S.activity_date 
   AND T.user_id = S.user_id 
   AND T.org_id = S.org_id
   AND T.contact_id = S.contact_id
   AND T.user_number = S.agent_phone_number
WHEN MATCHED THEN UPDATE SET 
    T.org_id = S.org_id,
    T.conversation_starter_of_day = S.conversation_starter_of_day,
    T.agent_message_count = S.agent_message_count,
    T.contact_message_count = S.contact_message_count,
    T.avg_agent_response_time_seconds = S.avg_agent_response_time_seconds,
    T.time_to_first_response_seconds = S.time_to_first_response_seconds,
    T.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    activity_date,
    user_id,
    user_number,
    contact_id,
    org_id,
    conversation_starter_of_day,
    agent_message_count,
    contact_message_count,
    avg_agent_response_time_seconds,
    time_to_first_response_seconds,
    created_at,
    updated_at
) VALUES (
    S.activity_date,
    S.user_id,
    S.agent_phone_number, -- user_number from agent_phone_number
    S.contact_id,
    S.org_id,
    S.conversation_starter_of_day,
    S.agent_message_count,
    S.contact_message_count,
    S.avg_agent_response_time_seconds,
    S.time_to_first_response_seconds,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);