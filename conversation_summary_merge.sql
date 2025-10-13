-- MERGE statement to create/update conversation_summary table
MERGE INTO `waba-454907.whatsapp_analytics.conversation_summary` AS T
USING (
-- This subquery calculates lifetime metrics for each conversation
WITH
events_with_day AS (
SELECT
*,
EXTRACT(DAYOFWEEK FROM message_timestamp AT TIME ZONE 'Asia/Kolkata') AS day_of_week
FROM
`waba-454907.whatsapp_analytics.message_events`
),
events_with_lag AS (
SELECT
*,
LAG(direction, 1) OVER (PARTITION BY chat_id ORDER BY message_timestamp) AS prev_direction,
LAG(message_timestamp, 1) OVER (PARTITION BY chat_id ORDER BY message_timestamp) AS prev_message_timestamp
FROM
events_with_day
),
-- Collect working hours data for each user/org combination
user_working_hours AS (
SELECT
user_id,
org_id,
MAX(CASE WHEN day_of_week = 1 THEN start_time_utc END) AS monday_start,
MAX(CASE WHEN day_of_week = 1 THEN end_time_utc END) AS monday_end,
MAX(CASE WHEN day_of_week = 2 THEN start_time_utc END) AS tuesday_start,
MAX(CASE WHEN day_of_week = 2 THEN end_time_utc END) AS tuesday_end,
MAX(CASE WHEN day_of_week = 3 THEN start_time_utc END) AS wednesday_start,
MAX(CASE WHEN day_of_week = 3 THEN end_time_utc END) AS wednesday_end,
MAX(CASE WHEN day_of_week = 4 THEN start_time_utc END) AS thursday_start,
MAX(CASE WHEN day_of_week = 4 THEN end_time_utc END) AS thursday_end,
MAX(CASE WHEN day_of_week = 5 THEN start_time_utc END) AS friday_start,
MAX(CASE WHEN day_of_week = 5 THEN end_time_utc END) AS friday_end,
MAX(CASE WHEN day_of_week = 6 THEN start_time_utc END) AS saturday_start,
MAX(CASE WHEN day_of_week = 6 THEN end_time_utc END) AS saturday_end,
MAX(CASE WHEN day_of_week = 7 THEN start_time_utc END) AS sunday_start,
MAX(CASE WHEN day_of_week = 7 THEN end_time_utc END) AS sunday_end
FROM
`waba-454907.whatsapp_analytics.working_hours`
GROUP BY user_id, org_id
),
response_events AS (
SELECT
events.chat_id,
events.user_id,
events.org_id,
events.message_timestamp,
events.prev_message_timestamp
FROM
events_with_lag AS events
WHERE
events.direction = 'OUTGOING' AND events.prev_direction = 'INCOMING'
),
chat_aggregates AS (
SELECT
chat_id,
agent_phone_number as phone_number,
MAX(org_id) as org_id,
MAX(user_id) as user_id,
MIN(message_timestamp) AS conversation_start_ts,
MAX(message_timestamp) AS last_message_ts,
ARRAY_AGG(STRUCT(direction) ORDER BY message_timestamp ASC LIMIT 1)[OFFSET(0)].direction AS starter_direction,
COUNTIF(direction = 'INCOMING') AS contact_message_count,
COUNTIF(direction = 'OUTGOING') AS agent_message_count,
MIN(IF(direction = 'INCOMING', message_timestamp, NULL)) AS first_contact_message_ts,
MIN(IF(direction = 'OUTGOING', message_timestamp, NULL)) AS first_agent_message_ts,
-- Additional fields needed for conversation_summary schema
COUNT(DISTINCT message_id) AS unique_messages,
COUNTIF(direction = 'OUTGOING' AND prev_direction = 'OUTGOING') AS follow_up_count,
ARRAY_AGG(STRUCT(direction) ORDER BY message_timestamp DESC LIMIT 1)[OFFSET(0)].direction AS last_message_direction
FROM
events_with_lag
GROUP BY
chat_id, agent_phone_number
)
-- Final combination of all metrics
SELECT
agg.chat_id,
agg.user_id AS uid,
agg.org_id,
agg.phone_number,
-- Analytics record structure
STRUCT(
  agg.agent_message_count AS messages_sent,
  agg.contact_message_count AS messages_received,
  (agg.agent_message_count + agg.contact_message_count) AS total_messages,
  agg.unique_messages,
  agg.follow_up_count AS number_of_follow_ups
) AS analytics,
-- Calculate Average Response Time using working hours UDF
(SELECT AVG(
  `waba-454907.whatsapp_analytics.calculate_working_seconds_sql`(
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
)
FROM response_events re 
LEFT JOIN user_working_hours wh ON re.user_id = wh.user_id AND re.org_id = wh.org_id
WHERE re.chat_id = agg.chat_id
) AS average_response_time,
-- Conversation starter mapping
IF(agg.starter_direction = 'OUTGOING', 'employee', 'contact') AS conversation_starter,
-- Last message from mapping
IF(agg.last_message_direction = 'OUTGOING', 'employee', 'contact') AS last_message_from,
agg.conversation_start_ts AS created_at,
agg.last_message_ts AS updated_at
FROM
chat_aggregates agg
) AS S
ON T.uid = S.uid 
   AND T.chat_id = S.chat_id
   AND T.phone_number = S.phone_number
WHEN MATCHED THEN UPDATE SET 
    T.org_id = S.org_id,
    T.analytics = S.analytics,
    T.average_response_time = S.average_response_time,
    T.conversation_starter = S.conversation_starter,
    T.last_message_from = S.last_message_from,
    T.updated_at = S.updated_at
WHEN NOT MATCHED THEN INSERT (
    uid,
    org_id,
    chat_id,
    phone_number,
    analytics,
    average_response_time,
    conversation_starter,
    last_message_from,
    created_at,
    updated_at
) VALUES (
    S.uid,
    S.org_id,
    S.chat_id,
    S.phone_number,
    S.analytics,
    S.average_response_time,
    S.conversation_starter,
    S.last_message_from,
    S.created_at,
    S.updated_at
);
