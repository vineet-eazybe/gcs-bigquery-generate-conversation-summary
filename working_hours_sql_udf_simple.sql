-- Simple and correct UDF for calculating working hours intersection
-- This UDF properly handles responses outside working hours
-- Now with timezone support

CREATE OR REPLACE FUNCTION `waba-454907.whatsapp_analytics.calculate_working_seconds_sql`(
  start_timestamp TIMESTAMP,
  end_timestamp TIMESTAMP,
  monday_start TIME,
  monday_end TIME,
  tuesday_start TIME,
  tuesday_end TIME,
  wednesday_start TIME,
  wednesday_end TIME,
  thursday_start TIME,
  thursday_end TIME,
  friday_start TIME,
  friday_end TIME,
  saturday_start TIME,
  saturday_end TIME,
  sunday_start TIME,
  sunday_end TIME
)
RETURNS FLOAT64
AS (
  -- If start >= end, return 0
  CASE 
    WHEN start_timestamp >= end_timestamp THEN 0
    ELSE (
      -- For same day responses, calculate working time within that day
      -- Convert to Asia/Kolkata timezone for consistent day-of-week calculation
      CASE 
        WHEN DATE(start_timestamp, 'Asia/Kolkata') = DATE(end_timestamp, 'Asia/Kolkata') THEN
          -- Same day - check if there's any overlap with working hours
          CASE 
            WHEN EXTRACT(DAYOFWEEK FROM start_timestamp AT TIME ZONE 'Asia/Kolkata') = 2 AND monday_start IS NOT NULL AND monday_end IS NOT NULL AND NOT (monday_start = TIME(0,0,0) AND monday_end = TIME(0,0,0)) THEN
              CASE 
                WHEN TIME(start_timestamp, 'Asia/Kolkata') < monday_start OR TIME(end_timestamp, 'Asia/Kolkata') > monday_end THEN
                  0  -- Response starts before or ends after working hours
                ELSE
                  -- Response is entirely within working hours
                  CAST(TIMESTAMP_DIFF(end_timestamp, start_timestamp, SECOND) AS FLOAT64)
              END
            WHEN EXTRACT(DAYOFWEEK FROM start_timestamp AT TIME ZONE 'Asia/Kolkata') = 3 AND tuesday_start IS NOT NULL AND tuesday_end IS NOT NULL AND NOT (tuesday_start = TIME(0,0,0) AND tuesday_end = TIME(0,0,0)) THEN
              CASE 
                WHEN TIME(start_timestamp, 'Asia/Kolkata') < tuesday_start OR TIME(end_timestamp, 'Asia/Kolkata') > tuesday_end THEN
                  0  -- Response starts before or ends after working hours
                ELSE
                  -- Response is entirely within working hours
                  CAST(TIMESTAMP_DIFF(end_timestamp, start_timestamp, SECOND) AS FLOAT64)
              END
            WHEN EXTRACT(DAYOFWEEK FROM start_timestamp AT TIME ZONE 'Asia/Kolkata') = 4 AND wednesday_start IS NOT NULL AND wednesday_end IS NOT NULL AND NOT (wednesday_start = TIME(0,0,0) AND wednesday_end = TIME(0,0,0)) THEN
              CASE 
                WHEN TIME(start_timestamp, 'Asia/Kolkata') < wednesday_start OR TIME(end_timestamp, 'Asia/Kolkata') > wednesday_end THEN
                  0  -- Response starts before or ends after working hours
                ELSE
                  -- Response is entirely within working hours
                  CAST(TIMESTAMP_DIFF(end_timestamp, start_timestamp, SECOND) AS FLOAT64)
              END
            WHEN EXTRACT(DAYOFWEEK FROM start_timestamp AT TIME ZONE 'Asia/Kolkata') = 5 AND thursday_start IS NOT NULL AND thursday_end IS NOT NULL AND NOT (thursday_start = TIME(0,0,0) AND thursday_end = TIME(0,0,0)) THEN
              CASE 
                WHEN TIME(start_timestamp, 'Asia/Kolkata') < thursday_start OR TIME(end_timestamp, 'Asia/Kolkata') > thursday_end THEN
                  0  -- Response starts before or ends after working hours
                ELSE
                  -- Response is entirely within working hours
                  CAST(TIMESTAMP_DIFF(end_timestamp, start_timestamp, SECOND) AS FLOAT64)
              END
            WHEN EXTRACT(DAYOFWEEK FROM start_timestamp AT TIME ZONE 'Asia/Kolkata') = 6 AND friday_start IS NOT NULL AND friday_end IS NOT NULL AND NOT (friday_start = TIME(0,0,0) AND friday_end = TIME(0,0,0)) THEN
              CASE 
                WHEN TIME(start_timestamp, 'Asia/Kolkata') < friday_start OR TIME(end_timestamp, 'Asia/Kolkata') > friday_end THEN
                  0  -- Response starts before or ends after working hours
                ELSE
                  -- Response is entirely within working hours
                  CAST(TIMESTAMP_DIFF(end_timestamp, start_timestamp, SECOND) AS FLOAT64)
              END
            WHEN EXTRACT(DAYOFWEEK FROM start_timestamp AT TIME ZONE 'Asia/Kolkata') = 7 AND saturday_start IS NOT NULL AND saturday_end IS NOT NULL AND NOT (saturday_start = TIME(0,0,0) AND saturday_end = TIME(0,0,0)) THEN
              CASE 
                WHEN TIME(start_timestamp, 'Asia/Kolkata') < saturday_start OR TIME(end_timestamp, 'Asia/Kolkata') > saturday_end THEN
                  0  -- Response starts before or ends after working hours
                ELSE
                  -- Response is entirely within working hours
                  CAST(TIMESTAMP_DIFF(end_timestamp, start_timestamp, SECOND) AS FLOAT64)
              END
            WHEN EXTRACT(DAYOFWEEK FROM start_timestamp AT TIME ZONE 'Asia/Kolkata') = 1 AND sunday_start IS NOT NULL AND sunday_end IS NOT NULL AND NOT (sunday_start = TIME(0,0,0) AND sunday_end = TIME(0,0,0)) THEN
              CASE 
                WHEN TIME(start_timestamp, 'Asia/Kolkata') < sunday_start OR TIME(end_timestamp, 'Asia/Kolkata') > sunday_end THEN
                  0  -- Response starts before or ends after working hours
                ELSE
                  -- Response is entirely within working hours
                  CAST(TIMESTAMP_DIFF(end_timestamp, start_timestamp, SECOND) AS FLOAT64)
              END
            ELSE 0
          END
        ELSE
          -- Different days - for now, return 0 (can be enhanced later)
          0
      END
    )
  END
);
