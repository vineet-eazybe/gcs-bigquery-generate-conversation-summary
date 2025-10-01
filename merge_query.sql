MERGE `waba-454907.whatsapp_analytics.conversation_summary` AS T
USING (
    SELECT
        user_id,
        org_id,
        DATE(message_timestamp) AS date,
        sender_number,
        SUM(CASE WHEN direction = 'OUTGOING' THEN 1 ELSE 0 END) AS total_sent,
        SUM(CASE WHEN direction = 'INCOMING' THEN 1 ELSE 0 END) AS total_received,
        COUNT(DISTINCT message_id) AS unique_messages
    FROM
        `waba-454907.whatsapp_analytics.message_events`
    WHERE
        DATE(ingestion_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND CURRENT_DATE()
    GROUP BY
        user_id,
        org_id,
        date,
        sender_number
) AS S
ON T.uid = S.user_id AND T.org_id = S.org_id AND T.date = S.date
   AND (T.phone_number = S.sender_number OR (T.phone_number IS NULL AND S.sender_number IS NULL))
WHEN MATCHED THEN
    UPDATE SET
        T.analytics = STRUCT(
            S.total_sent AS total_sent,
            S.total_received AS total_received,
            S.unique_messages AS unique_messages
        ),
        T.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (uid, org_id, date, phone_number, analytics, updated_at, created_at)
    VALUES (
        S.user_id,
        S.org_id,
        S.date,
        S.sender_number,
        STRUCT(S.total_sent, S.total_received, S.unique_messages),
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    );

