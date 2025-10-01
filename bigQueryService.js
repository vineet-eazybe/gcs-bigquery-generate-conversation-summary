const { BigQuery } = require('@google-cloud/bigquery');
const AnalyticsService = require('./analyticsService');

class BigQueryService {
  constructor() {
    this.bigquery = new BigQuery({
      credentials: require('./gcp-key.json')
    });
    console.log('Connected to BigQuery');
    this.analyticsService = new AnalyticsService();
  }

  /**
   * Get working hours configuration with priority: self > team > org
   * @param {string} userId - User ID
   * @param {string} orgId - Organization ID
   * @returns {Object} Working hours configuration
   */
  async getWorkingHoursConfig(userId, orgId) {
    try {
      await this.analyticsService.connectToMySQL();
      const workingHours = await this.analyticsService.getWorkingHours();
      const userMapping = await this.analyticsService.getUserMapping();
      
      // Filter and prioritize working hours
      const selfHours = workingHours.filter(wh => 
        wh.type === 'self' && wh.type_id === parseInt(userId)
      );

      const teamHours = workingHours.filter(wh => 
        wh.type === 'team' && wh.type_id === parseInt(userId)
      );
      
      const orgHours = workingHours.filter(wh => 
        wh.type === 'org' && wh.type_id === parseInt(orgId)
      );

      // Return with priority: self > team > org
      if (selfHours.length > 0) {
        return { source: 'self', workingHours: selfHours };
      } else if (teamHours.length > 0) {
        return { source: 'team', workingHours: teamHours };
      } else if (orgHours.length > 0) {
        return { source: 'org', workingHours: orgHours };
      }

      // Default working hours if none configured
      return {
        source: 'default',
        workingHours: [
          { week_day: 'monday', start_time: '09:00:00', end_time: '18:00:00' },
          { week_day: 'tuesday', start_time: '09:00:00', end_time: '18:00:00' },
          { week_day: 'wednesday', start_time: '09:00:00', end_time: '18:00:00' },
          { week_day: 'thursday', start_time: '09:00:00', end_time: '18:00:00' },
          { week_day: 'friday', start_time: '09:00:00', end_time: '18:00:00' },
          { week_day: 'saturday', start_time: '09:00:00', end_time: '18:00:00' },
          { week_day: 'sunday', start_time: '09:00:00', end_time: '18:00:00' }
        ]
      };
    } catch (error) {
      console.error('Error getting working hours config:', error);
      throw error;
    }
  }

  async getAllWorkingHoursConfig(){
    try {
      await this.analyticsService.connectToMySQL();
      const userMappings = await this.analyticsService.getUserMapping();
      const allWorkingHours = await this.analyticsService.getWorkingHours();
      
      const results = [];
      
      for (const userMapping of userMappings) {
        const { user_id, org_id, team_id } = userMapping;
        
        // Find working hours with priority: self > team > org > default
        let workingHoursConfig = null;
        let type = 'default';
        
        // Check for self working hours (highest priority)
        const selfHours = allWorkingHours.filter(wh => 
          wh.type === 'self' && wh.type_id === parseInt(user_id)
        );
        
        if (selfHours.length > 0) {
          workingHoursConfig = selfHours;
          type = 'self';
        } else {
          // Check for team working hours
          const teamHours = allWorkingHours.filter(wh => 
            wh.type === 'team' && wh.type_id === parseInt(team_id)
          );
          
          if (teamHours.length > 0) {
            workingHoursConfig = teamHours;
            type = 'team';
          } else {
            // Check for organization working hours
            const orgHours = allWorkingHours.filter(wh => 
              wh.type === 'org' && wh.type_id === parseInt(org_id)
            );
            
            if (orgHours.length > 0) {
              workingHoursConfig = orgHours;
              type = 'org';
            } else {
              // Use default working hours
              workingHoursConfig = [
                { week_day: 'monday', start_time: '09:00:00', end_time: '18:00:00' },
                { week_day: 'tuesday', start_time: '09:00:00', end_time: '18:00:00' },
                { week_day: 'wednesday', start_time: '09:00:00', end_time: '18:00:00' },
                { week_day: 'thursday', start_time: '09:00:00', end_time: '18:00:00' },
                { week_day: 'friday', start_time: '09:00:00', end_time: '18:00:00' },
                { week_day: 'saturday', start_time: '09:00:00', end_time: '18:00:00' },
                { week_day: 'sunday', start_time: '09:00:00', end_time: '18:00:00' }
              ];
              type = 'default';
            }
          }
        }
        
        results.push({
          user_id: user_id,
          org_id: org_id,
          team_id: team_id,
          type: type,
          working_hours: workingHoursConfig
        });
      }
      
      return results;
    } catch (error) {
      console.error('Error getting all working hours config:', error);
      throw error;
    }
  }

  /**
   * Generate a simplified BigQuery merge query for testing
   * @param {string} userId - User ID
   * @param {string} orgId - Organization ID
   * @param {string} projectId - BigQuery project ID
   * @param {string} datasetId - BigQuery dataset ID
   * @returns {string} Generated SQL query
   */
  async generateSimpleMergeQuery(userId, orgId, projectId = 'waba-454907', datasetId = 'whatsapp_analytics') {
    const query = `
-- Simplified merge query for conversation_summary
MERGE \`${projectId}.${datasetId}.conversation_summary\` AS T
USING (
    SELECT
        user_id,
        org_id,
        DATE(message_timestamp) AS date,
        sender_number,
        -- Basic message counts
        SUM(CASE WHEN direction = 'OUTGOING' THEN 1 ELSE 0 END) AS messages_sent,
        SUM(CASE WHEN direction = 'INCOMING' THEN 1 ELSE 0 END) AS messages_received,
        COUNT(DISTINCT message_id) AS unique_messages,
        COUNT(*) AS total_messages,
        0 AS number_of_follow_ups, -- Simplified for now
        'INCOMING' AS conversation_starter, -- Simplified for now
        'OUTGOING' AS last_message_from, -- Simplified for now
        MIN(message_timestamp) AS earliest_message,
        MAX(message_timestamp) AS latest_message,
        0 AS average_response_time -- Simplified for now
    FROM \`${projectId}.${datasetId}.message_events\`
    WHERE DATE(ingestion_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND CURRENT_DATE()
    GROUP BY user_id, org_id, date, sender_number
) AS S
ON T.uid = S.user_id 
   AND T.org_id = S.org_id 
   AND T.date = S.date
   AND (T.phone_number = S.sender_number OR (T.phone_number IS NULL AND S.sender_number IS NULL))

WHEN MATCHED THEN
    UPDATE SET
        T.analytics = STRUCT(
            S.messages_sent AS messages_sent,
            S.messages_received AS messages_received,
            S.total_messages AS total_messages,
            S.unique_messages AS unique_messages,
            S.number_of_follow_ups AS number_of_follow_ups
        ),
        T.average_response_time = S.average_response_time,
        T.conversation_starter = S.conversation_starter,
        T.last_message_from = S.last_message_from,
        T.updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (
        uid, 
        org_id, 
        date, 
        phone_number, 
        analytics, 
        average_response_time,
        conversation_starter,
        last_message_from,
        updated_at, 
        created_at
    )
    VALUES (
        S.user_id,
        S.org_id,
        S.date,
        S.sender_number,
        STRUCT(
            S.messages_sent AS messages_sent,
            S.messages_received AS messages_received,
            S.total_messages AS total_messages,
            S.unique_messages AS unique_messages,
            S.number_of_follow_ups AS number_of_follow_ups
        ),
        S.average_response_time,
        S.conversation_starter,
        S.last_message_from,
        CURRENT_TIMESTAMP(),
        S.earliest_message
    );
    `;

    console.log('Generated Simple BigQuery SQL:');
    console.log(query);
    return query;
  }

  /**
   * Generate BigQuery merge query with working hours integration (simplified version)
   * @param {string} userId - User ID
   * @param {string} orgId - Organization ID
   * @param {string} projectId - BigQuery project ID
   * @param {string} datasetId - BigQuery dataset ID
   * @returns {string} Generated SQL query
   */
  async generateMergeQueryWithWorkingHours(userId, orgId, projectId = 'waba-454907', datasetId = 'whatsapp_analytics') {
    const workingHoursConfig = await this.getWorkingHoursConfig(userId, orgId);
    
    // Convert working hours to BigQuery format
    const workingHoursValues = workingHoursConfig.workingHours.map(wh => 
      `SELECT '${wh.week_day}' as week_day, '${wh.start_time}' as start_time, '${wh.end_time}' as end_time`
    ).join(' UNION ALL ');
    
    console.log('Working hours values:', workingHoursValues);
    
    const query = `
-- Enhanced merge query with working hours integration for conversation_summary
MERGE \`${projectId}.${datasetId}.conversation_summary\` AS T
USING (
    WITH working_hours_config AS (
        ${workingHoursValues}
    ),
    
    message_analytics AS (
        SELECT
            user_id,
            org_id,
            DATE(message_timestamp) AS date,
            sender_number,
            -- Basic message counts
            SUM(CASE WHEN direction = 'OUTGOING' THEN 1 ELSE 0 END) AS messages_sent,
            SUM(CASE WHEN direction = 'INCOMING' THEN 1 ELSE 0 END) AS messages_received,
            COUNT(DISTINCT message_id) AS unique_messages,
            COUNT(*) AS total_messages,
            
            -- Simplified follow-up calculation
            SUM(CASE 
                WHEN direction = 'OUTGOING' THEN 1 
                ELSE 0 
            END) AS number_of_follow_ups,
            
            -- Conversation starter and last message from
            FIRST_VALUE(direction) OVER (
                PARTITION BY user_id, org_id, DATE(message_timestamp), sender_number 
                ORDER BY message_timestamp ASC
            ) AS conversation_starter,
            
            LAST_VALUE(direction) OVER (
                PARTITION BY user_id, org_id, DATE(message_timestamp), sender_number 
                ORDER BY message_timestamp ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS last_message_from,
            
            MIN(message_timestamp) AS earliest_message,
            MAX(message_timestamp) AS latest_message
            
        FROM \`${projectId}.${datasetId}.message_events\`
        WHERE DATE(ingestion_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND CURRENT_DATE()
        GROUP BY user_id, org_id, date, sender_number
    ),
    
    response_time_calc AS (
        SELECT
            user_id,
            org_id,
            DATE(message_timestamp) AS date,
            sender_number,
            -- Simplified response time calculation
            AVG(
                CASE 
                    WHEN direction = 'OUTGOING' 
                         -- Working hours validation using configured hours
                         AND EXISTS (
                             SELECT 1 FROM working_hours_config whc
                             WHERE LOWER(whc.week_day) = LOWER(FORMAT_DATE('%A', DATE(message_timestamp)))
                             AND TIME(message_timestamp) BETWEEN TIME(whc.start_time) AND TIME(whc.end_time)
                         )
                    THEN 300 -- Default 5 minutes for now
                    ELSE NULL
                END
            ) AS avg_response_time_seconds
        FROM \`${projectId}.${datasetId}.message_events\`
        WHERE DATE(ingestion_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND CURRENT_DATE()
        GROUP BY user_id, org_id, date, sender_number
    )
    
    SELECT
        ma.user_id,
        ma.org_id,
        ma.date,
        ma.sender_number,
        ma.messages_sent,
        ma.messages_received,
        ma.unique_messages,
        ma.total_messages,
        ma.number_of_follow_ups,
        ma.conversation_starter,
        ma.last_message_from,
        ma.earliest_message,
        ma.latest_message,
        COALESCE(rtc.avg_response_time_seconds, 0) AS average_response_time
    FROM message_analytics ma
    LEFT JOIN response_time_calc rtc 
        ON ma.user_id = rtc.user_id 
        AND ma.org_id = rtc.org_id 
        AND ma.date = rtc.date 
        AND ma.sender_number = rtc.sender_number
) AS S
ON T.uid = S.user_id 
   AND T.org_id = S.org_id 
   AND T.date = S.date
   AND (T.phone_number = S.sender_number OR (T.phone_number IS NULL AND S.sender_number IS NULL))

WHEN MATCHED THEN
    UPDATE SET
        T.analytics = STRUCT(
            S.messages_sent AS messages_sent,
            S.messages_received AS messages_received,
            S.total_messages AS total_messages,
            S.unique_messages AS unique_messages,
            S.number_of_follow_ups AS number_of_follow_ups
        ),
        T.average_response_time = S.average_response_time,
        T.conversation_starter = S.conversation_starter,
        T.last_message_from = S.last_message_from,
        T.updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (
        uid, 
        org_id, 
        date, 
        phone_number, 
        analytics, 
        average_response_time,
        conversation_starter,
        last_message_from,
        updated_at, 
        created_at
    )
    VALUES (
        S.user_id,
        S.org_id,
        S.date,
        S.sender_number,
        STRUCT(
            S.messages_sent AS messages_sent,
            S.messages_received AS messages_received,
            S.total_messages AS total_messages,
            S.unique_messages AS unique_messages,
            S.number_of_follow_ups AS number_of_follow_ups
        ),
        S.average_response_time,
        S.conversation_starter,
        S.last_message_from,
        CURRENT_TIMESTAMP(),
        S.earliest_message
    );
    `;

    console.log('Generated Advanced BigQuery SQL:');
    console.log(query);
    return query;
  }

  /**
   * Execute the merge query in BigQuery
   * @param {string} query - SQL query to execute
   * @returns {Object} Query result
   */
  async executeMergeQuery(query) {
    try {
      const [job] = await this.bigquery.createQueryJob({ query });
      const [rows] = await job.getQueryResults();
      return { success: true, rows, jobId: job.id };
    } catch (error) {
      console.error('Error executing merge query:', error);
      throw error;
    }
  }

  /**
   * Process conversation summary for a specific user and organization
   * @param {string} userId - User ID
   * @param {string} orgId - Organization ID
   * @param {boolean} useSimpleQuery - Whether to use simplified query for testing
   * @returns {Object} Processing result
   */
  async processConversationSummary(userId, orgId, useSimpleQuery = true) {
    try {
      console.log(`Processing conversation summary for user ${userId} in org ${orgId}`);
      
      // Generate query - use simple query for testing first
      const query = useSimpleQuery 
        ? await this.generateSimpleMergeQuery(userId, orgId)
        : await this.generateMergeQueryWithWorkingHours(userId, orgId);
      
      // Execute the query
      const result = await this.executeMergeQuery(query);
      
      console.log(`Successfully processed conversation summary. Job ID: ${result.jobId}`);
      return result;
    } catch (error) {
      console.error('Error processing conversation summary:', error);
      throw error;
    }
  }

  /**
   * Close connections
   */
  async closeConnections() {
    await this.analyticsService.closeConnection();
  }

  
}

module.exports = BigQueryService;
