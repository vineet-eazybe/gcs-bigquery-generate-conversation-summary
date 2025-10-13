const mysql = require('mysql2/promise');
const { BigQuery } = require('@google-cloud/bigquery');

class AnalyticsService {
  constructor() {
    this.mysqlConn = null;
    this.bigquery = new BigQuery({
      credentials: require('./gcp-key.json')
    });
    console.log('Connected to BigQuery (AnalyticsService)');
  }

  async connectToMySQL() {
    try {
      this.mysqlConn = await mysql.createConnection({
        host: process.env.MYSQL_HOST,
        user: process.env.MYSQL_USER,
        password: process.env.MYSQL_PASS,
        database: process.env.MYSQL_DB
      });
      console.log('Connected to MySQL database');
    } catch (error) {
      console.error('Error connecting to MySQL:', error);
      throw error;
    }
  }

  // get user mapping from mysql for an array of user ids and org ids
  async getUserMapping() {
    try {
      if (!this.mysqlConn) {
        await this.connectToMySQL();
      }
      
      const query = `SELECT user_id, team_id, org_id FROM callyzer_user_mappings`;
      const [rows] = await this.mysqlConn.execute(query);
      return rows;
    } catch (error) {
      console.error('Error fetching user mapping:', error);
      throw error;
    }
  }

  async getWorkingHours() {
    try {
      if (!this.mysqlConn) {
        await this.connectToMySQL();
      }
      
      const [workingHours] = await this.mysqlConn.execute("SELECT * FROM working_hours");
      return workingHours;
    } catch (error) {
      console.error('Error fetching working hours:', error);
      throw error;
    }
  }


  /**
   * Get all working hours configuration with priority: self > team > org
   * @returns {Array} Array of working hours configurations for all users
   */
  async getAllWorkingHoursConfig(){
    try {
      await this.connectToMySQL();
      const userMappings = await this.getUserMapping();
      const allWorkingHours = await this.getWorkingHours();
      
      const results = [];
      const processedUsers = new Set(); // Track processed users to avoid duplicates
      
      for (const userMapping of userMappings) {
        const { user_id, org_id, team_id } = userMapping;
        
        // Skip if we've already processed this user
        if (processedUsers.has(user_id)) {
          continue;
        }
        
        // Mark user as processed
        processedUsers.add(user_id);
        
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
                { week_day: 'monday', start_time: '09:00:00', end_time: '18:00:00', timezone_offset: 0 },
                { week_day: 'tuesday', start_time: '09:00:00', end_time: '18:00:00', timezone_offset: 0 },
                { week_day: 'wednesday', start_time: '09:00:00', end_time: '18:00:00', timezone_offset: 0 },
                { week_day: 'thursday', start_time: '09:00:00', end_time: '18:00:00', timezone_offset: 0 },
                { week_day: 'friday', start_time: '09:00:00', end_time: '18:00:00', timezone_offset: 0 },
                { week_day: 'saturday', start_time: '09:00:00', end_time: '18:00:00', timezone_offset: 0 },
                { week_day: 'sunday', start_time: '09:00:00', end_time: '18:00:00', timezone_offset: 0 }
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
   * Transform working hours data into a format suitable for BigQuery
   * @param {Array} data - Array of user working hours data
   * @returns {Array} Transformed configuration array
   */
  prepareWorkingHoursConfig(data) {
    const configs = [];
    
    for (const user of data) {
      for (const wh of user.working_hours) {
        configs.push({
          user_id: String(user.user_id),
          org_id: String(user.org_id),
          week_day: wh.week_day.toLowerCase(),
          start_time: wh.start_time,
          end_time: wh.end_time
        });
      }
    }
    
    return configs;
  }

  async getMessageEvents() {
    try {
    const query = `
      SELECT * FROM \`waba-454907.whatsapp_analytics.message_events\`
      WHERE DATE(ingestion_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND CURRENT_DATE() AND user_id = "1145566"
      ORDER BY user_id, org_id, chat_id, message_timestamp ASC
    `;
    
    const [job] = await this.bigquery.createQueryJob({ query });
    const [rows] = await job.getQueryResults();
      return rows;
    } catch (error) {
      console.error('Error fetching message events:', error);
      throw error;
    }
  }

  /**
   * Generate response time data based on working hours configuration
   * @param {Array} workingHoursConfig - Working hours configuration array
   * @param {string} projectId - BigQuery project ID
   * @param {string} datasetId - BigQuery dataset ID
   * @returns {Array} Array of objects with user_id, org_id, phone_number, date, avg_response_time
   */
  async generateResponseTimeData(workingHoursConfig, projectId = 'waba-454907', datasetId = 'whatsapp_analytics') {
    try {
      console.log('Generating response time data based on working hours...');
      
      // Simple query to get all messages from the past 2 days
      const query = `
        SELECT
          user_id,
          org_id,
          chat_id,
          sender_number,
          direction,
          message_timestamp,
          DATE(message_timestamp) AS date
        FROM \`${projectId}.${datasetId}.message_events\`
        WHERE DATE(ingestion_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND CURRENT_DATE() AND user_id = "1145566"
        ORDER BY user_id, org_id, chat_id, message_timestamp ASC
      `;

      console.log('Executing message events query...');
      const [job] = await this.bigquery.createQueryJob({ query });
      const [rows] = await job.getQueryResults();
      
      console.log(`Query executed successfully. Job ID: ${job.id}`);
      console.log(`Retrieved ${rows.length} message records`);
      
      // Process messages to calculate response times with working hours
      const responseTimeData = this.calculateResponseTimesWithWorkingHoursLogging(rows, '1145566', workingHoursConfig);
      
      console.log(`Generated ${responseTimeData.length} response time records`);
      return responseTimeData;
    } catch (error) {
      console.error('Error generating response time data:', error);
      throw error;
    }
  }

  /**
   * Calculate response times from message events data
   * @param {Array} messages - Array of message events from BigQuery
   * @returns {Array} Array of objects with user_id, org_id, phone_number, date, avg_response_time
   */
  calculateResponseTimes(messages) {
    const responseTimes = new Map();
    
    // Group messages by user, org, chat, and date
    const messageGroups = new Map();
    
    messages.forEach(msg => {
      // Extract date properly from BigQuery Date object
      const dateStr = msg.date.value ? msg.date.value : msg.date;
      const key = `${msg.user_id}_${msg.org_id}_${msg.chat_id}_${dateStr}`;
      if (!messageGroups.has(key)) {
        messageGroups.set(key, []);
      }
      messageGroups.get(key).push({
        sender_number: msg.sender_number,
        direction: msg.direction,
        message_timestamp: new Date(msg.message_timestamp.value),
        date: dateStr
      });
    });
    
    // Calculate response times for each group
    messageGroups.forEach((groupMessages, key) => {
      const [user_id, org_id, chat_id, date] = key.split('_');
      
      // Sort messages by timestamp
      groupMessages.sort((a, b) => a.message_timestamp - b.message_timestamp);
      
      const responseTimesForGroup = [];
      
      for (let i = 0; i < groupMessages.length; i++) {
        const currentMsg = groupMessages[i];
        
        // Look for outgoing messages (responses)
        if (currentMsg.direction === 'OUTGOING') {
          // Find the most recent incoming message before this outgoing message
          for (let j = i - 1; j >= 0; j--) {
            const prevMsg = groupMessages[j];
            if (prevMsg.direction === 'INCOMING') {
              // Check if there are any outgoing messages between prevMsg and currentMsg
              let hasOutgoingBetween = false;
              for (let k = j + 1; k < i; k++) {
                if (groupMessages[k].direction === 'OUTGOING') {
                  hasOutgoingBetween = true;
                  break;
                }
              }
              
              if (!hasOutgoingBetween) {
                // Calculate response time in seconds
                const responseTimeSeconds = (currentMsg.message_timestamp - prevMsg.message_timestamp) / 1000;
                responseTimesForGroup.push({
                  user_id: parseInt(user_id),
                  org_id: parseInt(org_id),
                  phone_number: currentMsg.sender_number,
                  date: groupMessages[0].date, // Use the date from the message object
                  response_time: responseTimeSeconds
                });
                break;
              }
            }
          }
        }
      }
      
      // Calculate average response time for this group
      if (responseTimesForGroup.length > 0) {
        const avgResponseTime = responseTimesForGroup.reduce((sum, rt) => sum + rt.response_time, 0) / responseTimesForGroup.length;
        
        const resultKey = `${user_id}_${org_id}_${responseTimesForGroup[0].phone_number}_${responseTimesForGroup[0].date}`;
        responseTimes.set(resultKey, {
          user_id: parseInt(user_id),
          org_id: parseInt(org_id),
          phone_number: responseTimesForGroup[0].phone_number,
          date: responseTimesForGroup[0].date,
          avg_response_time: Math.round(avgResponseTime)
        });
      }
    });
    
    return Array.from(responseTimes.values());
  }

  /**
   * Generate response time data with working hours filtering
   * @param {Array} workingHoursConfig - Working hours configuration array
   * @param {string} projectId - BigQuery project ID
   * @param {string} datasetId - BigQuery dataset ID
   * @returns {Array} Array of objects with user_id, org_id, phone_number, date, avg_response_time
   */
  async generateResponseTimeDataWithWorkingHours(workingHoursConfig, projectId = 'waba-454907', datasetId = 'whatsapp_analytics') {
    try {
      console.log('Generating response time data with working hours filtering...');
      
      // Get response time data with working hours already applied
      const responseTimeData = await this.generateResponseTimeData(workingHoursConfig, projectId, datasetId);
      
      console.log(`Generated ${responseTimeData.length} response time records with working hours applied`);
      return responseTimeData;
    } catch (error) {
      console.error('Error generating response time data with working hours:', error);
      throw error;
    }
  }

  /**
   * Test endpoint: Calculate average response time for specific user (14024) for last two dates WITH working hours
   * @param {string} userId - User ID to test (default: '14024')
   * @param {string} projectId - BigQuery project ID
   * @param {string} datasetId - BigQuery dataset ID
   * @returns {Object} Detailed test results with logging
   */
  async testUserResponseTime(userId = '14024', projectId = 'waba-454907', datasetId = 'whatsapp_analytics') {
    try {
      console.log(`\n=== TESTING RESPONSE TIME CALCULATION FOR USER ${userId} WITH WORKING HOURS ===`);
      
      // First, get working hours configuration for this user
      console.log(`🔍 Getting working hours configuration for user ${userId}...`);
      const allWorkingHoursData = await this.getAllWorkingHoursConfig();
      const workingHoursConfig = this.prepareWorkingHoursConfig(allWorkingHoursData);
      
      // Filter working hours config for this specific user
      const userWorkingHoursConfig = workingHoursConfig.filter(config => config.user_id === userId);
      console.log(`📋 Found ${userWorkingHoursConfig.length} working hours entries for user ${userId}`);
      console.log(`📋 Total working hours config entries: ${workingHoursConfig.length}`);
      console.log(`📋 Sample config entries:`, workingHoursConfig.slice(0, 3).map(c => `${c.user_id}_${c.org_id}: ${c.week_day} ${c.start_time}-${c.end_time}`));
      
      if (userWorkingHoursConfig.length > 0) {
        console.log(`⏰ Working hours configuration for user ${userId}:`);
        userWorkingHoursConfig.forEach(wh => {
          console.log(`   ${wh.week_day}: ${wh.start_time} - ${wh.end_time}`);
        });
      } else {
        console.log(`⚠️  No working hours configuration found for user ${userId}, using default hours`);
        console.log(`⚠️  Available user IDs in config:`, [...new Set(workingHoursConfig.map(c => c.user_id))].slice(0, 10));
      }
      
      // Query for specific user and last two dates
      const query = `
        SELECT
          user_id,
          org_id,
          chat_id,
          sender_number,
          direction,
          message_timestamp,
          DATE(message_timestamp) AS date
        FROM \`${projectId}.${datasetId}.message_events\`
        WHERE CAST(user_id AS STRING) = '${userId}'
          AND DATE(ingestion_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND CURRENT_DATE()
        ORDER BY user_id, org_id, chat_id, message_timestamp ASC
      `;

      console.log(`📊 Executing test query for user ${userId}...`);
      console.log(`Query: ${query.replace(/\s+/g, ' ').trim()}`);
      
      const [job] = await this.bigquery.createQueryJob({ query });
      const [rows] = await job.getQueryResults();
      
      console.log(`✅ Query executed successfully. Job ID: ${job.id}`);
      console.log(`📈 Retrieved ${rows.length} message records for user ${userId}`);
      
      if (rows.length === 0) {
        console.log(`⚠️  No messages found for user ${userId} in the last 2 days`);
        return {
          success: true,
          userId: userId,
          messageCount: 0,
          responseTimeData: [],
          workingHoursConfig: userWorkingHoursConfig,
          summary: 'No messages found for this user in the specified time period'
        };
      }

      // Log message distribution
      const messageStats = this.logMessageStatistics(rows, userId);
      
      // Process messages to calculate response times with detailed logging AND working hours filtering
      const responseTimeData = this.calculateResponseTimesWithWorkingHoursLogging(rows, userId, userWorkingHoursConfig);
      
      // Calculate summary statistics
      const summary = this.calculateSummaryStatistics(responseTimeData, userId);
      
      console.log(`\n=== TEST COMPLETED FOR USER ${userId} WITH WORKING HOURS ===`);
      console.log(`📊 Total response time records: ${responseTimeData.length}`);
      console.log(`📅 Date range: ${summary.dateRange}`);
      console.log(`⏱️  Average response time: ${summary.avgResponseTime} seconds`);
      console.log(`📱 Phone numbers analyzed: ${summary.phoneNumbers.length}`);
      console.log(`⏰ Working hours applied: ${userWorkingHoursConfig.length > 0 ? 'Yes' : 'No (using default)'}`);
      
      return {
        success: true,
        userId: userId,
        messageCount: rows.length,
        messageStats: messageStats,
        workingHoursConfig: userWorkingHoursConfig,
        responseTimeData: responseTimeData,
        summary: summary
      };
    } catch (error) {
      console.error(`❌ Error testing response time for user ${userId}:`, error);
      throw error;
    }
  }

  /**
   * Log message statistics for debugging
   * @param {Array} messages - Array of message events
   * @param {string} userId - User ID being tested
   * @returns {Object} Message statistics
   */
  logMessageStatistics(messages, userId) {
    console.log(`\n📊 MESSAGE STATISTICS FOR USER ${userId}:`);
    
    const stats = {
      totalMessages: messages.length,
      incomingMessages: 0,
      outgoingMessages: 0,
      uniqueChats: new Set(),
      uniquePhoneNumbers: new Set(),
      dateDistribution: new Map(),
      directionDistribution: new Map()
    };

    messages.forEach(msg => {
      // Count directions
      if (msg.direction === 'INCOMING') {
        stats.incomingMessages++;
      } else if (msg.direction === 'OUTGOING') {
        stats.outgoingMessages++;
      }
      
      // Track unique values
      stats.uniqueChats.add(msg.chat_id);
      stats.uniquePhoneNumbers.add(msg.sender_number);
      
      // Date distribution
      const dateStr = msg.date.value ? msg.date.value : msg.date;
      stats.dateDistribution.set(dateStr, (stats.dateDistribution.get(dateStr) || 0) + 1);
      
      // Direction distribution
      stats.directionDistribution.set(msg.direction, (stats.directionDistribution.get(msg.direction) || 0) + 1);
    });

    console.log(`   📨 Total messages: ${stats.totalMessages}`);
    console.log(`   📥 Incoming messages: ${stats.incomingMessages}`);
    console.log(`   📤 Outgoing messages: ${stats.outgoingMessages}`);
    console.log(`   💬 Unique chats: ${stats.uniqueChats.size}`);
    console.log(`   📱 Unique phone numbers: ${stats.uniquePhoneNumbers.size}`);
    
    console.log(`   📅 Date distribution:`);
    for (const [date, count] of stats.dateDistribution) {
      console.log(`      ${date}: ${count} messages`);
    }
    
    console.log(`   🔄 Direction distribution:`);
    for (const [direction, count] of stats.directionDistribution) {
      console.log(`      ${direction}: ${count} messages`);
    }

    return {
      ...stats,
      uniqueChats: Array.from(stats.uniqueChats),
      uniquePhoneNumbers: Array.from(stats.uniquePhoneNumbers),
      dateDistribution: Object.fromEntries(stats.dateDistribution),
      directionDistribution: Object.fromEntries(stats.directionDistribution)
    };
  }

  /**
   * Calculate response times with detailed logging AND working hours filtering
   * @param {Array} messages - Array of message events from BigQuery
   * @param {string} userId - User ID being tested
   * @param {Array} workingHoursConfig - Working hours configuration for the user
   * @returns {Array} Array of objects with user_id, org_id, phone_number, date, avg_response_time
   */
  calculateResponseTimesWithWorkingHoursLogging(messages, userId, workingHoursConfig) {
    console.log(`\n🔄 CALCULATING RESPONSE TIMES FOR USER ${userId} WITH WORKING HOURS FILTERING:`);
    
    const responseTimes = new Map();
    
    // Create working hours lookup map
    const workingHoursMap = new Map();
    workingHoursConfig.forEach(config => {
      const key = `${config.user_id}_${config.org_id}`;
      if (!workingHoursMap.has(key)) {
        workingHoursMap.set(key, []);
      }
      workingHoursMap.get(key).push({
        week_day: config.week_day,
        start_time: config.start_time,
        end_time: config.end_time
      });
    });
    
    console.log(`   ⏰ Working hours map created with ${workingHoursMap.size} entries`);
    console.log(`   ⏰ Map keys:`, Array.from(workingHoursMap.keys()).slice(0, 5));
    console.log(`   ⏰ User 14024 keys:`, Array.from(workingHoursMap.keys()).filter(k => k.includes('14024')));
    
    // Group messages by user, org, chat, and date
    const messageGroups = new Map();
    
    messages.forEach(msg => {
      // Extract date properly from BigQuery Date object
      const dateStr = msg.date.value ? msg.date.value : msg.date;
      const key = `${msg.user_id}_${msg.org_id}_${msg.chat_id}_${dateStr}`;
      if (!messageGroups.has(key)) {
        messageGroups.set(key, []);
      }
      messageGroups.get(key).push({
        sender_number: msg.sender_number,
        direction: msg.direction,
        message_timestamp: new Date(msg.message_timestamp.value),
        date: dateStr
      });
    });
    
    console.log(`   📊 Grouped messages into ${messageGroups.size} conversation groups`);
    
    // Calculate response times for each group
    let totalResponsePairs = 0;
    let workingHoursFilteredPairs = 0;
    messageGroups.forEach((groupMessages, key) => {
      const [user_id, org_id, chat_id, date] = key.split('_');
      
      console.log(`\n   💬 Processing chat ${chat_id} on ${date}:`);
      console.log(`      📱 Phone: ${groupMessages[0].sender_number}`);
      console.log(`      📨 Total messages in group: ${groupMessages.length}`);
      
      // Sort messages by timestamp
      groupMessages.sort((a, b) => a.message_timestamp - b.message_timestamp);
      
      const responseTimesForGroup = [];
      
      for (let i = 0; i < groupMessages.length; i++) {
        const currentMsg = groupMessages[i];
        
        // Look for outgoing messages (responses)
        if (currentMsg.direction === 'OUTGOING') {
          console.log(`      📤 Found outgoing message at position ${i}`);
          
          // Find the most recent incoming message before this outgoing message
          for (let j = i - 1; j >= 0; j--) {
            const prevMsg = groupMessages[j];
            if (prevMsg.direction === 'INCOMING') {
              console.log(`         📥 Found incoming message at position ${j}`);
              
              // Check if there are any outgoing messages between prevMsg and currentMsg
              let hasOutgoingBetween = false;
              for (let k = j + 1; k < i; k++) {
                if (groupMessages[k].direction === 'OUTGOING') {
                  hasOutgoingBetween = true;
                  console.log(`         ⚠️  Skipping - outgoing message found between at position ${k}`);
                  break;
                }
              }
              
              if (!hasOutgoingBetween) {
                // Calculate response time in seconds
                const responseTimeSeconds = (currentMsg.message_timestamp - prevMsg.message_timestamp) / 1000;
                console.log(`         ✅ Valid response pair found!`);
                console.log(`            📥 Incoming: ${prevMsg.message_timestamp.toISOString()}`);
                console.log(`            📤 Outgoing: ${currentMsg.message_timestamp.toISOString()}`);
                console.log(`            ⏱️  Raw response time: ${responseTimeSeconds.toFixed(2)} seconds`);
                
                // Apply working hours filtering
                const workingHoursKey = `${String(user_id)}_${String(org_id)}`;
                const userWorkingHours = workingHoursMap.get(workingHoursKey);
                
                console.log(`            ⏰ Looking up working hours for key: ${workingHoursKey}`);
                console.log(`            ⏰ Found working hours:`, userWorkingHours ? userWorkingHours.length : 'null');
                
                if (userWorkingHours && userWorkingHours.length > 0) {
                  const adjustedResponseTime = this.adjustResponseTimeForWorkingHours(
                    responseTimeSeconds, 
                    prevMsg.message_timestamp, 
                    currentMsg.message_timestamp, 
                    userWorkingHours,
                    userId
                  );
                  
                  if (adjustedResponseTime !== null) {
                    console.log(`            ⏰ Working hours adjusted response time: ${adjustedResponseTime.toFixed(2)} seconds`);
                    
                    responseTimesForGroup.push({
                      user_id: parseInt(user_id),
                      org_id: parseInt(org_id),
                      phone_number: currentMsg.sender_number,
                      date: groupMessages[0].date,
                      response_time: adjustedResponseTime,
                      raw_response_time: responseTimeSeconds
                    });
                    workingHoursFilteredPairs++;
                  } else {
                    console.log(`            ⏰ Response time filtered out by working hours`);
                  }
                } else {
                  console.log(`            ⏰ No working hours config, using raw response time`);
                  responseTimesForGroup.push({
                    user_id: parseInt(user_id),
                    org_id: parseInt(org_id),
                    phone_number: currentMsg.sender_number,
                    date: groupMessages[0].date,
                    response_time: responseTimeSeconds,
                    raw_response_time: responseTimeSeconds
                  });
                }
                
                totalResponsePairs++;
                break;
              }
            }
          }
        }
      }
      
      // Calculate average response time for this group
      if (responseTimesForGroup.length > 0) {
        const avgResponseTime = responseTimesForGroup.reduce((sum, rt) => sum + rt.response_time, 0) / responseTimesForGroup.length;
        console.log(`      📊 Group summary: ${responseTimesForGroup.length} response pairs, avg: ${avgResponseTime.toFixed(2)}s`);
        
        const resultKey = `${user_id}_${org_id}_${responseTimesForGroup[0].phone_number}_${responseTimesForGroup[0].date}`;
        responseTimes.set(resultKey, {
          user_id: parseInt(user_id),
          org_id: parseInt(org_id),
          phone_number: responseTimesForGroup[0].phone_number,
          date: responseTimesForGroup[0].date,
          avg_response_time: Math.round(avgResponseTime)
        });
      } else {
        console.log(`      ⚠️  No valid response pairs found in this group`);
      }
    });
    
    console.log(`\n   📊 RESPONSE TIME CALCULATION SUMMARY WITH WORKING HOURS:`);
    console.log(`      💬 Total conversation groups: ${messageGroups.size}`);
    console.log(`      ⏱️  Total response pairs found: ${totalResponsePairs}`);
    console.log(`      ⏰ Working hours filtered pairs: ${workingHoursFilteredPairs}`);
    console.log(`      📈 Unique response time records: ${responseTimes.size}`);
    
    return Array.from(responseTimes.values());
  }

  /**
   * Adjust response time based on working hours with next working period logic
   * @param {number} rawResponseTime - Raw response time in seconds
   * @param {Date} incomingTime - Incoming message timestamp
   * @param {Date} outgoingTime - Outgoing message timestamp
   * @param {Array} workingHours - Working hours configuration
   * @param {string} userId - User ID for logging
   * @returns {number|null} Adjusted response time or null if filtered out
   */
  adjustResponseTimeForWorkingHours(rawResponseTime, incomingTime, outgoingTime, workingHours, userId) {
    // Get the day of the week for the incoming message using UTC to avoid timezone issues
    const daysOfWeek = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
    const incomingDayOfWeek = daysOfWeek[incomingTime.getUTCDay()];
    const outgoingDayOfWeek = daysOfWeek[outgoingTime.getUTCDay()];
    
    console.log(`            ⏰ Checking working hours for ${incomingDayOfWeek} (${incomingTime.toISOString()})`);
    console.log(`            ⏰ Available working hours:`, workingHours.map(wh => `${wh.week_day}: ${wh.start_time}-${wh.end_time}`));
    
    // Find working hours for incoming day
    const incomingDayWorkingHours = workingHours.find(wh => wh.week_day === incomingDayOfWeek);
    
    if (!incomingDayWorkingHours) {
      console.log(`            ⏰ No working hours found for ${incomingDayOfWeek}`);
      
      // Check if agent also responded outside working hours
      const outgoingDayWorkingHours = workingHours.find(wh => wh.week_day === outgoingDayOfWeek);
      if (!outgoingDayWorkingHours) {
        console.log(`            ⏰ No working hours found for ${outgoingDayOfWeek} either, filtering out (both client and agent outside working hours)`);
        return null;
      } else {
        // Agent responded on a day with working hours, but client texted on a day without working hours
        // Check if agent replied within working hours of the outgoing day
        const [outgoingStartHour, outgoingStartMin] = outgoingDayWorkingHours.start_time.split(':').map(Number);
        const [outgoingEndHour, outgoingEndMin] = outgoingDayWorkingHours.end_time.split(':').map(Number);
        
        const outgoingWorkingStart = new Date(outgoingTime);
        outgoingWorkingStart.setUTCHours(outgoingStartHour, outgoingStartMin, 0, 0);
        
        const outgoingWorkingEnd = new Date(outgoingTime);
        outgoingWorkingEnd.setUTCHours(outgoingEndHour, outgoingEndMin, 0, 0);
        
        if (outgoingWorkingEnd <= outgoingWorkingStart) {
          outgoingWorkingEnd.setDate(outgoingWorkingEnd.getDate() + 1);
        }
        
        console.log(`            ⏰ Outgoing day working hours: ${outgoingWorkingStart.toISOString()} to ${outgoingWorkingEnd.toISOString()}`);
        
        if (outgoingTime >= outgoingWorkingStart && outgoingTime <= outgoingWorkingEnd) {
          // Calculate working hours for all days between incoming and outgoing
          let totalWorkingHours = 0;
          const daysOfWeek = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
          
          // Get the day after the incoming day
          const incomingDayIndex = incomingTime.getUTCDay();
          const outgoingDayIndex = outgoingTime.getUTCDay();
          
          // Calculate days between (including the outgoing day)
          let currentDayIndex = (incomingDayIndex + 1) % 7;
          while (currentDayIndex !== (outgoingDayIndex + 1) % 7) {
            const currentDayName = daysOfWeek[currentDayIndex];
            const currentDayWorkingHours = workingHours.find(wh => wh.week_day === currentDayName);
            
            if (currentDayWorkingHours) {
              const [startHour, startMin] = currentDayWorkingHours.start_time.split(':').map(Number);
              const [endHour, endMin] = currentDayWorkingHours.end_time.split(':').map(Number);
              
              // Calculate working hours for this day
              let dayWorkingHours = (endHour * 3600 + endMin * 60) - (startHour * 3600 + startMin * 60);
              
              // Handle overnight working hours (e.g., 22:00 to 06:00)
              if (dayWorkingHours < 0) {
                dayWorkingHours += 24 * 3600; // Add 24 hours
              }
              
              // For the outgoing day, only count up to the response time
              if (currentDayIndex === outgoingDayIndex) {
                const timeFromOutgoingDayStart = (outgoingTime.getTime() - outgoingWorkingStart.getTime()) / 1000;
                dayWorkingHours = timeFromOutgoingDayStart;
                console.log(`            ⏰ Working hours for ${currentDayName} (partial): ${dayWorkingHours.toFixed(2)} seconds (${(dayWorkingHours/3600).toFixed(1)} hours)`);
              } else {
                console.log(`            ⏰ Working hours for ${currentDayName}: ${dayWorkingHours} seconds (${(dayWorkingHours/3600).toFixed(1)} hours)`);
              }
              
              totalWorkingHours += dayWorkingHours;
            }
            
            currentDayIndex = (currentDayIndex + 1) % 7;
          }
          
          console.log(`            ✅ Agent replied within working hours of outgoing day, adjusted response time: ${totalWorkingHours.toFixed(2)} seconds (${(totalWorkingHours/3600).toFixed(1)} hours)`);
          return Math.max(0, totalWorkingHours);
        } else {
          console.log(`            ❌ Agent replied outside working hours of outgoing day, filtering out`);
          return null;
        }
      }
    }
    
    const startTime = incomingDayWorkingHours.start_time;
    const endTime = incomingDayWorkingHours.end_time;
    
    console.log(`            ⏰ Working hours for ${incomingDayOfWeek}: ${startTime} - ${endTime}`);
    
    // Parse working hours
    const [startHour, startMin] = startTime.split(':').map(Number);
    const [endHour, endMin] = endTime.split(':').map(Number);
    
    const workingStart = new Date(incomingTime);
    workingStart.setUTCHours(startHour, startMin, 0, 0);
    
    const workingEnd = new Date(incomingTime);
    workingEnd.setUTCHours(endHour, endMin, 0, 0);
    
    // If working hours span midnight, adjust end time
    if (workingEnd <= workingStart) {
      workingEnd.setDate(workingEnd.getDate() + 1);
    }
    
    console.log(`            ⏰ Working period: ${workingStart.toISOString()} to ${workingEnd.toISOString()}`);
    console.log(`            ⏰ Incoming time: ${incomingTime.toISOString()}`);
    console.log(`            ⏰ Outgoing time: ${outgoingTime.toISOString()}`);
    
    // STEP 1: Check if the entire response time falls within working hours
    if (incomingTime >= workingStart && outgoingTime <= workingEnd) {
      console.log(`            ✅ Response time fully within working hours`);
      return rawResponseTime;
    }
    
    // STEP 2: Check if client texted within working hours but agent replied in next working day (PRIORITY)
    if (incomingTime >= workingStart && incomingTime <= workingEnd) {
      console.log(`            ⏰ Client texted within working hours, checking if agent replied in next working day`);
      
      // Check if agent replied on a different day
      if (incomingDayOfWeek !== outgoingDayOfWeek) {
        console.log(`            ⏰ Agent replied on different day (${outgoingDayOfWeek}), checking for next working day`);
        
        // Check if agent replied within working hours of the outgoing day
        const outgoingDayWorkingHours = workingHours.find(wh => wh.week_day === outgoingDayOfWeek);
        
        if (outgoingDayWorkingHours) {
          const [outgoingStartHour, outgoingStartMin] = outgoingDayWorkingHours.start_time.split(':').map(Number);
          const [outgoingEndHour, outgoingEndMin] = outgoingDayWorkingHours.end_time.split(':').map(Number);
          
          const outgoingWorkingStart = new Date(outgoingTime);
          outgoingWorkingStart.setUTCHours(outgoingStartHour, outgoingStartMin, 0, 0);
          
          const outgoingWorkingEnd = new Date(outgoingTime);
          outgoingWorkingEnd.setUTCHours(outgoingEndHour, outgoingEndMin, 0, 0);
          
          if (outgoingWorkingEnd <= outgoingWorkingStart) {
            outgoingWorkingEnd.setDate(outgoingWorkingEnd.getDate() + 1);
          }
          
          console.log(`            ⏰ Outgoing day working hours: ${outgoingWorkingStart.toISOString()} to ${outgoingWorkingEnd.toISOString()}`);
          
          // Check if agent replied within working hours of the outgoing day
          if (outgoingTime >= outgoingWorkingStart && outgoingTime <= outgoingWorkingEnd) {
            // Calculate time remaining in current working day
            const timeRemainingInCurrentDay = (workingEnd.getTime() - incomingTime.getTime()) / 1000;
            
            // Calculate time from start of outgoing working day to agent response
            const timeFromOutgoingDayStart = (outgoingTime.getTime() - outgoingWorkingStart.getTime()) / 1000;
            
            // Calculate working hours for all days in between
            let totalWorkingHoursInBetween = 0;
            const daysOfWeek = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
            
            // Get the day after the incoming day
            const incomingDayIndex = incomingTime.getUTCDay();
            const outgoingDayIndex = outgoingTime.getUTCDay();
            
            // Calculate days between (excluding the incoming and outgoing days)
            let currentDayIndex = (incomingDayIndex + 1) % 7;
            while (currentDayIndex !== outgoingDayIndex) {
              const currentDayName = daysOfWeek[currentDayIndex];
              const currentDayWorkingHours = workingHours.find(wh => wh.week_day === currentDayName);
              
              if (currentDayWorkingHours) {
                const [startHour, startMin] = currentDayWorkingHours.start_time.split(':').map(Number);
                const [endHour, endMin] = currentDayWorkingHours.end_time.split(':').map(Number);
                
                // Calculate working hours for this day
                let dayWorkingHours = (endHour * 3600 + endMin * 60) - (startHour * 3600 + startMin * 60);
                
                // Handle overnight working hours (e.g., 22:00 to 06:00)
                if (dayWorkingHours < 0) {
                  dayWorkingHours += 24 * 3600; // Add 24 hours
                }
                
                totalWorkingHoursInBetween += dayWorkingHours;
                console.log(`            ⏰ Working hours for ${currentDayName}: ${dayWorkingHours} seconds (${(dayWorkingHours/3600).toFixed(1)} hours)`);
              }
              
              currentDayIndex = (currentDayIndex + 1) % 7;
            }
            
            // Total adjusted response time
            const adjustedResponseTime = timeRemainingInCurrentDay + totalWorkingHoursInBetween + timeFromOutgoingDayStart;
            
            console.log(`            ⏰ Time remaining in current day: ${timeRemainingInCurrentDay.toFixed(2)} seconds (${(timeRemainingInCurrentDay/3600).toFixed(1)} hours)`);
            console.log(`            ⏰ Working hours in between: ${totalWorkingHoursInBetween.toFixed(2)} seconds (${(totalWorkingHoursInBetween/3600).toFixed(1)} hours)`);
            console.log(`            ⏰ Time from outgoing day start: ${timeFromOutgoingDayStart.toFixed(2)} seconds (${(timeFromOutgoingDayStart/3600).toFixed(1)} hours)`);
            console.log(`            ✅ Agent replied within working hours of outgoing day, adjusted response time: ${adjustedResponseTime.toFixed(2)} seconds (${(adjustedResponseTime/3600).toFixed(1)} hours)`);
            return Math.max(0, adjustedResponseTime);
          } else {
            console.log(`            ❌ Agent replied outside working hours of outgoing day, filtering out`);
            return null;
          }
        } else {
          console.log(`            ❌ No working hours found for outgoing day (${outgoingDayOfWeek}), filtering out`);
          return null;
        }
      }
    }
    
    // STEP 3: Check if agent replied outside working hours (for same-day responses)
    const outgoingDayWorkingHours = workingHours.find(wh => wh.week_day === outgoingDayOfWeek);
    if (outgoingDayWorkingHours) {
      const [outStartHour, outStartMin] = outgoingDayWorkingHours.start_time.split(':').map(Number);
      const [outEndHour, outEndMin] = outgoingDayWorkingHours.end_time.split(':').map(Number);
      
      const outgoingWorkingStart = new Date(outgoingTime);
      outgoingWorkingStart.setUTCHours(outStartHour, outStartMin, 0, 0);
      
      const outgoingWorkingEnd = new Date(outgoingTime);
      outgoingWorkingEnd.setUTCHours(outEndHour, outEndMin, 0, 0);
      
      if (outgoingWorkingEnd <= outgoingWorkingStart) {
        outgoingWorkingEnd.setDate(outgoingWorkingEnd.getDate() + 1);
      }
      
      console.log(`            ⏰ Agent working hours check: ${outgoingWorkingStart.toISOString()} to ${outgoingWorkingEnd.toISOString()}`);
      console.log(`            ⏰ Agent replied at: ${outgoingTime.toISOString()}`);
      
      // If agent replied outside working hours, filter out
      if (outgoingTime < outgoingWorkingStart || outgoingTime > outgoingWorkingEnd) {
        console.log(`            ❌ Agent replied outside working hours (${outgoingTime.toISOString()}), filtering out`);
        return null;
      } else {
        console.log(`            ✅ Agent replied within working hours`);
      }
    }
    
    // STEP 4: Check if client texted outside working hours but agent replied within working hours of any future day
    if (incomingTime < workingStart || incomingTime > workingEnd) {
      console.log(`            ⏰ Client texted outside working hours, checking if agent replied within working hours of any future day`);
      
      // Check if agent replied within working hours of the outgoing day
      const outgoingDayWorkingHours = workingHours.find(wh => wh.week_day === outgoingDayOfWeek);
      
      if (outgoingDayWorkingHours) {
        const [outgoingStartHour, outgoingStartMin] = outgoingDayWorkingHours.start_time.split(':').map(Number);
        const [outgoingEndHour, outgoingEndMin] = outgoingDayWorkingHours.end_time.split(':').map(Number);
        
        const outgoingWorkingStart = new Date(outgoingTime);
        outgoingWorkingStart.setUTCHours(outgoingStartHour, outgoingStartMin, 0, 0);
        
        const outgoingWorkingEnd = new Date(outgoingTime);
        outgoingWorkingEnd.setUTCHours(outgoingEndHour, outgoingEndMin, 0, 0);
        
        if (outgoingWorkingEnd <= outgoingWorkingStart) {
          outgoingWorkingEnd.setDate(outgoingWorkingEnd.getDate() + 1);
        }
        
        console.log(`            ⏰ Outgoing day working hours: ${outgoingWorkingStart.toISOString()} to ${outgoingWorkingEnd.toISOString()}`);
        
        // Check if agent replied within working hours of the outgoing day
        if (outgoingTime >= outgoingWorkingStart && outgoingTime <= outgoingWorkingEnd) {
          // Calculate working hours for all days between incoming and outgoing
          let totalWorkingHours = 0;
          const daysOfWeek = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
          
          // Get the day after the incoming day
          const incomingDayIndex = incomingTime.getUTCDay();
          const outgoingDayIndex = outgoingTime.getUTCDay();
          
          // Calculate days between (including the outgoing day)
          let currentDayIndex = (incomingDayIndex + 1) % 7;
          while (currentDayIndex !== (outgoingDayIndex + 1) % 7) {
            const currentDayName = daysOfWeek[currentDayIndex];
            const currentDayWorkingHours = workingHours.find(wh => wh.week_day === currentDayName);
            
            if (currentDayWorkingHours) {
              const [startHour, startMin] = currentDayWorkingHours.start_time.split(':').map(Number);
              const [endHour, endMin] = currentDayWorkingHours.end_time.split(':').map(Number);
              
              // Calculate working hours for this day
              let dayWorkingHours = (endHour * 3600 + endMin * 60) - (startHour * 3600 + startMin * 60);
              
              // Handle overnight working hours (e.g., 22:00 to 06:00)
              if (dayWorkingHours < 0) {
                dayWorkingHours += 24 * 3600; // Add 24 hours
              }
              
              // For the outgoing day, only count up to the response time
              if (currentDayIndex === outgoingDayIndex) {
                const timeFromOutgoingDayStart = (outgoingTime.getTime() - outgoingWorkingStart.getTime()) / 1000;
                dayWorkingHours = timeFromOutgoingDayStart;
                console.log(`            ⏰ Working hours for ${currentDayName} (partial): ${dayWorkingHours.toFixed(2)} seconds (${(dayWorkingHours/3600).toFixed(1)} hours)`);
              } else {
                console.log(`            ⏰ Working hours for ${currentDayName}: ${dayWorkingHours} seconds (${(dayWorkingHours/3600).toFixed(1)} hours)`);
              }
              
              totalWorkingHours += dayWorkingHours;
            }
            
            currentDayIndex = (currentDayIndex + 1) % 7;
          }
          
          console.log(`            ✅ Agent replied within working hours of outgoing day, adjusted response time: ${totalWorkingHours.toFixed(2)} seconds (${(totalWorkingHours/3600).toFixed(1)} hours)`);
          return Math.max(0, totalWorkingHours); // Ensure non-negative
        } else {
          console.log(`            ❌ Agent replied outside working hours of outgoing day, filtering out`);
          return null;
        }
      } else {
        console.log(`            ❌ No working hours found for outgoing day (${outgoingDayOfWeek}), filtering out`);
        return null;
      }
    }
    
    // STEP 4: Handle other edge cases (response time spans working hours, partial overlaps, etc.)
    // Check if response time spans working hours
    if (incomingTime < workingStart && outgoingTime > workingEnd) {
      console.log(`            ⏰ Response time spans working hours, calculating working time portion`);
      
      // Calculate the portion of response time that falls within working hours
      const workingTimeStart = Math.max(incomingTime.getTime(), workingStart.getTime());
      const workingTimeEnd = Math.min(outgoingTime.getTime(), workingEnd.getTime());
      
      if (workingTimeStart < workingTimeEnd) {
        const workingTimeSeconds = (workingTimeEnd - workingTimeStart) / 1000;
        console.log(`            ⏰ Working time portion: ${workingTimeSeconds.toFixed(2)} seconds`);
        return workingTimeSeconds;
      } else {
        console.log(`            ⏰ No working time overlap, filtering out`);
        return null;
      }
    }
    
    // If response time is partially within working hours
    if (incomingTime < workingStart && outgoingTime >= workingStart && outgoingTime <= workingEnd) {
      const workingTimeSeconds = (outgoingTime.getTime() - workingStart.getTime()) / 1000;
      console.log(`            ⏰ Partial working time (after start): ${workingTimeSeconds.toFixed(2)} seconds`);
      return workingTimeSeconds;
    }
    
    if (incomingTime >= workingStart && incomingTime <= workingEnd && outgoingTime > workingEnd) {
      const workingTimeSeconds = (workingEnd.getTime() - incomingTime.getTime()) / 1000;
      console.log(`            ⏰ Partial working time (before end): ${workingTimeSeconds.toFixed(2)} seconds`);
      return workingTimeSeconds;
    }
    
    // Response time is completely outside working hours
    console.log(`            ⏰ Response time completely outside working hours, filtering out`);
    return null;
  }

  /**
   * Find the next working period after a given time
   * @param {Date} fromTime - Time to find next working period from
   * @param {Array} workingHours - Working hours configuration
   * @returns {Object|null} Next working period with start and end times
   */
  findNextWorkingPeriod(fromTime, workingHours) {
    const daysOfWeek = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
    const currentDayIndex = fromTime.getUTCDay();
    
    // Check current day first (if time is before working hours end)
    const currentDayWorkingHours = workingHours.find(wh => wh.week_day === daysOfWeek[currentDayIndex]);
    if (currentDayWorkingHours) {
      const [startHour, startMin] = currentDayWorkingHours.start_time.split(':').map(Number);
      const [endHour, endMin] = currentDayWorkingHours.end_time.split(':').map(Number);
      
      const workingStart = new Date(fromTime);
      workingStart.setUTCHours(startHour, startMin, 0, 0);
      
      const workingEnd = new Date(fromTime);
      workingEnd.setUTCHours(endHour, endMin, 0, 0);
      
      if (workingEnd <= workingStart) {
        workingEnd.setDate(workingEnd.getDate() + 1);
      }
      
      // If current time is before working hours end today, return today's working period
      if (fromTime < workingEnd) {
        return {
          start: workingStart,
          end: workingEnd,
          day: daysOfWeek[currentDayIndex]
        };
      }
    }
    
    // Check next 7 days for working hours
    for (let i = 1; i <= 7; i++) {
      const nextDayIndex = (currentDayIndex + i) % 7;
      const nextDayWorkingHours = workingHours.find(wh => wh.week_day === daysOfWeek[nextDayIndex]);
      
      if (nextDayWorkingHours) {
        const [startHour, startMin] = nextDayWorkingHours.start_time.split(':').map(Number);
        
        const nextWorkingStart = new Date(fromTime);
        nextWorkingStart.setDate(nextWorkingStart.getDate() + i);
        nextWorkingStart.setUTCHours(startHour, startMin, 0, 0);
        
        const nextWorkingEnd = new Date(nextWorkingStart);
        const [endHour, endMin] = nextDayWorkingHours.end_time.split(':').map(Number);
        nextWorkingEnd.setUTCHours(endHour, endMin, 0, 0);
        
        if (nextWorkingEnd <= nextWorkingStart) {
          nextWorkingEnd.setDate(nextWorkingEnd.getDate() + 1);
        }
        
        return {
          start: nextWorkingStart,
          end: nextWorkingEnd,
          day: daysOfWeek[nextDayIndex]
        };
      }
    }
    
    return null;
  }

  /**
   * Find the next working day after a given time
   * @param {Date} fromTime - Time to find next working day from
   * @param {Array} workingHours - Working hours configuration
   * @returns {Object|null} Next working day with start and end times
   */
  findNextWorkingDay(fromTime, workingHours) {
    const daysOfWeek = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
    const currentDayIndex = fromTime.getUTCDay();
    
    // Check next 7 days for working hours (skip current day)
    for (let i = 1; i <= 7; i++) {
      const nextDayIndex = (currentDayIndex + i) % 7;
      const nextDayWorkingHours = workingHours.find(wh => wh.week_day === daysOfWeek[nextDayIndex]);
      
      if (nextDayWorkingHours) {
        const [startHour, startMin] = nextDayWorkingHours.start_time.split(':').map(Number);
        
        const nextWorkingStart = new Date(fromTime);
        nextWorkingStart.setDate(nextWorkingStart.getDate() + i);
        nextWorkingStart.setUTCHours(startHour, startMin, 0, 0);
        
        const nextWorkingEnd = new Date(nextWorkingStart);
        const [endHour, endMin] = nextDayWorkingHours.end_time.split(':').map(Number);
        nextWorkingEnd.setUTCHours(endHour, endMin, 0, 0);
        
        if (nextWorkingEnd <= nextWorkingStart) {
          nextWorkingEnd.setDate(nextWorkingEnd.getDate() + 1);
        }
        
        return {
          start: nextWorkingStart,
          end: nextWorkingEnd,
          day: daysOfWeek[nextDayIndex]
        };
      }
    }
    
    return null;
  }

  /**
   * Calculate summary statistics for response time data
   * @param {Array} responseTimeData - Array of response time records
   * @param {string} userId - User ID being tested
   * @returns {Object} Summary statistics
   */
  calculateSummaryStatistics(responseTimeData, userId) {
    if (responseTimeData.length === 0) {
      return {
        avgResponseTime: 0,
        minResponseTime: 0,
        maxResponseTime: 0,
        totalRecords: 0,
        phoneNumbers: [],
        dateRange: 'No data'
      };
    }

    const responseTimes = responseTimeData.map(rt => rt.avg_response_time);
    const dates = [...new Set(responseTimeData.map(rt => rt.date))].sort();
    const phoneNumbers = [...new Set(responseTimeData.map(rt => rt.phone_number))];

    return {
      avgResponseTime: Math.round(responseTimes.reduce((sum, rt) => sum + rt, 0) / responseTimes.length),
      minResponseTime: Math.min(...responseTimes),
      maxResponseTime: Math.max(...responseTimes),
      totalRecords: responseTimeData.length,
      phoneNumbers: phoneNumbers,
      dateRange: `${dates[0]} to ${dates[dates.length - 1]}`
    };
  }

  async closeConnection() {
    if (this.mysqlConn) {
      await this.mysqlConn.end();
      console.log('MySQL connection closed');
    }
  }
}

module.exports = AnalyticsService;
