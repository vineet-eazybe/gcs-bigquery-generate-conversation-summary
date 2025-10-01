# BigQuery Conversation Summary with Working Hours Integration

This project provides a comprehensive solution for processing WhatsApp message analytics in BigQuery with working hours integration for accurate average response time calculations.

## Features

- **Working Hours Integration**: Supports self, team, and organization-level working hours with priority-based selection
- **Complete Analytics**: Calculates all conversation metrics including messages sent/received, follow-ups, and response times
- **BigQuery Integration**: Efficient merge operations for conversation summary data
- **MySQL Integration**: Fetches working hours configuration from MySQL database
- **RESTful API**: Easy-to-use endpoints for processing and configuration

## Schema Overview

### message_events Table
Contains raw WhatsApp message data with fields like:
- `event_id`, `message_id`, `chat_id`
- `user_id`, `org_id`, `sender_number`
- `message_timestamp`, `direction`, `message_text`
- `type`, `ack`, `file_url`, etc.

### conversation_summary Table
Contains aggregated analytics with:
- **Basic Fields**: `uid`, `org_id`, `date`, `phone_number`
- **Analytics Record**: `messages_sent`, `messages_received`, `total_messages`, `unique_messages`, `number_of_follow_ups`
- **Response Metrics**: `average_response_time` (in seconds)
- **Conversation Flow**: `conversation_starter`, `last_message_from`
- **Timestamps**: `created_at`, `updated_at`

### working_hours Table (MySQL)
Configuration table with:
- `type`: 'org', 'team', or 'self'
- `type_id`: ID of the organization, team, or user
- `start_time`, `end_time`: Working hours
- `week_day`: Days of the week (SET type)
- `timezone_offset`: Timezone configuration

## Average Response Time Calculation

The system calculates average response time following these rules:

### 1. Working Hours Priority
1. **Self** working hours (highest priority)
2. **Team** working hours (medium priority)
3. **Organization** working hours (lowest priority)
4. **Default**: 9 AM - 6 PM for all days if no configuration exists

### 2. Message Filtering Criteria
- Only messages sent **during working hours**
- Only **agent responses to client messages on the same day**
- Only the **first response** to a client message
- **Timezone settings** are applied to all calculations

### 3. Calculation Logic
```sql
-- Response time is calculated as:
TIMESTAMP_DIFF(response_message_timestamp, incoming_message_timestamp, SECOND)
```

### 4. Working Hours Validation
```sql
-- Messages must be within configured working hours
AND EXISTS (
    SELECT 1 FROM working_hours_config whc
    WHERE LOWER(whc.week_day) = LOWER(FORMAT_DATE('%A', DATE(message_timestamp)))
    AND TIME(message_timestamp) BETWEEN TIME(whc.start_time) AND TIME(whc.end_time)
)
```

## API Endpoints

### 1. Get Working Hours
```http
GET /working-hours
```
Returns all working hours configurations from MySQL.

### 2. Get Working Hours Configuration
```http
GET /working-hours-config/:userId/:orgId
```
Returns the working hours configuration for a specific user/org with priority applied.

### 3. Process Conversation Summary
```http
POST /process-conversation-summary
Content-Type: application/json

{
  "userId": "123",
  "orgId": "456"
}
```
Processes conversation summary data with working hours integration.

## Usage Examples

### Basic Usage
```javascript
const BigQueryService = require('./bigQueryService');
const bigQueryService = new BigQueryService();

// Process conversation summary for a user
const result = await bigQueryService.processConversationSummary('123', '456');
console.log('Job ID:', result.jobId);
```

### Get Working Hours Configuration
```javascript
const config = await bigQueryService.getWorkingHoursConfig('123', '456');
console.log('Using working hours from:', config.source);
console.log('Working hours:', config.workingHours);
```

### Execute Custom Query
```javascript
const query = await bigQueryService.generateMergeQueryWithWorkingHours('123', '456');
const result = await bigQueryService.executeMergeQuery(query);
```

## Environment Variables

Create a `.env` file with the following variables:

```env
# MySQL Configuration
MYSQL_HOST=your-mysql-host
MYSQL_USER=your-mysql-user
MYSQL_PASS=your-mysql-password
MYSQL_DB=your-mysql-database

# BigQuery Configuration
GOOGLE_APPLICATION_CREDENTIALS=./gcp-key.json

# Server Configuration
PORT=3000
```

## Installation

1. Install dependencies:
```bash
npm install
```

2. Set up environment variables (copy from `env.example`)

3. Configure Google Cloud credentials in `gcp-key.json`

4. Start the server:
```bash
npm start
```

## SQL Query Files

- `enhanced_merge_query.sql`: Basic enhanced merge query with all analytics fields
- `working_hours_merge_query.sql`: Advanced merge query with working hours integration
- `merge_query.sql`: Original basic merge query

## Key Improvements Over Original Query

1. **Complete Analytics Fields**: All fields from `conversation_summary` schema are now populated
2. **Working Hours Integration**: Response time calculations respect configured working hours
3. **Follow-up Messages**: Proper calculation of follow-up message counts
4. **Conversation Flow**: Tracks conversation starter and last message sender
5. **Priority-based Configuration**: Self > Team > Organization working hours priority
6. **Timezone Support**: Proper timezone handling for working hours validation

## Performance Considerations

- The query processes data for the last day by default
- Working hours validation is optimized with EXISTS clauses
- Response time calculations use efficient window functions
- Merge operations are atomic and handle both INSERT and UPDATE cases

## Error Handling

The system includes comprehensive error handling for:
- MySQL connection issues
- BigQuery authentication problems
- Invalid user/org IDs
- Missing working hours configurations
- Query execution failures

## Monitoring

- All operations are logged with timestamps
- BigQuery job IDs are returned for tracking
- Error details are included in API responses
- Graceful shutdown handling for connections
