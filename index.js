const express = require('express');
const AnalyticsService = require('./analyticsService');
const BigQueryService = require('./bigQueryService');
require('dotenv').config();
const app = express();
const analyticsService = new AnalyticsService();
const bigQueryService = new BigQueryService();

// Middleware
app.use(express.json());

app.get('/user-mapping', async (req, res) => {
  const userMapping = await analyticsService.getUserMapping();
  res.json(userMapping);
});

// Route to get working hours
app.get('/working-hours', async (req, res) => {
  try {
    const workingHours = await analyticsService.getWorkingHours();
    res.json(workingHours);
  } catch (error) {
    console.error('Error in /working-hours route:', error);
    res.status(500).json({ error: 'Failed to fetch working hours' });
  }
});

// Route to process conversation summary with working hours
app.post('/process-conversation-summary', async (req, res) => {
  try {
    const { userId, orgId, useSimpleQuery = false } = req.body;
    
    if (!userId || !orgId) {
      return res.status(400).json({ 
        error: 'userId and orgId are required' 
      });
    }

    const result = await bigQueryService.processConversationSummary(userId, orgId, useSimpleQuery);
    res.json({
      success: true,
      message: 'Conversation summary processed successfully',
      jobId: result.jobId,
      rowsProcessed: result.rows.length,
      queryType: useSimpleQuery ? 'simple' : 'advanced'
    });
  } catch (error) {
    console.error('Error in /process-conversation-summary route:', error);
    res.status(500).json({ 
      error: 'Failed to process conversation summary',
      details: error.message 
    });
  }
});

// Route to get working hours configuration for a specific user/org
app.get('/working-hours-config/:userId/:orgId', async (req, res) => {
  try {
    const { userId, orgId } = req.params;
    const config = await bigQueryService.getWorkingHoursConfig(userId, orgId);
    res.json(config);
  } catch (error) {
    console.error('Error in /working-hours-config route:', error);
    res.status(500).json({ 
      error: 'Failed to fetch working hours configuration',
      details: error.message 
    });
  }
});

// Route to get all working hours configurations for all users
app.get('/all-working-hours-config', async (req, res) => {
  try {
    const configs = await bigQueryService.getAllWorkingHoursConfig();
    res.json({
      success: true,
      count: configs.length,
      data: configs.filter(config => config.user_id === 14024)
    });
  } catch (error) {
    console.error('Error in /all-working-hours-config route:', error);
    res.status(500).json({ 
      error: 'Failed to fetch all working hours configurations',
      details: error.message 
    });
  }
});

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down gracefully...');
  await analyticsService.closeConnection();
  await bigQueryService.closeConnections();
  process.exit(0);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
