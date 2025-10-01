const express = require('express');
const AnalyticsService = require('./analyticsService');
require('dotenv').config();
const app = express();
const analyticsService = new AnalyticsService();

// Middleware
app.use(express.json());

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

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down gracefully...');
  await analyticsService.closeConnection();
  process.exit(0);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
