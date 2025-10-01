const mysql = require('mysql2/promise');

class AnalyticsService {
  constructor() {
    this.mysqlConn = null;
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

  async closeConnection() {
    if (this.mysqlConn) {
      await this.mysqlConn.end();
      console.log('MySQL connection closed');
    }
  }
}

module.exports = AnalyticsService;
