const { Pool } = require('pg')
const client = new Pool({
  name:'service_payment',
  user: 'h2hadm',
  host: '10.10.16.58',
  database: 'saph2h',
  password: 'susujahe',
  port: 5090,
  idleTimeoutMillis: 600000,
  connectionTimeoutMillis: 60000,
});

module.exports = client;