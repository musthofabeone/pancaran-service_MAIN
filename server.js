// LIB
const port = process.env.PORT || 3000;
const express = require('express');
const bodyparser = require('body-parser');
const nodeFetch = require('node-fetch')
const fetch = require('fetch-cookie')(nodeFetch);
const routes = require('./route');
const app = express();
const server = app.listen(3000);
server.keepAliveTimeout = 61 * 1000;
const morgan = require('morgan')
const logger = require('./logger');
const pjson = require("./package.json");
const basicAuth = require('express-basic-auth');

server.keepAliveTimeout = 61 * 1000;
// SSL
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0

const https = require('http');
const fs = require('fs');

app.use(morgan('combined'));

const users = {
    'SERVICE': 'Serv1ceH2hPancar@n'
  };

app.use(basicAuth({
    users: users,
    unauthorizedResponse: (req) => {
        logger.error('Unauthorized')
      return 'Unauthorized';
    }
  }));
  
  // Protected route
  app.get('/protected', (req, res) => {
    
    res.send('Authenticated');
  });

// ROUTES
app.use(bodyparser.json());
app.use((req, res, next) => {
    res.set('Access-Control-Allow-Origin', '*')
    res.header("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type")
    next()
})
routes(app);

//app.listen(port);
logger.info(`Payment -> Mobopay running → PORT ${server.address().port}`);

// https.createServer({
//     key: fs.readFileSync('pancaran-payment-gateway.key'),
//     cert: fs.readFileSync('pancaran-payment-gateway.crt')
//   }, app).listen(port, () => {
//      console.log("Run in port " + port);
//      console.log("Run in " + ENV + " mode");
//   });
  
