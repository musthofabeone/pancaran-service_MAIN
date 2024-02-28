const nodeFetch = require('node-fetch')
const fetch = require('fetch-cookie')(nodeFetch);
const connection = require('./conn');
const base64 = require('base-64');
const cron = require('node-cron');
const crypto = require('crypto');
var https = require('follow-redirects').https;
var fs = require('fs');
const privateKey = fs.readFileSync('./pancaran-pg-sap-dev.key');
var jwt = require('jsonwebtoken');
var moment = require('moment'); // untuk definisi tanggal
const config = require('./config');
const { Pool } = require('pg') // untuk postgresql
var retry = require('promise-fn-retry'); // untik retry & interval
const { base64encode, base64decode } = require('nodejs-base64');
const fastJson = require('fast-json-stringify')
const { AbortController } = require('node-abort-controller');
const fetchRetry = require('node-fetch-retry-timeout');
const { resolve } = require('path');
const { match } = require('assert');
const logger = require('./logger');
const { job } = require('cron');
const { response } = require('express');


// create pool
const pool = new Pool({
  name: 'service_payment',
  user: 'h2hadm',
  host: '10.10.16.58',
  database: 'saph2h',
  password: 'susujahe',
  port: 5090,
});
// SSL (Only if needed)
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0


// PostgreSql Query Adapter
async function executeQuery(query, queryparams) {
  return new Promise((resolve, reject) => {
    connection.query(query, queryparams,
      function (error, rows, fields) {
        if (error) {
          logger.info(moment().format('YYYY-MM-DD h:mm:ss a') + ": ");
          logger.info("PostgreSql Query failed");
          logger.info(error);
          reject(error);
        } else {
          // logger.info(this.sql); // Debug
          resolve(rows);
        }
      });
  })
}


async function resendPaymet() {

  logger.info('Resend Paymet.............................');
  let result = await executeQuery('SELECT "REFERENCY", "TRANSFERTYPE", TO_CHAR("TRANSTIME", \'HHMI\') "TRANSTIME", "DB" FROM paymenth2h."BOS_TRANSACTIONS" ' +
    'WHERE "TRXID" NOT IN (SELECT "trxId" FROM paymenth2h."BOS_DO_STATUS") ' +
    'AND "TRXID" != \'\' AND "REFERENCY" =  \'OUT/20000412/101\'' +
    'ORDER BY "REFERENCY"')
  return result;

}

async function Mobopay() {
  //logger.info('Service payment host to host.............................');
  // scheduler untuk kirim ke mobopay yg belom pernah ke Interfacing

  let result = await executeQuery('SELECT T0.* FROM (SELECT ROW_NUMBER () OVER ( ' +
    'PARTITION BY X0."REFERENCY" ' +
    'ORDER BY X0."ACTION") "ROWNUMBER",  * FROM( ' +
    'SELECT  X0."REFERENCY", X0."TRANSFERTYPE", TO_CHAR(X0."TRANSTIME", \'HHMI\') "TRANSTIME"  ' +
    ', X0."DB", CASE WHEN X1."TRXID" = \'\' THEN \'PAYMENT\' ELSE \'RESEND_PAYMENT\' END AS "ACTION"  ' +
    ', "PAYMENTOUTTYPE", COALESCE(X1."TRXID" , \'\') "TRXID" ' +
    'FROM paymenth2h."BOS_TRANSACTIONS" X0  ' +
    'INNER JOIN (  ' +
    'SELECT "REFERENCY", COALESCE("TRXID", \'0\') "TRXID" FROM paymenth2h."BOS_LOG_TRANSACTIONS" ' +
    'GROUP BY "REFERENCY", "TRXID") X1 ON X0."REFERENCY" = X1."REFERENCY" ' +
    'WHERE X0."INTERFACING" IN (\'0\') ' +
    'GROUP BY X0."REFERENCY", X0."TRANSFERTYPE", X0."TRANSTIME", X0."DB", X0."ACTION", X0."PAYMENTOUTTYPE", X1."TRXID" ' +
    'ORDER BY X0."REFERENCY" ASC) X0) T0 ' +
    'INNER JOIN (SELECT MAX(X0."ROWNUMBER") "MAXNUMBER", X0."REFERENCY" FROM (SELECT ROW_NUMBER () OVER ( ' +
    'PARTITION BY  X0."REFERENCY" ' +
    'ORDER BY X0."ACTION") "ROWNUMBER",  * FROM( ' +
    'SELECT  X0."REFERENCY", X0."TRANSFERTYPE", TO_CHAR(X0."TRANSTIME", \'HHMI\') "TRANSTIME"  ' +
    ', X0."DB", CASE WHEN X1."TRXID" = \'\' THEN \'PAYMENT\' ELSE \'RESEND_PAYMENT\' END AS "ACTION"  ' +
    ', "PAYMENTOUTTYPE", COALESCE(X1."TRXID" , \'\') "TRXID" ' +
    'FROM paymenth2h."BOS_TRANSACTIONS" X0  ' +
    'INNER JOIN (  ' +
    'SELECT "REFERENCY", COALESCE("TRXID", \'0\') "TRXID" FROM paymenth2h."BOS_LOG_TRANSACTIONS" ' +
    'GROUP BY "REFERENCY", "TRXID") X1 ON X0."REFERENCY" = X1."REFERENCY" ' +
    'WHERE X0."INTERFACING" IN (\'0\') ' +
    'GROUP BY X0."REFERENCY", X0."TRANSFERTYPE", X0."TRANSTIME", X0."DB", X0."ACTION", X0."PAYMENTOUTTYPE", X1."TRXID" ' +
    'ORDER BY X0."REFERENCY" ASC) X0 ' +
    'ORDER BY X0."REFERENCY", X0."ACTION") X0 ' +
    'GROUP BY X0."REFERENCY") T1 ON T0."ROWNUMBER" = T1."MAXNUMBER" AND T0."REFERENCY" = T1."REFERENCY" ' +
    'ORDER BY T0."DB", T0."REFERENCY" ASC')
  return result
}

async function getAfter20Minutes() {
  //logger.info('Service payment host to host.............................');
  // scheduler untuk kirim ke mobopay yg belom pernah ke Interfacing

  let result = await executeQuery('SELECT T0.* FROM (SELECT ROW_NUMBER () OVER ( ' +
    'PARTITION BY X0."REFERENCY" ' +
    'ORDER BY X0."ACTION") "ROWNUMBER",  * FROM( ' +
    'SELECT  X0."REFERENCY", X0."TRANSFERTYPE", TO_CHAR(X0."TRANSTIME", \'HHMI\') "TRANSTIME"  ' +
    ', X0."DB", CASE WHEN X1."TRXID" = \'\' THEN \'PAYMENT\' ELSE \'RESEND_PAYMENT\' END AS "ACTION"  ' +
    ', "PAYMENTOUTTYPE", COALESCE(X1."TRXID" , \'\') "TRXID" ' +
    'FROM paymenth2h."BOS_TRANSACTIONS" X0  ' +
    'INNER JOIN (  ' +
    'SELECT "REFERENCY", COALESCE("TRXID", \'0\') "TRXID" FROM paymenth2h."BOS_LOG_TRANSACTIONS" ' +
    'GROUP BY "REFERENCY", "TRXID") X1 ON X0."REFERENCY" = X1."REFERENCY" ' +
    'WHERE X0."INTERFACING" IN (\'0\') ' +
    'GROUP BY X0."REFERENCY", X0."TRANSFERTYPE", X0."TRANSTIME", X0."DB", X0."ACTION", X0."PAYMENTOUTTYPE", X1."TRXID" ' +
    'ORDER BY X0."REFERENCY" ASC) X0) T0 ' +
    'INNER JOIN (SELECT MAX(X0."ROWNUMBER") "MAXNUMBER", X0."REFERENCY" FROM (SELECT ROW_NUMBER () OVER ( ' +
    'PARTITION BY  X0."REFERENCY" ' +
    'ORDER BY X0."ACTION") "ROWNUMBER",  * FROM( ' +
    'SELECT  X0."REFERENCY", X0."TRANSFERTYPE", TO_CHAR(X0."TRANSTIME", \'HHMI\') "TRANSTIME"  ' +
    ', X0."DB", CASE WHEN X1."TRXID" = \'\' THEN \'PAYMENT\' ELSE \'RESEND_PAYMENT\' END AS "ACTION"  ' +
    ', "PAYMENTOUTTYPE", COALESCE(X1."TRXID" , \'\') "TRXID" ' +
    'FROM paymenth2h."BOS_TRANSACTIONS" X0  ' +
    'INNER JOIN (  ' +
    'SELECT "REFERENCY", COALESCE("TRXID", \'0\') "TRXID" FROM paymenth2h."BOS_LOG_TRANSACTIONS" ' +
    'GROUP BY "REFERENCY", "TRXID") X1 ON X0."REFERENCY" = X1."REFERENCY" ' +
    'WHERE X0."INTERFACING" IN (\'0\') ' +
    'GROUP BY X0."REFERENCY", X0."TRANSFERTYPE", X0."TRANSTIME", X0."DB", X0."ACTION", X0."PAYMENTOUTTYPE", X1."TRXID" ' +
    'ORDER BY X0."REFERENCY" ASC) X0 ' +
    'ORDER BY X0."REFERENCY", X0."ACTION") X0 ' +
    'GROUP BY X0."REFERENCY") T1 ON T0."ROWNUMBER" = T1."MAXNUMBER" AND T0."REFERENCY" = T1."REFERENCY" ' +
    'ORDER BY T0."DB", T0."REFERENCY" ASC')
  return result
}

async function getCutOff(dbName, chModelId, time_cutoff, auth) {

  var cutfOff = '';

  await fetch(config.base_url_xsjs + "/PaymentService/get_CutOff.xsjs?dbName=" + dbName + "&chmodelid=" + chModelId + "&time_cutoff=" + time_cutoff, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': auth,
      'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
    },
    'maxRedirects': 20
  })
    .then(res => res.json())
    .then(_CutOff => {
      var line = 0
      //logger.info(_CutOff)
      Object.keys(_CutOff.CUTOFF).forEach(function (key) {
        var d = _CutOff.CUTOFF[key];
        cutfOff = d.INCUTOFF;
      })
    }).catch(err => {
      logger.error("Get Error: " + err.message)
    });

  return cutfOff;
}

let taskRunning = false
//cron jobs untuk kirim ke mobopay
let jobs = cron.schedule('*/10 * * * * *', async () => {
  // scheduler untuk kirim ke mobopay yg belom pernah ke Interfacing
  //testing()
  jobs.stop()
  //await Login("");
  logger.info('Service payment host to host.............................');
  try {
    if (taskRunning) {
      logger.info('returning')
      return
    }
    taskRunning = true
    const d = new Date();
    let result = await Mobopay();
    var start = new Date();
    var _time = moment(start).format("HHmm")

    // longForLoop(10);
    var timeout = 0;
    const delay = ms => new Promise(res => setTimeout(res, ms));
    for await (let data of result.rows) {

      if (data.rowCount != "0") {
        await jobs.stop()
        //logger.info(data.DB + "," + data.TRANSFERTYPE)
        let CutOff = await getCutOff(data.DB, data.TRANSFERTYPE, _time, "Basic " + config.auth_basic)

        if (CutOff == "FALSE") {
          logger.info("Payment (" + data.DB + "), Ref: " + data.REFERENCY + ", TRXID:" + data.trxId)
          connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "CUTOFF" = \'N\' WHERE "REFERENCY" = $1', [data.REFERENCY])
          await interfacing_mobopay_Transaction(data.REFERENCY, data.ACTION, data.PAYMENTOUTTYPE, data.trxId)
        } else {
          await updateCufOff(data.REFERENCY, data.DB, data.PAYMENTOUTTYPE, data.trxId)
        }
      }
      await delay(config.timedelay);
    }

    await jobs.start()

    setTimeout(() => {
      resolve();
      taskRunning = false

    });
  } catch (err) {
    logger.error(err);
  }
  // let login = await LoginV2("PSTTESTING2022");
  // logger.info(login)
});

// let jobs_after20 = cron.schedule('* */20 * * * *', async () => {
//   //testing()
//   jobs.stop()
//   logger.info('Service payment host to host.............................');
//   try {
//     if (taskRunning) {
//       logger.info('returning')
//       return
//     }
//     taskRunning = true
//     const d = new Date();
//     let result = await getAfter20Minutes();
//     var start = new Date();
//     var _time = moment(start).format("HHmm")

//     // longForLoop(10);
//     var timeout = 0;
//     const delay = ms => new Promise(res => setTimeout(res, ms));
//     for await (let data of result.rows) {

//       if (data.rowCount != "0") {
//         await jobs.stop()
//         //logger.info(data.DB + "," + data.TRANSFERTYPE)
//         let CutOff = await getCutOff(data.DB, data.TRANSFERTYPE, _time, "Basic " + config.auth_basic)

//         if (CutOff == "FALSE") {
//           logger.info("Payment (" + data.DB + "), Ref: " + data.REFERENCY + ", TRXID:" + data.trxId)
//           connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "CUTOFF" = \'N\' WHERE "REFERENCY" = $1', [data.REFERENCY])
//           await interfacing_mobopay_Transaction(data.REFERENCY, data.ACTION, data.PAYMENTOUTTYPE, data.trxId)
//         } else {
//           await updateCufOff(data.REFERENCY, data.DB, data.PAYMENTOUTTYPE, data.trxId)
//         }
//       }
//       await delay(config.timedelay);
//     }

//     await jobs.start()

//     setTimeout(() => {
//       resolve();
//       taskRunning = false

//     });
//   } catch (err) {
//     logger.error(err);
//   }
//   // let login = await LoginV2("PSTTESTING2022");
//   // logger.info(login)
// });

async function updateCufOff(reference, db, outType, trxId) {
  var start = new Date();
  await connection.query('SELECT "CUTOFF" FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1', [reference], async function (error, result, fields) {
    if (error) {
      logger.error(error)
    } else {
      //logger.info(result)
      for (var data of result.rows) {
        if (data.CUTOFF == "N" || data.CUTOFF == null) {
          await getEntryUdoOut(reference, db, trxId, outType, true)

          // update flag cutoff
          await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "CUTOFF" = \'Y\' WHERE "REFERENCY" = $1', [reference])
          // insert ke log jika terkena cutoff
          await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
            'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 4, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID",  $5' +
            'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', ["Transaction at cutoff time", reference, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), "EOD"], async function (error, result, fields) {
              if (error) {
                logger.error(error)
              }
            });
        } else {
          //let cutUpdateUdo = await getEntryUdoOut(reference, db, trxId, outType, true)
          //await logger.info(cutUpdateUdo)
        }
      }
    }
  });
}

exports.get_Signature = function (req, res) {
  logger.info('Body: ' + req.body.message)
  var Response_ = signature(req.body.message)
  timeout = true;
  var result = {
    'Signature': Response_
  }
  res.json(result);
  logger.info('Signature: ' + Response_)
}


exports.get_Token = function (req, res) {
  var token = jwt.sign({ foo: 'bar' }, 'shhhhh');
  var result = {
    'access_token': token,
    'Timeout': '5 minutes'
  }
  res.json(result);
  logger.info('Token: ' + result)
}


function generateMessageBodySignature(message, privateKey) {
  try {
    const sign = crypto.createSign('RSA-SHA1');
    sign.update(message);
    sign.end();
    const signature = sign.sign(privateKey);
    return signature.toString('base64')
  } catch (error) {
    logger.error(error);
  }
}

exports.get_Verification = async function (req, res) {
  var start = new Date();
  var date = moment(start).format('YYYYMMDD')

  logger.info("(IN) Moboay -> Middleware: Proses Validation Payment, Body ", req.body);
  await connection.query('SELECT "PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", CAST("AMOUNT" AS DECIMAL(18,2)) "AMOUNT"' +
    ', TO_CHAR("TRANSDATE", \'YYYYMMDD\') "TRANSDATE", TO_CHAR("TRANSTIME", \'HH24MIss\') "TRANSTIME", "STATUS", "REASON", CAST("BANKCHARGE" AS DECIMAL(18,2)) "BANKCHARGE", "FLAGUDO"' +
    ', "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID"' +
    'FROM paymenth2h."BOS_LOG_TRANSACTIONS"  WHERE "TRXID" = $1 limit 1', [req.body.trxId], async function (error, result, fields) {
      if (error) {
        logger.info(error)
        var rowdata = {
          'result_code': '1',
          'Result_msg': 'OK',
          'Message': error.message,
          'date': moment(start).format('YYYY-MM-DD'),
          'time': moment(start).format('HH:mm:DD')
        };
        logger.info("Error: " + rowdata)
        res.json(rowdata);
      } else {
        //logger.info(result)
        rowdata = {};
        dbkode = "";
        for (var data of result.rows) {
          rowdata = {
            'paymentOutType': data.PAYMENTOUTTYPE,
            'paymentNo': data.PAYMENTNO,
            'vendor': data.VENDOR,
            'cusRef': data.REFERENCY,
            'trxId': data.TRXID,
            'clientId': data.CLIENTID,
            'preferedMethodTransferId': data.TRANSFERTYPE,
            'sourceAccount': data.SOURCEACCOUNT,
            'targetAccount': data.ACCOUNT,
            'amount': data.AMOUNT
          }
        }

        logger.info(rowdata)

        if (rowdata != "") {
          res.json(rowdata);
        } else {
          await connection.query('SELECT "PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", CAST("AMOUNT" AS DECIMAL(18,2)) "AMOUNT"' +
            ', TO_CHAR("TRANSDATE", \'YYYYMMDD\') "TRANSDATE", TO_CHAR("TRANSTIME", \'HH24MIss\') "TRANSTIME", "STATUS", "REASON", CAST("BANKCHARGE" AS DECIMAL(18,2)) "BANKCHARGE", "FLAGUDO"' +
            ', "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID"' +
            'FROM paymenth2h."BOS_LOG_TRANSACTIONS"  WHERE "TRXID" = $1 limit 1', [req.body.trxId], function (error, result, fields) {
              if (error) {
                logger.info(error)
                var rowdata = {
                  'result_code': '1',
                  'Result_msg': 'OK',
                  'Message': error.message,
                  'date': moment(start).format('YYYY-MM-DD'),
                  'time': moment(start).format('HH:mm:DD')
                };
                logger.info("Error: " + rowdata)
                res.json(rowdata);
              } else {
                //logger.info(result)
                rowdata = {};
                dbkode = "";
                for (var data of result.rows) {
                  rowdata = {
                    'paymentOutType': data.PAYMENTOUTTYPE,
                    'paymentNo': data.PAYMENTNO,
                    'vendor': data.VENDOR,
                    'cusRef': data.REFERENCY,
                    'trxId': data.TRXID,
                    'clientId': data.CLIENTID,
                    'preferedMethodTransferId': data.TRANSFERTYPE,
                    'sourceAccount': data.SOURCEACCOUNT,
                    'targetAccount': data.ACCOUNT,
                    'amount': data.AMOUNT
                  }
                }
                logger.info(rowdata)
                res.json(rowdata);
              }
            })
        }
      }
    });
}

exports.post_statusPayment = async function (req, res) {
  var start = new Date();
  var date = moment(start).format('YYYYMMDD')

  logger.info("(IN) Moboay -> Middleware: Payment Status, Body ", JSON.stringify(req.body));
  //logger.info("Response Transaksi.......!!!!")
  var rowdata = {
    'resultCode': '0',
    'resultMsg': 'OK',
    'message': 'Transaction Success',
    'date': moment(start).format('YYYY-MM-DD'),
    'time': moment(start).format('HH:mm:DD')
  };
  res.json(rowdata);
  var data_transaction = [];
  data_transaction = [
    req.body.trxId
    , req.body.resultCode
    , req.body.message
    , req.body.errorMessage
    , req.body.debitAccount
    , req.body.creditAccount
    , req.body.valueCurrency
    , parseFloat(req.body.valueAmount)
    , req.body.customerReffNumber
    , '0'
    , req.headers.authorization
  ]


  //logger.info(data_transaction)


  if (req.body.resultCode == "1") {
    var data_transaction = [];
    data_transaction = [
      req.body.trxId
      , req.body.resultCode
      , req.body.message
      , req.body.errorMessage
      , req.body.debitAccount
      , req.body.creditAccount
      , req.body.valueCurrency
      , parseFloat(req.body.valueAmount)
      , req.body.customerReffNumber
      , '0'
      , req.headers.authorization
    ]


    // INSERT KE LOG, JIKA SUDAH MENDAPATAKN STATUS PEMBAYARAN DARI MOBOPAY
    await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
      'SELECT "PAYMENTOUTTYPE", "PAYMENTNO",$6, "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 0, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
      'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', ['Mobopay ' + req.body.message + ' - Payment Success', req.body.customerReffNumber, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), '', req.body.trxId], async function (error, result, fields) {
        if (error) {
          logger.error(error)
        }
      });

    //logger.info(data_transaction)
    // cek payment di log
    await connection.query('SELECT COUNT(*) "COUNTING" FROM paymenth2h."BOS_DO_STATUS" WHERE "trxId" = $1', [req.body.trxId], async function (error, result, fields) {
      if (error) {
        logger.error(error)
      } else {
        for (var data of result.rows) {
          if (data.COUNTING == "0") // jika ada baka tidak diinsert lagi
          {
            let CompanyDb = await executeQuery('SELECT "DB", "PAYMENTOUTTYPE" FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1 limit 1', [req.body.customerReffNumber])
            for await (var db of CompanyDb.rows) {
              await getEntryUdoOut(req.body.customerReffNumber, db.DB, req.body.trxId, db.PAYMENTOUTTYPE, false)
            }
            await executeQuery('INSERT INTO paymenth2h."BOS_DO_STATUS" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)', data_transaction, function (error, result1, fields) {
              if (error) {

              } else {

              }
            });
          } else // jika belom ada maka diinsett
          {
            var rowdata = {
              'resultCode': '1',
              'resultMsg': 'FAIL',
              'message': 'Transaction has already exist',
              'date': moment(start).format('YYYY-MM-DD'),
              'time': moment(start).format('HH:mm:DD')
            };
            logger.info(rowdata)
            res.json(rowdata);
          }
        }
      }
    });
  }
  if (req.body.resultCode != "1") {

    var data_transaction = [];
    data_transaction = [
      req.body.trxId
      , req.body.resultCode
      , req.body.message
      , req.body.errorMessage
      , req.body.debitAccount
      , req.body.creditAccount
      , req.body.valueCurrency
      , parseFloat(req.body.valueAmount)
      , req.body.customerReffNumber
      , '0'
      , req.headers.authorization
    ]

    //logger.info(data_transaction)

    let cekError = await executeQuery('SELECT COUNT(*) "errorCount" FROM paymenth2h."BOS_LOG_TRANSACTIONS" WHERE "REFERENCY" = $1 AND "ERRORCODE" = $2', [req.body.customerReffNumber, req.body.resultCode])
    if (cekError.rowCount > 0) {
      for (var data of cekError.rows) {
        if (data.errorCount == "0") {
          await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
            'SELECT "PAYMENTOUTTYPE", "PAYMENTNO",$6, "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 4, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
            'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [req.body.errorMessage, req.body.customerReffNumber, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), req.body.resultCode, req.body.trxId], async function (error, result, fields) {
              if (error) {
                logger.error(error)
              }
            });
          var trx_id = "";
          if (req.body.trxId != "") {
            trx_id = req.body.trxId;
            await executeQuery('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "TRXID" = $2, "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [req.body.customerReffNumber, req.body.trxId])
          }
          await executeQuery('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [req.body.customerReffNumber])
          let CompanyDb = await executeQuery('SELECT "DB", "PAYMENTOUTTYPE" FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1 limit 1', [req.body.customerReffNumber])
          for await (var db of CompanyDb.rows) {
            await getEntryUdoOut(req.body.customerReffNumber, db.DB, trx_id, db.PAYMENTOUTTYPE, true)
          }
        } else {
          await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
            'SELECT "PAYMENTOUTTYPE", "PAYMENTNO",$6, "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 4, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
            'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [req.body.errorMessage, req.body.customerReffNumber, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), req.body.resultCode, req.body.trxId], async function (error, result, fields) {
              if (error) {
                logger.error(error)
              }
            });
          var trx_id = "";
          if (req.body.trxId != "") {
            trx_id = req.body.trxId;
            await executeQuery('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "TRXID" = $2, "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [req.body.customerReffNumber, req.body.trxId])
          }
          await executeQuery('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [req.body.customerReffNumber])
          let CompanyDb = await executeQuery('SELECT "DB", "PAYMENTOUTTYPE" FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1 limit 1', [req.body.customerReffNumber])
          for await (var db of CompanyDb.rows) {
            await getEntryUdoOut(req.body.customerReffNumber, db.DB, trx_id, db.PAYMENTOUTTYPE, true)
          }
        }
      }
    }
  }
}

exports.post_closePayment = async function (req, res) {
  var start = new Date();
  var date = moment(start).format('YYYYMMDD')

  logger.info("(IN) SAP -> Middleware: Close Transaksi, Body" + req.body);
  //logger.info("Close Transaksi.......!!!!")
  var rowdata = {
    'result': '0',
    'msgCode': 'OK',
    'msgDscriptions': 'Close Transaction Success'
  };
  res.json(rowdata);

  // cek data jika ada
  let result = await connection.query('SELECT COUNT(*) "COUNTING" FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1', [req.body.referency])
  for (var data of result.rows) {
    if (data.COUNTING != "0") // jika ada baka tidak diinsert lagi
    {
      await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID")' +
        'SELECT "PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 0, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID"' +
        'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', ['Generate Outgoing by user', req.body.referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss")], async function (error, result, fields) {
          if (error) {
            logger.error(error)
          }
        });

      let update = await executeQuery('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = $2 WHERE "REFERENCY" = $1', [req.body.referency, 'Y'])
      await executeQuery('UPDATE paymenth2h."BOS_DO_STATUS" SET "flag" = \'1\' WHERE "customerReffNumber" = $1', [req.body.referency], async function (error, result, fields) {
        if (error) {
          logger.error(error)
        }
      });
    }
  }
}

async function Login(dbName) {
  var raw = JSON.stringify({
    "CompanyDB": dbName,
    "UserName": config.userName,
    "Password": config.Password
  });
  let breakTheLoop = false;
  for (config.sl_nextPort = 0; config.sl_nextPort <= 4; config.sl_nextPort++) {

    let result = await fetch(config.base_url_SL + config.sl_nextPort + "/b1s/v2/Login",
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: raw,
        headers: { 'Content-Type': 'application/json' },
        withCredentials: true,
        xhrFields: {
          'withCredentials': true
        }
      }
    )
      .then(response => response.text())
      .then(result => {
        return result;
      })
      .catch(error => {
        //logger.info('error', error)
        var error = {
          "error": "Error Login"
        };

        return error;
      });

    logger.info(result);
    var resultMsg = result;
    if (result != "") {
      const resultBody = JSON.parse(result)
      logger.info(resultBody)
      if (!resultBody.error) {
        Session = resultBody.SessionId
        config.sl_realPort = config.base_url_SL + config.sl_nextPort
        logger.info("Port is Used: " + config.sl_realPort)
        break;
      } else {
        logger.info("error: " + config.sl_realPort)
        continue;
      }
    }
  }
}

async function LoginV2(dbName) {
  jobs.stop();
  /* 1 */
  var myHeaders = new nodeFetch.Headers();
  //var retries = 3;
  var backoff = 300;
  myHeaders.append("Authorization", "Basic " + config.auth_basic);
  myHeaders.append("Content-Type", "application/json");
  //myHeaders.append("Cookie", "B1SESSION=01d87ac4-3379-11ed-8000-000c297b22e1");
  await logger.info("(OUT) Middleware - Service Layer (" + dbName + "), Login : " + config.base_url_SL + config.sl_nextPort + "/b1s/v2/Login")
  var raw = JSON.stringify({
    "CompanyDB": dbName,
    "UserName": config.userName,
    "Password": config.Password
  });

  var requestOptions = {
    method: 'POST',
    headers: myHeaders,
    body: raw,
    redirect: 'follow'
  };

  const fetchWithTimeout = (input, init, timeout) => {
    const controller = new AbortController();
    setTimeout(() => {
      controller.abort();
    }, timeout)

    return fetch(input, { signal: controller.signal, ...init });
  }

  const wait = (timeout) =>
    new Promise((resolve) => {
      setTimeout(() => resolve(), timeout);
    })

  const fetchWithRetry = async (input, init, timeout, retries) => {
    let increseTimeOut = config.interval_retries;
    let count = retries;

    while (count > 0) {
      try {
        return await fetchWithTimeout(input, init, timeout);
      } catch (e) {
        logger.info(e.name)
        if (e.name !== "AbortError") throw e;
        count--;
        logger.error(
          `fetch Login, retrying login in ${increseTimeOut}s, ${count} retries left`
        );
        await wait(increseTimeOut)
      }
    }
  }

  var Session = ""
  var url = config.base_url_SL + config.sl_nextPort + "/b1s/v2/Login";
  const retryCodes = [408, 500, 502, 503, 504, 522, 524]
  return await fetchWithRetry(url, requestOptions, config.timeOut, config.max_retries)
    .then(async res => {
      //logger.info("Login Session (" + dbName + "): ", res.status);
      if (res.status == 200) return res.text()

      if (config.max_retries > config.min_retries && (res.status != 200 || res.status == "")) {
        setTimeout(() => {
          /* 2 */
          logger.info("Retry Login Session (" + dbName + "): Count: " + config.min_retries++ + " From " + config.max_retries);
          return LoginV2(dbName) /* 3 */
        }, config.interval_retries) /* 2 */
      } else {
        throw new Error(res.text())
      }
    })
    .then(result => {
      logger.info("Body Result Login: ", JSON.parse(result))
      if (result != "") {
        const resultBody = JSON.parse(result)
        if (!resultBody.error) {
          Session = resultBody.SessionId
          config.sl_realPort = config.base_url_SL + config.sl_nextPort
          logger.info("Port is Used: " + config.sl_realPort)

          return Session;
        } else {
          logger.info("Error Login (" + dbName + ")", result);
        }
      }

    })
    .catch(error => {
      logger.info('Error Login', error)
    });

  jobs.start();
}

async function Login_V2(dbName) {
  var myHeaders = new nodeFetch.Headers();
  myHeaders.append("Authorization", "Basic " + config.auth_basic);
  myHeaders.append("Content-Type", "application/json");
  //myHeaders.append("Cookie", "B1SESSION=01d87ac4-3379-11ed-8000-000c297b22e1");

  var raw = JSON.stringify({
    "CompanyDB": dbName,
    "UserName": config.userName,
    "Password": config.Password
  });

  var requestOptions = {
    method: 'POST',
    headers: myHeaders,
    body: raw,
    redirect: 'follow'
  };

  const fetchWithTimeout = (input, init, timeout) => {
    const controller = new AbortController();
    setTimeout(() => {
      controller.abort();
    }, timeout)

    return fetch(input, { signal: controller.signal, ...init });
  }

  const wait = (timeout) =>
    new Promise((resolve) => {
      setTimeout(() => resolve(), timeout);
    })

  const fetchWithRetry = async (input, init, timeout, retries) => {
    let increseTimeOut = config.interval_retries;
    let count = retries;

    while (count > 0) {
      try {
        return await fetchWithTimeout(input, init, timeout);
      } catch (e) {
        logger.info(e.name)
        if (e.name !== "AbortError") throw e;
        count--;
        logger.error(
          `fetch Login, retrying login in ${increseTimeOut}s, ${count} retries left`
        );
        await wait(increseTimeOut)
      }
    }
  }
  var Session = ""
  var breakTheLoop = false;
  await logger.info("(OUT) Middleware - Service Layer (" + dbName + "), Login : " + config.base_url_SL + config.sl_nextPort + "/b1s/v2/Login")
  await fetchWithRetry(config.base_url_SL + config.sl_nextPort + "/b1s/v2/Login", requestOptions, 120000, 3)
    .then(response => response.text())
    .then(result => {
      logger.info("Login Session: " + result);
      if (result != "") {

        const resultBody = JSON.parse(result)
        if (!resultBody.error) {
          Session = resultBody.SessionId
          config.sl_realPort = config.base_url_SL + config.sl_nextPort
          logger.error("Port is Used: " + config.sl_realPort)
          breakTheLoop = true;
        } else {
          logger.error(result);
        }
      }
    })
    .catch(error => {
      logger.error('error', error)
    });


  return Session;
}

async function post_Incoming(trxid, customerReffNumber, valueAmount, auth, dbName) {

  var start = new Date();
  var date = moment(start).format('YYYYMMDD')

  var Header = []
  var PaymentInvoices = []
  var linesCashfLOW = []
  var _bankCharge = 0;
  var _TotalAmount = 0;
  var result = {};

  // mendaptakand Bank Charge
  await connection.query('SELECT CAST("BANKCHARGE" AS DECIMAL(19,2)) "BANKCHARGE", CAST("AMOUNT" AS DECIMAL(19,2)) "AMOUNT"  FROM paymenth2h."BOS_TRANSACTIONS" WHERE "TRXID" = $1 AND "REFERENCY" = $2', [trxid, customerReffNumber], async function (error, result, fields) {
    if (error) {
      var rowdata = {
        'result_code': '1',
        'Result_msg': 'FAIL',
        'Message': error.message,
        'date': moment(start).format('YYYY-MM-DD'),
        'time': moment(start).format('HH-mm-DD')
      };
      logger.info("Error: " + rowdata)
      res.json(rowdata);
    } else {
      for (var data of result.rows) {
        _bankCharge = parseFloat(data.BANKCHARGE)
        _TotalAmount = parseFloat(data.AMOUNT)
      }
    }
  });

  // mendaptakand detail
  await fetch(config.base_url_xsjs + "/PaymentService/get_invDetail.xsjs?trxid=" + trxid + "&dbName=" + dbName, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': auth,
      'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
    },
    'maxRedirects': 20
  })
    .then(res => res.json())
    .then(dataInvoices => {
      var line = 0
      logger.info(dataInvoices)
      Object.keys(dataInvoices.DETAIL).forEach(function (key) {
        var d = dataInvoices.DETAIL[key];
        var intype = '';
        if (d.U_DOCTYPE == "APDP") {
          intype = '204'
        } else if (d.U_DOCTYPE == "AP") {
          intype = '18'
        } else {
          intype = '19'
        }

        logger.info(d.U_DOCTYPE)
        var rowdata = {
          "LineNum": line,
          "DocEntry": d.DocEntry,
          "SumApplied": d.U_REQTOTAL,
          "InvoiceType": intype,
          "InstallmentId": d.U_INSTALLMENT
        };
        PaymentInvoices.push(rowdata);
        line++;
      })

    })
    .catch(err => {
      logger.info("Get Error: " + err.message)
      var rowdata = {
        'result_code': '1',
        'Result_msg': 'OK',
        'Message': err.message,
        'date': moment(start).format('YYYY-MM-DD'),
        'time': moment(start).format('HH-mm-DD')
      };
      logger.info("Error: " + rowdata)
      res.json(rowdata);

      logger.info("Get Error: " + err.message)
    });

  // casfhflow line item
  await fetch(config.base_url_xsjs + "/PaymentService/get_CFL.xsjs?trxid=" + trxid + "&dbName=" + dbName, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': auth,
      'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
    },
    'maxRedirects': 20
  })
    .then(res => res.json())
    .then(data => {
      logger.info(data)
      Object.keys(data.CFL).forEach(function (key) {
        var d = data.CFL[key];

        if (parseFloat(_bankCharge) > 0) {
          var amount_cf
          if (d.Row == "1") {
            amount_cf = parseFloat(d.Amount) + parseFloat(_bankCharge)
          } else {
            amount_cf = parseFloat(d.Amount)
          }
          var rowdata = {
            'CashFlowLineItemID': d.CFWId,
            'PaymentMeans': 'pmtBankTransfer',
            'AmountLC': amount_cf
          };
          linesCashfLOW.push(rowdata);
        }
      })
      logger.info(linesCashfLOW)
    })
    .catch(err => {
      logger.info("Get Error: " + err.message)
      var rowdata = {
        'result_code': '1',
        'Result_msg': 'OK',
        'Message': err.message,
        'date': moment(start).format('YYYY-MM-DD'),
        'time': moment(start).format('HH-mm-DD')
      };
      logger.info("Error: " + rowdata)
      res.json(rowdata);

      logger.info("Get Error: " + err.message)
    });

  // header
  await fetch(config.base_url_xsjs + "/PaymentService/get_Header.xsjs?trxid=" + trxid + "&dbName=" + dbName, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': auth,
      'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
    },
    'maxRedirects': 20
  })
    .then(res => res.json())
    .then(async data => {

      Object.keys(data.HEADER).forEach(async function (key) {
        var d = data.HEADER[key];
        var amoutNet = parseFloat(d.Amount) + parseFloat(_bankCharge)
        result = {
          Series: d.Series,
          DocObjectCode: 'bopot_OutgoingPayments',
          DocType: 'rSupplier',
          CardCode: d.CardCode,
          DocDate: date,
          DueDate: date,
          VatDate: date,
          Remarks: 'Interfacing Payment Approval, Code :' + d.Payment_no,
          TransferAccount: d.Account,
          TransferSum: amoutNet,
          BankChargeAmount: _bankCharge,
          U_PYMNO: d.Payment_no,
          PaymentInvoices: PaymentInvoices,
          CashFlowAssignments: linesCashfLOW
        }
      })
      logger.info(result)
      if (data != null) {
        // interfacing ke outgoing payment

        await LoginV2(dbName) // login ke service layer version 1

        await fetch(config.sl_realPort + '/b1s/v2/VendorPayments', {
          method: 'POST',
          body: JSON.stringify(result),
          headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
          },
          crossDomain: true,
          credentials: "include",
          withCredentials: true,
          xhrFields: {
            'withCredentials': true
          },
        }).then(res3 => {
          return res3.json()
        }).then(async data => {
          logger.info("Berhasil membuat Outgoing Payment")

          if (!data.error) {
            logger.info("Update Flag.....................")
            // jika sudah berhasil, update ditabel LOG Transaction
            await connection.query('CALL paymenth2h.Update_Flag($1)', [trxid], async function (error, result, fields) {
              if (error) {
                logger.info(error)
              } else {
                // jika tidak error maka update flag UDO menjadi success
                // get data lines dari payment OUT/OUT STATUS
                await fetch(config.base_url_xsjs + "/PaymentService/get_detail_lines.xsjs?trxid=" + trxid + "&dbName=" + dbName, {
                  method: 'GET',
                  headers: {
                    'Content-Type': 'application/json',
                    'Authorization': auth,
                    'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
                  },
                  'maxRedirects': 20
                })
                  .then(res => res.json())
                  .then(async data => {
                    Object.keys(data.LINES).forEach(async function (key) {
                      var d = data.LINES[key];
                      var details = []

                      if (d.Type == "OUT") // jika payment out
                      {
                        var row = {
                          'Lines': d.Lines,
                          'U_STATUS': 'Success'
                        }

                        details.push(row)

                        await LoginV2()


                        // update UDO
                        await fetch(config.sl_realPort + "/b1s/v2/PYM_OUT(" + parseFloat(d.DocEntry) + ")",
                          {
                            method: 'PATCH',
                            headers: {
                              'Content-Type': 'application/json'
                            },
                            body: JSON.stringify({ BOS_PYM_OUT1Collection: details }),
                            headers: { 'Content-Type': 'application/json' },
                            withCredentials: true,
                            xhrFields: {
                              'withCredentials': true
                            }
                          }
                        )
                          .then(Response => Response.json())
                          .then(data => {
                            logger.info(data)
                          })
                          .catch(err => {
                            logger.info(err)
                            var Status = [];
                            var s = {
                              'ErrorCode': '[300]',
                              'Msg': 'Failed, Error: ' + err.message.value,
                              'SuccessCode': ''
                            }
                            Status.push(s)
                            logger.info(Status);
                          });

                        // akhir dari update UDO
                      } else if (d.Type == "OUTSTATUS") // jika payment out STATUS
                      {
                        var row = {
                          'LineId': d.Lines,
                          'U_STATUS': 'Success'
                        }

                        details.push(row)


                        // update UDO
                        await fetch(config.sl_realPort + "/b1s/v2/PYMOUT_STATUS(" + parseFloat(d.DocEntry) + ")",
                          {
                            method: 'PATCH',
                            headers: {
                              'Content-Type': 'application/json'
                            },
                            body: JSON.stringify({ BOS_PYMOUT_STATUS1Collection: details }),
                            headers: { 'Content-Type': 'application/json' },
                            withCredentials: true,
                            xhrFields: {
                              'withCredentials': true
                            }
                          }
                        )
                          .then(Response => Response.json())
                          .then(data => {
                            logger.info(data)
                          })
                          .catch(err => {
                            logger.info(err)
                            var Status = [];
                            var s = {
                              'ErrorCode': '[300]',
                              'Msg': 'Failed, Error: ' + err.message.value,
                              'SuccessCode': ''
                            }
                            Status.push(s)
                            logger.info(Status);
                          });

                        // akhir dari update UDO
                      }
                    })
                  })
                  .catch(err => {
                    logger.info("Get Error: " + err.message)
                    var rowdata = {
                      'result_code': '1',
                      'Result_msg': 'OK',
                      'Message': err.message,
                      'date': moment(start).format('YYYY-MM-DD'),
                      'time': moment(start).format('HH-mm-DD')
                    };
                    logger.info("Error: " + rowdata)
                    res.json(rowdata);

                    logger.info("Get Error: " + err.message)
                  });

              }
            })
          }

        }).catch(err => {
          logger.info(err)
          var Status = [];
          var s = {
            'ErrorCode': '[300]',
            'Msg': 'Outgoing Payment Failed,Msg: ' + err.error.message.value,
            'SuccessCode': ''
          }
          Status.push(s)
          logger.info(s)
          res.json(s)
        });
        // end of interfacing outgoing payment
      }
    })
    .catch(err => {
      logger.info(err)
      var rowdata = {
        'result_code': '1',
        'Result_msg': 'Fail',
        'Message': 'Failed Create Outgiong Payment',
        'date': moment(start).format('YYYY-MM-DD'),
        'time': moment(start).format('HH-mm-DD')
      };
      logger.info("Error: " + rowdata)
      res.json(rowdata);

      logger.info("Get Error: " + err.message)
    });
}

exports.get_testing = function (req, res) {
  pool.query('select "ID_DB","NM_DB"  from paymenth2h."BOS_DB_LIST"', function (error, result, fields) {
    if (error) {
      logger.error(error)
    } else {
      for (var data of result.rows) {
        logger.info(data.ID_DB)
      }
    }
  });
}

exports.post_logTransactions = async function (req, res) {
  var start = new Date();
  var date = moment(start).format('YYYYMMDD')

  //jobs.stop();

  //logger.info(req.body) 

  logger.info("(IN) SAP - Middleware: Insert Data Transaksi, Body: ", req.body)


  var data_transaction = [];
  data_transaction = [req.body.paymentOutType
    , req.body.paymentNo
    , req.body.trxId
    , req.body.referency
    , req.body.vendor
    , req.body.account
    , req.body.amount
    , req.body.transDate
    , req.body.transTime
    , req.body.status
    , req.body.reason
    , req.body.bankCharge
    , req.body.flagUdo
    , req.body.sourceAccount
    , req.body.transferType
    , req.body.clientId
    , req.body.db
    , '0'
    , ''
    , req.body.chargingModelId
    , req.body.action
    , "0"
    , "N"
  ]

  // untuk pengecekan data jika payment sudah berhasil di mobopay
  await connection.query('SELECT COUNT(*) AS "Counting"  FROM paymenth2h."BOS_DO_STATUS" WHERE "customerReffNumber" = $1', [req.body.referency], async function (errorCheck, resultCheck, fieldCheck) {
    if (errorCheck) {
      logger.error(errorCheck)
    } else {
      for (var data of resultCheck.rows) {
        if (data.Counting == "0") {
          if (req.body.paymentOutType == "OUT") {
            await connection.query('SELECT COUNT(*) AS "Counting"  FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1', [req.body.referency], async function (error, result, fields) {
              if (error) {
                logger.error(error)
              } else {
                for (var data of result.rows) {
                  if (data.Counting == "0") {
                    await connection.query('INSERT INTO paymenth2h."BOS_TRANSACTIONS" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)', data_transaction, async function (error, result1, fields) {
                      if (error) {
                        logger.error(error)
                      } else {
                        var data1 = {
                          'result': "0",
                          'msgCode': "0",
                          'msgDscriptions': "Success"
                        }
                        res.json(data1);

                        // jika berhasil di add maka insert ke log
                        await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
                          'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, $1, $6, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
                          'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [1, req.body.referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), '', ''], async function (error, result, fields) {
                            if (error) {
                              logger.error(error)
                            }
                            //await executeQuery('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [referency])
                          });
                      }
                    });
                  } else {
                    var data1 = {
                      'result': "0",
                      'msgCode': "0",
                      'msgDscriptions': "Success"
                    }
                    res.json(data1);
                    logger.info(data)
                  }
                }
              }
            });

          } else { // jika dari payment outstatus
            let cekError = await executeQuery('SELECT CAST(coalesce(MAX("RESENDCOUNT"),\'0\') as INT) + 1 as "RESENDCOUNT" FROM  paymenth2h."BOS_TRANSACTIONS"  WHERE "REFERENCY"=$1', [req.body.referency])
            if (cekError.rowCount > 0) {
              for (var data of cekError.rows) {
                logger.info("check resend count ("+req.body.db+"): " + req.body.referency+ ", "+ parseFloat(data.RESENDCOUNT))
                if (parseFloat(data.RESENDCOUNT) <= config.resend_payment_retries) {
                  //await executeQuery('UPDATE  paymenth2h."BOS_TRANSACTIONS_D" SET "preferredTransferMethodId" = $1  WHERE "customerReferenceNumber" = $2', [req.body.chargingModelId, req.body.referency])
                 
                  
                  await connection.query('DELETE FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1', [req.body.referency], async function (error, result, fields) {
                    if (error) {
                      logger.error(error)
                    } else {

                      var data1 = {
                        'result': "0",
                        'msgCode': "0",
                        'msgDscriptions': "Success"
                      }
                      res.json(data1);

                      await connection.query('INSERT INTO paymenth2h."BOS_TRANSACTIONS" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)', data_transaction, async function (error1, result1, fields) {
                        if (error1) {
                          logger.error(error1)
                        } else {
                          await executeQuery('UPDATE  paymenth2h."BOS_TRANSACTIONS" SET "RESENDCOUNT" = $1  WHERE "REFERENCY" = $2', [data.RESENDCOUNT, req.body.referency])
                         
                          await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
                            'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, $1, $6, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
                            'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [3, req.body.referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), '', ''], async function (error, result, fields) {
                              if (error) {
                                logger.error(error)
                              }
                              //await executeQuery('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [referency])
                            });
                        }
                      });
                    }
                  });
                } else {

                  //CEK JIKA MASIH ERROR CODE MAX RESED
                  await executeQuery('UPDATE  paymenth2h."BOS_TRANSACTIONS_D" SET "preferredTransferMethodId" = $1  WHERE "customerReferenceNumber" = $2', [req.body.chargingModelId, req.body.referency])
                  
                  
                  let cekError = await executeQuery('SELECT COUNT(*) "errorCount", "TRXID"  FROM paymenth2h."BOS_LOG_TRANSACTIONS" WHERE "REFERENCY" = $1 AND "ERRORCODE" = $2 AND "TRXID" != \'\' GROUP BY "TRXID"', [req.body.referency, 'SAP-MR01'])

                  var trxid;
                  if (cekError.rowCount >= 0 || cekError.rowCount == "") {
                    for (var data of cekError.rows) {

                      trxid = data.TRXID

                      if (data.errorCount == "0" || data.errorCount == "") {
                        await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
                          'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, $1, $5, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $6' +
                          'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [4, req.body.referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), 'Maximum Resend', 'SAP-MR01'], async function (error, result, fields) {
                            if (error) {
                              logger.error(error)
                            }
                          });
                      }
                    }
                  }

                  res.json({
                    'result': "OK",
                    'msgCode': "1",
                    'msgDscriptions': 'Maximum Resend'
                    // 'result': "0",
                    // 'msgCode': "0",
                    // 'msgDscriptions': "Success"
                  });
                  await getEntryUdoOut(req.body.referency, req.body.db, trxid, req.body.paymentOutType, true)
                }
              }
            }
          }
        } else {
          await executeQuery('UPDATE paymenth2h."BOS_DO_STATUS" SET "flag" = \'1\' WHERE "customerReffNumber" = $1', [req.body.referency], async function (error, result, fields) {
            if (error) {
            }
          });
          await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
            'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, $1, $5, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $6' +
            'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [4, req.body.referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), "Can't Reproses this payment, Payment has been Success", 'SAP-MR02'], async function (error, result, fields) {
              if (error) {
                logger.info(error)
              }
            });

          res.json({
            'result': "OK",
            'msgCode': "2",
            'msgDscriptions': 'Payment Success'
          });
        }
      }
    }
  })


}

// function detail transaksi
exports.post_detailTrasactions = async function (req, res) {
  var start = new Date();
  var date = moment(start).format('YYYYMMDD')

  logger.info("(IN) SAP - Middleware: Details Payment, Body: ", req.body)
  var data = [];
  data = [req.body.preferredTransferMethodId,
  req.body.debitAccount,
  req.body.creditAccount,
  req.body.customerReferenceNumber,
  req.body.chargingModelId,
  req.body.defaultCurrencyCode,
  req.body.debitCurrency,
  req.body.creditCurrency,
  req.body.chargesCurrency,
  req.body.remark1,
  req.body.remark2,
  req.body.remark3,
  req.body.remark4,
  req.body.paymentMethod,
  req.body.extendedPaymentDetail,
  req.body.preferredCurrencyDealId,
  req.body.destinationBankCode,
  req.body.beneficiaryBankName,
  req.body.switcher,
  req.body.beneficiaryBankAddress1,
  req.body.beneficiaryBankAddress2,
  req.body.beneficiaryBankAddress3,
  req.body.valueDate,
  req.body.debitAmount,
  req.body.beneficiaryName,
  req.body.beneficiaryEmailAddress,
  req.body.beneficiaryAddress1,
  req.body.beneficiaryAddress2,
  req.body.beneficiaryAddress3,
  req.body.debitAccount2,
  req.body.debitAmount2,
  req.body.debitAccount3,
  req.body.debitAmount3,
  req.body.creditAccount2,
  req.body.creditAccount3,
  req.body.creditAccount4,
  req.body.instructionCode1,
  req.body.instructionRemark1,
  req.body.instructionCode2,
  req.body.instructionRemark2,
  req.body.instructionCode3,
  req.body.instructionRemark3,
  req.body.paymentRemark1,
  req.body.paymentRemark2,
  req.body.paymentRemark3,
  req.body.paymentRemark4,
  req.body.reservedAccount1,
  req.body.reservedAmount1,
  req.body.reservedAccount2,
  req.body.reservedAmount2,
  req.body.reservedAccount3,
  req.body.reservedAmount3,
  req.body.reservedField1,
  req.body.reservedField2,
  req.body.reservedField3,
  req.body.reservedField4,
  req.body.reservedField5,
  req.body.reservedField6,
  req.body.reservedField7,
  req.body.reservedField8,
  req.body.reservedField9,
  req.body.reservedField10,
  req.body.reservedField11,
  req.body.reservedField12,
  req.body.reservedField13,
  req.body.reservedField14,
  req.body.reservedField15,
  req.body.reservedField16,
  req.body.reservedField17,
  req.body.reservedField18,
  req.body.reservedField19,
  req.body.reservedField20
  ]

  await executeQuery('DELETE FROM paymenth2h."BOS_TRANSACTIONS_D" WHERE "customerReferenceNumber" = $1', [req.body.customerReferenceNumber])

  await connection.query('INSERT INTO paymenth2h."BOS_TRANSACTIONS_D" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72);', data, async function (error, result, fields) {
    if (error) {
      logger.error(error)
      data = {
        'result': "Failed",
        'msgCode': "1",
        'msgDscriptions': error.message
      }
      res.json(data);
    } else {
      data = {
        'result': "OK",
        'msgCode': "0",
        'msgDscriptions': "Success"
      }
      res.json(data);
      //await executeQuery('UPDATE  paymenth2h."BOS_TRANSACTIONS_D" SET "preferredTransferMethodId" = $1  WHERE "customerReferenceNumber" = $2', [req.body.preferredTransferMethodId, req.body.customerReferenceNumber])
    }
  });
}

exports.get_kodeDb = async function (req, res) {
  connection.query('select * from paymenth2h."BOS_DB_LIST" WHERE "NM_DB" = $1', [req.body.dbName], function (error, result, fields) {
    if (error) {
      logger.error(error)
      //pool.end()
    } else {
      // pool.end()
      rowdata = {};
      dbkode = "";
      for (var data of result.rows) {
        rowdata = {
          'dbName': data.KODE_DB,
          'clientId': data.ID_DB,
          'clientSecret': data.CLIENT_SECRET
        }

        dbkode = data.KODE_DB
        logger.info("Kode Database: ", dbkode)
      }
      res.json(rowdata);
    }
  });
  // pool.end()
}

exports.get_History = function (req, res) {
  logger.info("Get History, Ref: " + req.body.referency);


  connection.query('SELECT DISTINCT "PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", CAST("AMOUNT" AS DECIMAL(18,2)) "AMOUNT"' +
    ', TO_CHAR("TRANSDATE", \'YYYYMMDD\') "TRANSDATE", TO_CHAR("TRANSTIME", \'HH24MIss\') "TRANSTIME", "STATUS", "REASON", CAST("BANKCHARGE" AS DECIMAL(18,2)) "BANKCHARGE", "FLAGUDO"' +
    ', "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE"' +
    'FROM paymenth2h."BOS_LOG_TRANSACTIONS" WHERE "REFERENCY" = $1', [req.body.referency], async function (error, result, fields) {
      if (error) {
        logger.error(error)
        //pool.end()
      } else {
        rowdata = {};
        resultrow = []
        dbkode = "";
        for (var data of result.rows) {
          rowdata = {
            'paymentOutType': data.PAYMENTOUTTYPE,
            'paymentNo': data.PAYMENTNO,
            'trxId': data.TRXID,
            'referency': data.REFERENCY,
            'vendor': data.VENDOR,
            'account': data.ACCOUNT,
            'amount': data.AMOUNT,
            'transDate': data.TRANSDATE,
            'transTime': data.TRANSTIME,
            'status': data.STATUS,
            'reason': data.REASON,
            'bankCharge': data.BANKCHARGE,
            'flagUdo': data.FLAGUDO,
            'sourceAccount': data.SOURCEACCOUNT,
            'transferType': data.TRANSFERTYPE,
            'clientId': data.CLIENTID,
            'errorCode': data.ERRORCODE
          }

          // jika kalau ada time out maka update flag
          if (data.ERRORCODE == "EOD") {
            //await updateStatusCutOff(data.REFERENCY, data.DB)
          }


          resultrow.push(rowdata)
        }

        res.json(resultrow);
      }
    });
  // pool.end()
}

async function updateStatusCutOff(reference, dbName, outType) {
  var start = new Date();

  //let result = await executeQuery('SELECT COUNT(*) "rtoCount" FROM paymenth2h."BOS_LOG_TRANSACTIONS"  WHERE "REFERENCY" = $1 AND "ERRORCODE" = \'EOD\'', [reference])
  //logger.info(result)
  await getEntryUdoOut(reference, dbName, "", outType, false)
}

// function detail transaksi
exports.post_configDb = async function (req, res) {
  var start = new Date();
  var date = moment(start).format('YYYYMMDD')

  var dbName = req.body.dbName

  await connection.query('select COUNT(*) "dbCount" from paymenth2h."BOS_DB_LIST" WHERE "NM_DB" = $1', [dbName], function (error, result, fields) {
    if (error) {
      logger.error(error)
    } else {
      for (var data of result.rows) {
        if (data.dbCount == "0") {

        } else {
          // await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
          //                 'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, $1, $5, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $6' +
          //                 'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [4, req.body.referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), 'Maximum Resend', 'SAP-MR01'], async function (error, result, fields) {
          //                   if (error) {
          //                     logger.info(error)
          //                   }
          //                 });
        }
      }
    }
  });
}

// interfacing to mobopay
async function interfacing_mobopay_Transaction(referency, payment, outType, trxId) {

  // untk format tanggal
  var start = new Date();
  var date = moment(start).format("yyyy-MM-DDTHH:mm:ss.SSS") + "Z"

  try {
    // ambil data jika masih ada yg belom berhasil diinsterfacing
    let result = await executeQuery('SELECT "preferredTransferMethodId", "debitAccount", "creditAccount", "customerReferenceNumber", "chargingModelId"' +
      ', "defaultCurrencyCode", "debitCurrency", "creditCurrency", "chargesCurrency", "remark1", "remark2", "remark3", "remark4"' +
      ', "paymentMethod", "extendedPaymentDetail", "preferredCurrencyDealId", "destinationBankCode", "beneficiaryBankName"' +
      ', switcher, "beneficiaryBankAddress1", "beneficiaryBankAddress2", "beneficiaryBankAddress3", CASE WHEN "valueDate" < CURRENT_DATE THEN TO_CHAR(CURRENT_DATE, \'yyyy-MM-dd\') ELSE TO_CHAR("valueDate", \'yyyy-MM-dd\') END AS "valueDate"' +
      ', CAST("debitAmount" AS DECIMAL(18,2)) "debitAmount", "beneficiaryName", "beneficiaryEmailAddress", "beneficiaryAddress1", "beneficiaryAddress2", "beneficiaryAddress3"' +
      ', "debitAccount2", CAST("debitAmount2" AS DECIMAL(18,2)) "debitAmount2", "debitAccount3", CAST("debitAmount3" AS DECIMAL(18,2))"debitAmount3", "creditAccount2", "creditAccount3", "creditAccount4"' +
      ', "instructionCode1", "instructionRemark1", "instructionCode2", "instructionRemark2", "instructionCode3"' +
      ', "instructionRemark3", "paymentRemark1", "paymentRemark2", "paymentRemark3", "paymentRemark4"' +
      ', "reservedAccount1", CAST("reservedAmount1" AS DECIMAL(18,2)) "reservedAmount1", "reservedAccount2", CAST("reservedAmount2" AS DECIMAL(18,2)) "reservedAmount2", "reservedAccount3"' +
      ', CAST("reservedAmount3" AS DECIMAL(18,2)) "reservedAmount3", "reservedField1", "reservedField2", "reservedField3", "reservedField4", "reservedField5"' +
      ', "reservedField6", "reservedField7", "reservedField8", "reservedField9", "reservedField10", "reservedField11"' +
      ', "reservedField12", "reservedField13", "reservedField14", "reservedField15", "reservedField16", "reservedField17"' +
      ', "reservedField18", "reservedField19", "reservedField20"  , X2.* FROM paymenth2h."BOS_TRANSACTIONS_D" X0 ' +
      'INNER JOIN (SELECT "REFERENCY", "DB", "CLIENTID", "INTERFACING" ' +
      'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "STATUS" NOT IN (\'0\') GROUP BY "REFERENCY", "DB", "CLIENTID", "INTERFACING") X1 ON X0."customerReferenceNumber" = X1."REFERENCY"' +
      'INNER JOIN paymenth2h."BOS_DB_LIST" X2 ON X1."DB" = X2."NM_DB" AND X1."CLIENTID" = X2."ID_DB" ' +
      'WHERE X1."REFERENCY" = $1 ORDER BY X1."INTERFACING" ASC limit 1', [referency])

    if (result.rowCount > 0) {
      // deklarasi
      bodyPayment = {};
      _signature = "";
      _token = "";
      _base64key = "";
      _clientId = "";
      _bodyToken = "";

      for await (var data of result.rows) {

        _clientId = data.ID_DB
        _bodyToken = base64encode(data.CUST_KEY + ":" + data.CUST_SECRET);
        _token = await token(_bodyToken)
        _signature = await signature(data.ID_DB + data.CLIENT_SECRET + date + data.debitAccount + data.creditAccount + data.debitAmount + data.customerReferenceNumber)

        bodyPayment = {
          'preferredTransferMethodId': data.preferredTransferMethodId,
          'debitAccount': data.debitAccount,
          'creditAccount': data.creditAccount,
          'customerReferenceNumber': data.customerReferenceNumber,
          'chargingModelId': data.chargingModelId,
          'defaultCurrencyCode': data.defaultCurrencyCode,
          'debitCurrency': data.debitCurrency,
          'creditCurrency': data.creditCurrency,
          'chargesCurrency': data.chargesCurrency,
          'remark1': data.remark1,
          'remark2': data.remark2,
          'remark3': data.remark3,
          'remark4': data.remark4,
          'paymentMethod': data.paymentMethod,
          'extendedPaymentDetail': data.extendedPaymentDetail,
          'preferredCurrencyDealId': data.preferredCurrencyDealId,
          'destinationBankCode': data.destinationBankCode,
          'beneficiaryBankName': data.beneficiaryBankName,
          'switcher': data.switcher,
          'beneficiaryBankAddress1': data.beneficiaryBankAddress1,
          'beneficiaryBankAddress2': data.beneficiaryBankAddress2,
          'beneficiaryBankAddress3': data.beneficiaryBankAddress3,
          'valueDate': data.valueDate,
          'debitAmount': data.debitAmount,
          'beneficiaryName': data.beneficiaryName,
          'beneficiaryEmailAddress': data.beneficiaryEmailAddress,
          'beneficiaryAddress1': data.beneficiaryAddress1,
          'beneficiaryAddress2': data.beneficiaryAddress2,
          'beneficiaryAddress3': data.beneficiaryAddress3,
          'debitAccount2': data.debitAccount2,
          'debitAmount2': data.debitAmount2,
          'debitAccount3': data.debitAccount3,
          'debitAmount3': data.debitAmount3,
          'creditAccount2': data.creditAccount2,
          'creditAccount3': data.creditAccount3,
          'creditAccount4': data.creditAccount4,
          'instructionCode1': data.instructionCode1,
          'instructionRemark1': data.instructionRemark1,
          'instructionCode2': data.instructionCode2,
          'instructionRemark2': data.instructionRemark2,
          'instructionCode3': data.instructionCode3,
          'instructionRemark3': data.instructionRemark3,
          'paymentRemark1': data.paymentRemark1,
          'paymentRemark2': data.paymentRemark2,
          'paymentRemark3': data.paymentRemark3,
          'paymentRemark4': data.paymentRemark4,
          'reservedAccount1': data.reservedAccount1,
          'reservedAmount1': data.reservedAmount1,
          'reservedAccount2': data.reservedAccount2,
          'reservedAmount2': data.reservedAmount2,
          'reservedAccount3': data.reservedAccount3,
          'reservedAmount3': data.reservedAmount3,
          'reservedField1': data.reservedField1,
          'reservedField2': data.reservedField2,
          'reservedField3': data.reservedField3,
          'reservedField4': data.reservedField4,
          'reservedField5': data.reservedField5,
          'reservedField6': data.reservedField6,
          'reservedField7': data.reservedField7,
          'reservedField8': data.reservedField8,
          'reservedField9': data.reservedField9,
          'reservedField10': data.reservedField10,
          'reservedField11': data.reservedField11,
          'reservedField12': data.reservedField12,
          'reservedField13': data.reservedField13,
          'reservedField14': data.reservedField14,
          'reservedField15': data.reservedField15,
          'reservedField16': data.reservedField16,
          'reservedField17': data.reservedField17,
          'reservedField18': data.reservedField18,
          'reservedField19': data.reservedField19,
          'reservedField20': data.reservedField20
        }
        logger.info("Body Payment -> Mobopay ", bodyPayment)
        await prosesInterfacing(bodyPayment, _token, _clientId, date, _signature, data.customerReferenceNumber, data.NM_DB, payment, outType, trxId)
      }
    }
  } catch (error) {
    logger.error(error)
  }

}

async function prosesInterfacing(body, token, clientId, date, signature, referency, dbName, payment, outType, trxId) {

  var start = new Date();
  var myHeaders = new nodeFetch.Headers();

  myHeaders.append("Authorization", "Bearer " + token);
  myHeaders.append("X-Client-Id", clientId);
  myHeaders.append("X-Timestamp", date);
  myHeaders.append("X-Signature", signature);
  myHeaders.append("Content-Type", "application/json");
  myHeaders.append("X-Action", payment);

  var raw = JSON.stringify(body);

  //logger.info(raw)
  var requestOptions = {
    method: 'POST',
    headers: myHeaders,
    body: raw,
    redirect: 'follow'
  };


  const fetchWithTimeout = (input, init, timeout) => {
    const controller = new AbortController();
    setTimeout(() => {
      controller.abort();
    }, timeout)

    return fetch(input, { signal: controller.signal, ...init });
  }

  const wait = (timeout) =>
    new Promise((resolve) => {
      setTimeout(() => resolve(), timeout);
    })

  const fetchWithRetry = async (input, init, timeout, retries) => {
    let increseTimeOut = config.interval_retries;
    let count = retries;

    while (count > 0) {
      try {
        if (count == 3) {
          // untuk pengiriman pertama kali
        } else {
          myHeaders.set("X-Action", payment); // jika resend payment
        }
        //logger.info(myHeaders)
        return await fetchWithTimeout(input, init, timeout);
      } catch (e) {
        if (e.name !== "AbortError") throw e;
        count--;
        logger.error(
          `fetch failed Cusreff: ${referency}, retrying in ${increseTimeOut}s, ${count} retries left`
        );

        if (count > 0) {
          // jika count sama dengan 0, maka payment akan menjadi failed
          //UpdateTableRetry(referency, "3", "Resend Payment")
          await wait(increseTimeOut)
          timedelay += increseTimeOut;
        } else if (count == 0) {
          await updateTableRetry(referency, "4", "Resend Payment")
          getEntryUdoOut(referency, dbName, trxId, outType, false) // jika mencapai maksimum retry, maka update detail UDO menjadi failed
        }
      }
    }
  }
  logger.info("(OUT) Middleware - Mobopay (" + dbName + "): Do Payment : " + config.base_url_payment + "/mobopay/mandiri/payment/do-payment")
  await fetchWithRetry(config.base_url_payment + "/mobopay/mandiri/payment/do-payment", requestOptions, config.timeOut, config.max_retries)
    .then(response => response.text())
    .then(async result => {
      const resultBody = JSON.parse(result)

      logger.info(result)

      if (resultBody.resultCode == "0") {

        // cek jika error code sudah ada
        logger.info("(IN) Mobopay - Middleware  (" + dbName + "): Do Payment Error : Payment Failed: " + referency + " , Error Code: " + resultBody.errorCode + " Msg Error: " + resultBody.message)
        //console.warn("Payment Failed: " + referency + " , Error Code: " + resultBody.errorCode + " Msg Error: " + resultBody.message)
        await executeQuery('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [referency])


        await getEntryUdoOut(referency, dbName, trxId, outType, true)
        await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
          'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 4, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID",  $5' +
          'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [resultBody.message, referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), resultBody.errorCode], async function (error, result, fields) {
            if (error) {
              logger.error(error)
            }
          });


      }
      else if (resultBody.resultCode == "1") {
        // JIKA TIDAK ERROR MAKA INSERT KE LOG

        await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID")' +
          'SELECT "PAYMENTOUTTYPE", "PAYMENTNO", $1, "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 2, $5, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID"' +
          'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [resultBody.data.trxId, referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), resultBody.message], async function (error, result, fields) {
            if (error) {
              logger.error(error)
            }
          });
        await getEntryUdoOut(referency, dbName, resultBody.data.trxId, outType, false)
        await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'Y\', "TRXID" = $2 WHERE "REFERENCY" = $1', [referency, resultBody.data.trxId])

      }
    })
    .catch(async err => {
      logger.error(err)
    }
    )
    .finally(() => {
      clearTimeout(timeout);
    });
}

async function updateTableRetry(reference, status, reason) {
  var start = new Date();
  //logger.info(reference + status + reason)
  // CEK JIKA RTO nya sudah lebih dari 3
  let result = await executeQuery('SELECT COUNT(*) "rtoCount" FROM paymenth2h."BOS_LOG_TRANSACTIONS"  WHERE "REFERENCY" = $1 AND "ERRORCODE" = \'RTO\'', [reference])
  //logger.info(result)

  if (result.rowCount > 0) {
    for (var data of result.rows) {
      if (data.rtoCount < config.max_retries) {
        await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
          'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, $1, $6, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
          'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [status, reference, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), "RTO", reason], async function (error, result, fields) {
            if (error) {
              logger.error(error)
            } else {
              if (status == 4) {
                await executeQuery('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [reference])
              }
            }

          });
      }
    }
  }
}

async function getEntryUdoOut(referency, dbName, trxId, outType, error) {
  // update ke UDO jika sudah mendapatakan trxid
  var start = new Date()
  //logger.info("Mencari DocEntry UDO, Payment Out Type: " + outType + "Ref: " + referency)

  var refr = []
  refr = referency.split('/')
  var DocNum = refr[1] // untuk mendapatakan nomor dokumen
  var Lines = refr[2].slice(-2)
  var resultLines = refr[2].substring(0, Lines) // hasil dari lines
  //logger.info("Line Out: " + resultLines)
  var EntryOutStatus = ""
  var resultLinesStatus = ""

  // mencari docentry dari payout
  logger.info("(OUT) Middleware - SAP  (" + dbName + "): GET Dockey UDO " + config.base_url_xsjs + "/PaymentService/getEntryOut.xsjs?docnum=" + referency + "&dbName=" + dbName)
  await fetch(config.base_url_xsjs + "/PaymentService/getEntryOut.xsjs?docnum=" + referency + "&dbName=" + dbName, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Basic ' + config.auth_basic,
      'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
    },
    'maxRedirects': 20
  })
    .then(res => res.json())
    .then(async data => {
      var Entry = ""
      logger.info("(IN) Middleware - SAP  (" + dbName + "): GET Dockey UDO Result ", data)
      await Object.keys(data.ENTRY).forEach(async function (key) {

        if (data.ENTRY[key].OUTYPE == 'OUT') {
          await updateUdoTrxId(data.ENTRY[key].DocEntry, data.ENTRY[key].LineId, trxId, 'Basic ' + config.auth_basic, dbName, referency, data.ENTRY[key].OUTYPE, error)
        } else {
          await updateUdoTrxId(data.ENTRY[key].DocEntry, data.ENTRY[key].LineId, trxId, 'Basic ' + config.auth_basic, dbName, referency, data.ENTRY[key].OUTYPE, error)
        }
      })

      let result = executeQuery('SELECT COUNT(*) "FLAG" FROM paymenth2h."BOS_TRANSACTIONS" WHERE "FLAGUDO" = \'1\'  AND "REFERENCY" = $1', [referency]);

    })
    .catch(err => {
      logger.error("Get Error: " + err.message)
    });
}

async function getLinesOutStatus(referency, Entry, dbName) {

  var Lines = ""
  logger.info("get line out status......: " + referency)
  await fetch(config.base_url_xsjs + "/PaymentService/getEntryOutStatus.xsjs?reference=" + referency + "&Entry=" + Entry + "&dbName=" + dbName, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Basic ' + config.auth_basic,
      'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
    },
    'maxRedirects': 20
  })
    .then(res => res.json())
    .then(async data => {
      var Entry = ""
      logger.info(data)
      await Object.keys(data.LINESOUTSTATUS).forEach(function (key) {
        Lines = data.LINESOUTSTATUS[key].LineId
      })
    })
    .catch(err => {
      logger.info("Get Error: " + err.message)
    });

  return Lines
}

async function updateUdoTrxId(DocEntry, Lines, trxid, auth, dbName, referency, outType, error) {

  // mendapatakan detail dari pym out
  await jobs.stop();
  let login = await LoginV2(dbName);


  if (outType == "OUT") {
    details = []
    //logger.info("Line Out: " + Lines + ", Entry: " + DocEntry)
    logger.info("(OUT) Mobopay - SAP  (" + dbName + "): GET Lines Detail UDO Payment Out" + config.base_url_xsjs + "/PaymentService/get_lines_out.xsjs?Entry=" + DocEntry + "&LineId=" + Lines + "&dbName=" + dbName)
    await fetch(config.base_url_xsjs + "/PaymentService/get_lines_out.xsjs?Entry=" + DocEntry + "&LineId=" + Lines + "&dbName=" + dbName, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': auth,
        'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
      },
      'maxRedirects': 20
    })
      .then(res => res.json())
      .then( data => {
         Object.keys(data.LINES).forEach(function (key) {
          var d = data.LINES[key];

          if (trxid == "") {
            var row = {
              'LineId': parseFloat(Lines),
              'U_STATUS': 'Failed',
              'U_PROSES_TYPE': d.ProcessType,
              'U_ACCT_PYM_REQ': d.PaymentMeans,
              'U_BANK_PYM_REQ': d.BankPaymentMeans,
              'U_SOURCEACCT': d.SourcePayment,
              'U_CUST_REFF': referency,
              'U_TRXID_SAP': trxid,
            }
            details.push(row)
          } else {
            if (error == true) {
              var row = {
                'LineId': parseFloat(Lines),
                'U_TRXID_SAP': trxid,
                'U_STATUS': 'Failed',
                'U_PROSES_TYPE': d.ProcessType,
                'U_ACCT_PYM_REQ': d.PaymentMeans,
                'U_BANK_PYM_REQ': d.BankPaymentMeans,
                'U_SOURCEACCT': d.SourcePayment,
                'U_CUST_REFF': referency
              }
              details.push(row)
            } else {
              var row = {
                'LineId': parseFloat(Lines),
                'U_TRXID_SAP': trxid,
                'U_PROSES_TYPE': d.ProcessType,
                'U_ACCT_PYM_REQ': d.PaymentMeans,
                'U_BANK_PYM_REQ': d.BankPaymentMeans,
                'U_SOURCEACCT': d.SourcePayment,
                'U_CUST_REFF': referency
              }
              details.push(row)
            }
          }
        })
        logger.info("Middleware  (" + dbName + "): Details Payment Out", details)

        // login ke sap

        if (login != "") {
          // update ke UDO
          logger.info("Update UDO Payment Out, Ref: " + referency)

          var myHeaders = new nodeFetch.Headers();
          myHeaders.append("Content-Type", "application/json");

          var requestOptions = {
            method: 'PATCH',
            headers: myHeaders,
            body: JSON.stringify({ BOS_PYM_OUT1Collection: details }),
            redirect: 'follow'
          };

          const fetchWithTimeout = (input, init, timeout) => {
            const controller = new AbortController();
            setTimeout(() => {
              controller.abort();
            }, timeout)

            return fetch(input, { signal: controller.signal, ...init });
          }

          const wait = (timeout) =>
            new Promise((resolve) => {
              setTimeout(() => resolve(), timeout);
            })

          const fetchWithRetry = async (input, init, timeout, retries) => {
            let increseTimeOut = config.interval_retries;
            let count = retries;

            while (count > 0) {
              try {
                return await fetchWithTimeout(input, init, timeout);
              } catch (e) {
                logger.info(e.name)
                if (e.name !== "AbortError") throw e;
                count--;
                logger.error(
                  `fetch patch, retrying patch in ${increseTimeOut}s, ${count} retries left`
                );
                await wait(increseTimeOut)
              }
            }
          }
          var Session = ""
          logger.info("(OUT) Mobopay - SAP  (" + dbName + "): Update Flag UDO Payment Out" + config.sl_realPort + "/b1s/v2/PYM_OUT(" + parseFloat(DocEntry) + ")")

           fetchWithRetry(config.sl_realPort + "/b1s/v2/PYM_OUT(" + parseFloat(DocEntry) + ")", requestOptions, 60000, 3)
            .then(response => {
              if (response.status == 204) {
                //logger.info(result)
                executeQuery('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "FLAGUDO" = \'1\' WHERE "REFERENCY" = $1', [referency], async function (error, result, fields) {
                  if (error) {
                    logger.error(error)
                  }
                });
              }
              response.text()
            }).then(async result => {
            }).catch(error => logger.error('error', error));
        }
      })
      .catch(err => {
        logger.error("Get Error: " + err.message)
      });
  } else {
    details = []
    //logger.info("Line Out Status: " + Lines + ", Entry: " + DocEntry)
    logger.info("(OUT) Mobopay - SAP  (" + dbName + "): GET Lines Detail UDO Payment Out Status" + config.base_url_xsjs + "/PaymentService/get_line_outstatus.xsjs?Entry=" + DocEntry + "&LineId=" + Lines + "&dbName=" + dbName)


    await fetch(config.base_url_xsjs + "/PaymentService/get_line_outstatus.xsjs?Entry=" + DocEntry + "&LineId=" + Lines + "&dbName=" + dbName, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': auth,
        'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
      },
      'maxRedirects': 20
    })
      .then(res => res.json())
      .then(data => {
        Object.keys(data.LINES).forEach(function (key) {
          var d = data.LINES[key];

          if (trxid == "") {
            var row = {
              'LineId': parseFloat(Lines),
              'U_STATUS': 'Failed',
              'U_PROSES_TYPE': d.ProcessType,
              'U_ACCT_PYM_REQ': d.PaymentMeans,
              'U_BANK_PYM_REQ': d.BankPaymentMeans,
              'U_SOURCEACCT': d.SourcePayment,
              'U_CUST_REFF': referency,
              'U_TRXID_SAP': trxid,
            }
            details.push(row)
          } else {
            if (error == true) {
              var row = {
                'LineId': parseFloat(Lines),
                'U_STATUS': 'Failed',
                'U_TRXID_SAP': trxid,
                'U_PROSES_TYPE': d.ProcessType,
                'U_ACCT_PYM_REQ': d.PaymentMeans,
                'U_BANK_PYM_REQ': d.BankPaymentMeans,
                'U_SOURCEACCT': d.SourcePayment,
                'U_CUST_REFF': referency
              }
              details.push(row)
            } else {
              var row = {
                'LineId': parseFloat(Lines),
                'U_TRXID_SAP': trxid,
                'U_PROSES_TYPE': d.ProcessType,
                'U_ACCT_PYM_REQ': d.PaymentMeans,
                'U_BANK_PYM_REQ': d.BankPaymentMeans,
                'U_SOURCEACCT': d.SourcePayment,
                'U_CUST_REFF': referency
              }
              details.push(row)
            }
          }
        })
        logger.info("Middleware  (" + dbName + "): Details Payment Out Status", details)


        // login ke sap
        //let login = await LoginV2(dbName);
        if (login != "") {
          // update ke UDO 
          logger.info("Update UDO Payment Out Status, Reff:" + referency)

          var myHeaders = new nodeFetch.Headers();
          myHeaders.append("Content-Type", "application/json");

          var requestOptions = {
            method: 'PATCH',
            headers: myHeaders,
            body: JSON.stringify({ BOS_PYMOUT_STATUS1Collection: details }),
            redirect: 'follow'
          };

          const fetchWithTimeout = (input, init, timeout) => {
            const controller = new AbortController();
            setTimeout(() => {
              controller.abort();
            }, timeout)

            return fetch(input, { signal: controller.signal, ...init });
          }

          const wait = (timeout) =>
            new Promise((resolve) => {
              setTimeout(() => resolve(), timeout);
            })

          const fetchWithRetry = async (input, init, timeout, retries) => {
            let increseTimeOut = config.interval_retries;
            let count = retries;

            while (count > 0) {
              try {
                return await fetchWithTimeout(input, init, timeout);
              } catch (e) {
                logger.info(e.name)
                if (e.name !== "AbortError") throw e;
                count--;
                logger.error(
                  `fetch patch, retrying patch in ${increseTimeOut}s, ${count} retries left`
                );
                await wait(increseTimeOut)
              }
            }
          }
          var Session = ""
          logger.info("(OUT) Mobopay - SAP  (" + dbName + "): Update Flag UDO Payment Out Status" + config.sl_realPort + "/b1s/v2/PYMOUT_STATUS(" + parseFloat(DocEntry) + ")")
           fetchWithRetry(config.sl_realPort + "/b1s/v2/PYMOUT_STATUS(" + parseFloat(DocEntry) + ")", requestOptions, 60000, 3)
            .then(response => {
              if (response.status == 204) {
                logger.info(result)
                executeQuery('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "FLAGUDO" = \'1\' WHERE "REFERENCY" = $1', [referency], async function (error, result, fields) {
                  if (error) {
                    logger.error(error)
                  }
                });
              }
              response.text()
            }).then(async result => {
              logger.info("(OUT) Mobopay - SAP  (" + dbName + "): Update Flag UDO Payment Out Status", result)
            }).catch(error => logger.error('error', error));
        }
      })
      .catch(err => {
        logger.error("Get Error: " + err.message)

      });

  }

  await jobs.start();
}

async function token(base64Key) {
  let stringToken
  var myHeaders = new nodeFetch.Headers();
  myHeaders.append("Authorization", "Basic " + base64Key);

  var urlencoded = new URLSearchParams();

  var requestOptions = {
    method: 'POST',
    headers: myHeaders,
    body: urlencoded,
    redirect: 'follow'
  };

  await fetch(config.base_url_mobopay + "/token?grant_type=client_credentials", requestOptions)
    .then(response => response.text())
    .then(result => {
      const resultToken = JSON.parse(result)
      stringToken = resultToken.access_token
    }).catch(error => logger.info('error', error));

  return stringToken;
}

function signature(message) {
  let _signature
  var Response_ = generateMessageBodySignature(message, privateKey)
  timeout = true;
  _signature = Response_

  return _signature
}

exports.post_pdateFlag = function (req, res) {
  connection.query('CALL paymenth2h.Update_Flag($1)', [req.body.trxId], async function (error, result, fields) {
    if (error) {
      logger.info(error)
      //pool.end()
    } else {
      // pool.end()
      rowdata = {};
      dbkode = "";
      for (var data of result.rows) {
        rowdata = {
          'dbName': data.KODE_DB,
          'clientId': data.ID_DB,
          'clientSecret': data.CLIENT_SECRET
        }

        dbkode = data.KODE_DB
      }
      logger.info("Kode Database: " + dbkode)
      res.json(rowdata);
    }
  });
  // pool.end()
}