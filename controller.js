const nodeFetch = require('node-fetch')
const fetch = require('fetch-cookie')(nodeFetch);
const connection = require('./conn');
const connection1 = require('./ClientConn');
const base64 = require('base-64');
const cron = require('node-cron');
const crypto = require('crypto');
var https = require('follow-redirects').https;
var fs = require('fs');
const privateKey = fs.readFileSync('./pancaran-payment-gateway.key');
var jwt = require('jsonwebtoken');
var moment = require('moment'); // untuk definisi tanggal
const config = require('./config');
const { Pool, Client } = require('pg') // untuk postgresql
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
const internal = require('stream');
const { setInterval } = require('timers');
const { Logger } = require('winston');
const { Promise } = require('node-fetch');
const start = new Date();
const CronJob = require('cron').CronJob;
var nodeBase64 = require('nodejs-base64-converter');
// create pool

// SSL (Only if needed)
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0


// PostgreSql Query Adapter
async function executeQuery(query, queryparams) {
  return new Promise((resolve, reject) => {
    connection.query(query, queryparams,
      function (error, rows, fields) {
        if (error) {
          logger.info(moment().format('YYYY-MM-DD h:mm:ss a') + ": ");
          logger.info("Postgree Query failed");
          logger.error(error);
          reject(error);
        } else {
          resolve(rows);
        }
      });
    if (connection) {
      connection.end();
    };
  })
}


async function resendPaymet() {

  logger.info('Resend Paymet.............................');
  let result = await connection.query('SELECT "REFERENCY", "TRANSFERTYPE", TO_CHAR("TRANSTIME", \'HHMI\') "TRANSTIME", "DB" FROM paymenth2h."BOS_TRANSACTIONS" ' +
    'WHERE "TRXID" NOT IN (SELECT "trxId" FROM paymenth2h."BOS_DO_STATUS") ' +
    'AND "TRXID" != \'\' AND "REFERENCY" =  \'OUT/20000412/101\'' +
    'ORDER BY "REFERENCY"')
  return result;

}


const checkCuttOff = (dbName, chModelId, time_cutoff, auth) => {
  return new Promise((resolve, reject) => {
    fetch(config.base_url_xsjs + "/PaymentService/get_CutOff.xsjs?dbName=" + dbName + "&chmodelid=" + chModelId + "&time_cutoff=" + time_cutoff, {
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
          resolve(d.INCUTOFF);
        })
      }).catch(err => {
        resolve(err.message)
      });
  });
}

async function getCutOff(dbName, chModelId, time_cutoff, auth) {

  var cutfOff = '';

  //logger.info("CuttOff :  " + config.base_url_xsjs + "/PaymentService/get_CutOff.xsjs?dbName=" + dbName + "&chmodelid=" + chModelId + "&time_cutoff=" + time_cutoff);
  fetch(config.base_url_xsjs + "/PaymentService/get_CutOff.xsjs?dbName=" + dbName + "&chmodelid=" + chModelId + "&time_cutoff=" + time_cutoff, {
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
const running = false;


var resultRowCount = 0
var typePayment = '';
async function main() {
  let d = "";
  var _time = "";

  try {
    let result = await connection.query('SELECT DISTINCT T0.* FROM (SELECT ROW_NUMBER () OVER ( ' +
      'PARTITION BY X0."REFERENCY" ' +
      'ORDER BY X0."ACTION") "ROWNUMBER",  * FROM( ' +
      'SELECT  X0."REFERENCY", X0."TRANSFERTYPE", TO_CHAR(X0."TRANSTIME", \'HHMI\') "TRANSTIME", EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - (X0."TRANSDATE" + X0."TRANSTIME"))) / 60 AS "INTERVAL"  ' +
      ', X0."DB", CASE WHEN X1."TRXID" = \'\' THEN \'PAYMENT\' ELSE \'RESEND_PAYMENT\' END AS "ACTION", X0."CUTOFF", X0."PAYMENTNO"  ' +
      ', "PAYMENTOUTTYPE", COALESCE(X1."TRXID" , \'\') "TRXID" ' +
      'FROM paymenth2h."BOS_TRANSACTIONS" X0  ' +
      'INNER JOIN (  ' +
      'SELECT "REFERENCY", COALESCE("TRXID", \'0\') "TRXID" FROM paymenth2h."BOS_LOG_TRANSACTIONS" ' +
      'GROUP BY "REFERENCY", "TRXID") X1 ON X0."REFERENCY" = X1."REFERENCY" ' +
      'WHERE X0."INTERFACING" IN (\'0\') ' +
      'GROUP BY X0."REFERENCY", X0."TRANSFERTYPE",X0."TRANSDATE", X0."TRANSTIME", X0."DB", X0."ACTION", X0."PAYMENTOUTTYPE", X1."TRXID" , X0."CUTOFF", X0."PAYMENTNO"' +
      'ORDER BY X0."REFERENCY" ASC) X0) T0 ' +
      'INNER JOIN (SELECT MAX(X0."ROWNUMBER") "MAXNUMBER", X0."REFERENCY" FROM (SELECT ROW_NUMBER () OVER ( ' +
      'PARTITION BY  X0."REFERENCY" ' +
      'ORDER BY X0."ACTION") "ROWNUMBER",  * FROM( ' +
      'SELECT  X0."REFERENCY", X0."TRANSFERTYPE", TO_CHAR(X0."TRANSTIME", \'HHMI\') "TRANSTIME", EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - (X0."TRANSDATE" + X0."TRANSTIME"))) / 60 AS "INTERVAL"  ' +
      ', X0."DB", CASE WHEN X1."TRXID" = \'\' THEN \'PAYMENT\' ELSE \'RESEND_PAYMENT\' END AS "ACTION", X0."CUTOFF"  ' +
      ', "PAYMENTOUTTYPE", COALESCE(X1."TRXID" , \'\') "TRXID" ' +
      'FROM paymenth2h."BOS_TRANSACTIONS" X0  ' +
      'INNER JOIN (  ' +
      'SELECT "REFERENCY", COALESCE("TRXID", \'0\') "TRXID" FROM paymenth2h."BOS_LOG_TRANSACTIONS" ' +
      'GROUP BY "REFERENCY", "TRXID") X1 ON X0."REFERENCY" = X1."REFERENCY" ' +
      'WHERE X0."INTERFACING" IN (\'0\') ' +
      'GROUP BY X0."REFERENCY", X0."TRANSFERTYPE",X0."TRANSDATE", X0."TRANSTIME", X0."DB", X0."ACTION", X0."PAYMENTOUTTYPE", X1."TRXID", X0."CUTOFF" ' +
      'ORDER BY X0."TRANSDATE", X0."TRANSTIME" ASC) X0 ' +
      'ORDER BY X0."REFERENCY") X0 ' +
      'GROUP BY X0."REFERENCY") T1 ON T0."ROWNUMBER" = T1."MAXNUMBER" AND T0."REFERENCY" = T1."REFERENCY"')

    if (result.rowCount > 0) {
      jobs.stop();
    }

    const timer = ms => new Promise(res => setTimeout(res, ms))
    for await (let store of result.rows) {
      resultRowCount++;
      d = new Date();
      _time = moment(d).format("HHmm")

      // check cut off
      var CutOff = await checkCuttOff(store.DB, store.TRANSFERTYPE, _time, "Basic " + config.auth_basic)
      logger.debug(store.REFERENCY + ", CutOff : " + CutOff);
      
      if (CutOff == "FALSE") { // jika tidak cut off maka transaksi akan dikirim ke mobopay
        // body payment
        typePayment = store.PAYMENTOUTTYPE;
        var BodyJson = await interfacing_mobopay_Transaction(store.REFERENCY, store.ACTION, store.PAYMENTOUTTYPE, store.trxId, store.DB)
        logger.debug(store.REFERENCY + ", BodyJson : " + BodyJson);
        
        if (Object.keys(BodyJson).length > 0) {

          // validasi amount
         var valAmount = await getValidationAmount(store.REFERENCY, store.DB, "Basic " + config.auth_basic)
          logger.debug(parseFloat(valAmount.Amount) + " - " + parseFloat(BodyJson.Amount))
    
          // Edited by Musthofa
          // untuk menjadikan failed
          // parseFloat(valAmount.Amount) === parseFloat(BodyJson.Amount)
          
          if (parseFloat(valAmount.Amount) === parseFloat(BodyJson.Amount)) {

            // proses payment ke mobopay
            var interfacing = await prosesInterfacing(BodyJson.BodyPayment, BodyJson.Token, BodyJson.ClientId,
              BodyJson.Date, BodyJson.Signature, store.REFERENCY, BodyJson.DbName, store.ACTION, store.PAYMENTOUTTYPE, store.trxId)

            if (interfacing.success === "success") { // jika proses ke mobopay sukses
              // var getEntry = await getEntryUdo(store.REFERENCY, BodyJson.DbName, interfacing.trxid, store.PAYMENTOUTTYPE, "N")
              // if (getEntry.length > 0) {
              //   let Login = await LoginSL(store.DB)
              //   if (Login !== "LoginError") {
              //     let updateUdo
              //     Object.keys(getEntry).forEach(async function (key) {
              //       if (getEntry[key].DocEntry != "") {
              //         updateUdo = await updateLineUdo(getEntry[key].DocEntry, getEntry[key].LineId, getEntry[key].TrxId, getEntry[key].Auth
              //           , getEntry[key].DbName, getEntry[key].Referency, getEntry[key].OutType, "N", store.PAYMENTNO)

              //         if (updateUdo === "success") {

              //         } else {
              //           logger.error("Error");
              //         }
              //       }
              //     })

              //   } else {
              //     logger.error(Login);
              //   }
              // } else {
              //   logger.error("Get DocEntry Error (" + store.DB + "): " + store.REFERENCY);

              // }

              logger.info("Transaction (" + store.DB + "): " + store.REFERENCY + " Has Finished");
            } else { // jika proses ke mobopay gagal
              var getEntry = await getEntryUdo(store.REFERENCY, store.DB, store.trxId, store.PAYMENTOUTTYPE, "Y")
              if (getEntry.length > 0) {
                let Login = await LoginSL(store.DB)
                if (Login !== "LoginError") {
                  logger.debug("Payment No :" + store.PAYMENTNO)

                  let updateUdo
                  Object.keys(getEntry).forEach(async function (key) {

                    if (getEntry[key].DocEntry != "") {
                      updateUdo = await updateLineUdo(getEntry[key].DocEntry, getEntry[key].LineId, getEntry[key].TrxId, getEntry[key].Auth
                        , getEntry[key].DbName, getEntry[key].Referency, getEntry[key].OutType, "Y", store.PAYMENTNO, store.PAYMENTOUTTYPE)

                      if (updateUdo === "success") {
                        logger.info("Transaction (" + store.DB + "): " + store.REFERENCY + " Has Finished");
                      } else {
                        logger.error("Error");
                      }
                    }

                  })


                } else {
                  logger.error(Login);
                }
              } else {
                logger.error("Get DocEntry Error (" + store.DB + "): " + store.REFERENCY);
              }
            }
          } else {
            logger.info('Payment (' + store.DB + ') Interfacing, Ref: ' + store.REFERENCY + ', Validation Amount Failed');

            // update interfacingnya mendjadi 1
            await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\' WHERE "REFERENCY" = $1', [store.REFERENCY])

            // insert ke log, jika nilai tidak sama
            await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
              'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 4, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID",  $5' +
              'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', ["SAP - Validation Amount Failed", store.REFERENCY, moment(d).format("yyyyMMDD"), moment(d).format("HH:mm:ss"), "EOD"]);


            // update status di payment
            var getEntry = await getEntryUdo(store.REFERENCY, store.DB, "", store.PAYMENTOUTTYPE, "Y")

            if (getEntry.length > 0) {
              let Login = await LoginSL(store.DB)
              if (Login !== "LoginError") {
                logger.debug("Payment No :" + store.PAYMENTNO)
                let updateUdo
                Object.keys(getEntry).forEach(async function (key) {
                  if (getEntry[key].DocEntry != "") {
                    updateUdo = await updateLineUdo(getEntry[key].DocEntry, getEntry[key].LineId, getEntry[key].TrxId, getEntry[key].Auth
                      , getEntry[key].DbName, getEntry[key].Referency, getEntry[key].OutType, "Y", store.PAYMENTNO, store.PAYMENTOUTTYPE)
                  }

                })
              } else {
                logger.error(Login);
              }
            } else {
              logger.error("Get DocEntry Error (" + store.DB + "): " + store.REFERENCY);
            }
          }
        }
        // connection.query('SELECT Count(*) "Log" FROM paymenth2h."BOS_LOG_TRANSACTIONS" WHERE "REFERENCY" = $1 AND "TRXID" != \'\'', [store.REFERENCY], async function (error, result, fields) {
        //   if (error) {
        //     logger.error(error)
        //   } else {
        //     for await (var data of result.rows) {
        //       if (parseFloat(data.Log) < 1) {

        //       } else {
        //         await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\' WHERE "REFERENCY" = $1', [store.REFERENCY])
        //       }
        //     }
        //   }
        // });

      } else if (CutOff === "TRUE") {
        logger.info('Payment (' + store.DB + ') Interfacing, Ref: ' + store.REFERENCY + ', Cut off:' + CutOff);
        let udpate = await updateCufOff1(store.REFERENCY, store.DB, store.PAYMENTOUTTYPE, store.trxId, store.PAYMENTNO)
        logger.info(udpate)
      }
      await timer(5000)
    };

    if (result.rowCount === resultRowCount) {
      logger.info("Interfacing Finished")
      resultRowCount = 0
    }

    jobs.start();

  } catch (error) {
    logger.error("Interfacing Error: " + error)
    resultRowCount = 0
    jobs.start();
  }
}


const jobs = new CronJob({
  cronTime: '*/5 * * * * *',
  onTick: async () => {
    try {
      main()
    } catch (err) {
      resultRowCount = 0
      jobs.start();
    }

  },
  start: true,
  timeZone: 'UTC'
})


const getEntryUdo = async (referency, dbName, trxId, outType, error) => {
  return new Promise((resolve, reject) => {

    var start = new Date();
    logger.info("(OUT) Middleware - SAP  (" + dbName + "): GET Dockey UDO " + config.base_url_xsjs + "/PaymentService/getEntryOut.xsjs?docnum=" + referency + "&dbName=" + dbName)
    fetch(config.base_url_xsjs + "/PaymentService/getEntryOut.xsjs?docnum=" + referency + "&dbName=" + dbName, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Basic ' + config.auth_basic,
        'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
      },
      'maxRedirects': 20
    })
      .then(res => res.json())
      .then(entries => {
        logger.info("(IN) Middleware - SAP  (" + dbName + "): GET Dockey UDO Result ", entries)
        var resulTjSON = []
        var data = {}
        Object.keys(entries.ENTRY).forEach(async function (key) {

          if (entries.ENTRY[key].OUTYPE === outType) {
            connection.query('INSERT INTO paymenth2h."BOS_UDO_STATUS_UPDATE" VALUES ($1, $2, $3, $4, $5, $6)'
              , [outType, entries.ENTRY[key].DocEntry, dbName, 'N', moment(start).format("yyyyMMDD"), referency], function (error1, result, fields) {
                if (error1) {
                  logger.info(error1)
                }
              });

            resulTjSON.push({
              DocEntry: entries.ENTRY[key].DocEntry,
              LineId: entries.ENTRY[key].LineId,
              TrxId: trxId,
              Auth: 'Basic ' + config.auth_basic,
              DbName: dbName,
              Referency: referency,
              OutType: entries.ENTRY[key].OUTYPE,
              HasError: error
            })
          }

        })

        resolve(resulTjSON)

      })
      .catch(err => {
        logger.error("Get Error: " + err.message)
      });

  });
};


const updateCufOff1 = async (reference, db, outType, trxId, docnumOut) => {
  return new Promise((resolve, reject) => {
    var start = new Date();
    try {
      connection.query('SELECT "CUTOFF" FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1', [reference], async function (error, result, fields) {
        if (error) {
          logger.error(error)
        } else {
          for (var data of result.rows) {
            if (data.CUTOFF == "N" || data.CUTOFF == null) {
              var getEntry = await getEntryUdo(reference, db, trxId, outType, "Y")
              if (getEntry.length > 0) {
                let Login = await LoginSL(db)
                if (Login !== "LoginError") {
                  let updateUdo
                  await Object.keys(getEntry).forEach(async function (key) {
                    if (getEntry[key].DocEntry != "") {
                      updateUdo = await updateLineUdo(getEntry[key].DocEntry, getEntry[key].LineId, getEntry[key].TrxId, getEntry[key].Auth
                        , getEntry[key].DbName, getEntry[key].Referency, getEntry[key].OutType, "Y", docnumOut, getEntry[key].OutType)
                      if (getEntry[key].OutType === "OUTSTATUS") {
                        console.log("update DocEntry")
                        var getEntryOut = await getEntryUdo(reference, db, "", 'OUT', "Y")
                        await Object.keys(getEntryOut).forEach(async function (key) {
                          updateUdo = await updateLineUdo(getEntryOut[key].DocEntry, getEntryOut[key].LineId, getEntryOut[key].TrxId, getEntryOut[key].Auth
                            , getEntryOut[key].DbName, getEntryOut[key].Referency, getEntryOut[key].OutType, "Y", docnumOut, "OUTSTATUS");

                        });
                      }

                      if (updateUdo === "success") {
                        await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "CUTOFF" = \'Y\', "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [reference])

                        // insert ke log jika terkena cutoff
                        await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
                          'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 4, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID",  $5' +
                          'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', ["Transaction at cutoff time", reference, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), "EOD"], async function (error, result, fields) {
                            if (error) {
                              logger.error(error)
                            }
                          });
                      } else {
                        logger.error("Error");
                      }
                    }

                  })


                } else {
                  logger.error(Login);
                }
              } else {
                logger.error("Get DocEntry Error (" + db + "): " + reference);
              }
            } else {
              await logger.info('Ref (' + db + '): ' + reference + ', Cutt Off: ' + data.CUTOFF + ' Already Updated')
            }
          }
          resolve("Update Cutt Off (" + db + "): " + reference + " Finished")
        }
      });

    } catch (err) {
      reject(err)
    }

  });
};


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

const getValidationAmount = async (custReff, dbName, auth) => {
  return new Promise((resolve, reject) => {
    fetch(config.base_url_xsjs + "/PaymentService/get_validation_amount.xsjs?reff=" + custReff + "&dbName=" + dbName, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': auth,
        'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
      },
      'maxRedirects': 20
    })
      .then(res => res.json())
      .then(valAmount => {
        var rowdata = {}
        // print log
        logger.info("(OUT) Middleware - XSJS (" + dbName + "): GET Validation Amount " + custReff + " :" + config.base_url_xsjs + "/PaymentService/get_validation_amount.xsjs?reff=" + custReff + "&dbName=" + dbName)

        Object.keys(valAmount.VALAMOUNT).forEach(function (key) {
          var d = valAmount.VALAMOUNT[key];

          rowdata = {
            "CustomerReff": d.CustomerReff,
            "Amount": d.TotalAmount
          };
        })

        if (rowdata === null) {
          resolve("AmountNull")
        } else {
          resolve(rowdata)
        }

      })
      .catch(err => {
        logger.error("Get Error: " + err.message)
      });
  })
}

function generateMessageBodySignature(message, privateKey) {
  try {
    const sign = crypto.createSign('RSA-SHA1');
    sign.update(message);
    sign.end();
    const signature = sign.sign(privateKey);
    logger.info("signature (" + db.DB + "): " + signature);
    return signature.toString('base64')
  } catch (error) {
    logger.error(error);
  }
}

const dataValidation = async (trxid) => {
  return new Promise(async (resolve, reject) => {
    var start = new Date();
    logger.info("get validation:" + trxid)
    await connection.query('SELECT "PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", CAST("AMOUNT" AS DECIMAL(18,2)) "AMOUNT"' +
      ', TO_CHAR("TRANSDATE", \'YYYYMMDD\') "TRANSDATE", TO_CHAR("TRANSTIME", \'HH24MIss\') "TRANSTIME", "STATUS", "REASON", CAST("BANKCHARGE" AS DECIMAL(18,2)) "BANKCHARGE", "FLAGUDO"' +
      ', "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID"' +
      'FROM paymenth2h."BOS_TRANSACTIONS"  WHERE "TRXID" = $1 limit 1', [trxid], async function (error, result, fields) {
        if (error) {
          logger.info(error)
          var rowdata = {
            'result_code': '1',
            'Result_msg': 'OK',
            'Message': error.message,
            'date': moment(start).format('YYYY-MM-DD'),
            'time': moment(start).format('HH:mm:DD')
          };
          resolve(rowdata)
        } else {
          //logger.info(result)
          rowdata = {};
          dbkode = "";
          if (result.rowCount > 0) {
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

            resolve(rowdata)
          } else {
            resolve("error")
          }
        }
      });
  })
}

exports.get_Verification = async function (req, res) {
  var start = new Date();

  

  logger.info('(IN) Moboay -> Middleware: Proses Validation Payment, Body: ' + req.body.trxId);

  const timer = ms => new Promise(res => setTimeout(res, ms))
  for (let index = 1; index <= 15; index++) {

    let validation = await dataValidation(req.body.trxId)
    if (validation === "error") {
      logger.info("Retry " + req.body.trxId + ", Count: 15/" + index);
      await timer(10000)

      if (index === 15) {
        var rowdata = {
          'result_code': '1',
          'Result_msg': 'OK',
          'Message': "Validation no matching record found",
          'date': moment(start).format('YYYY-MM-DD'),
          'time': moment(start).format('HH:mm:DD')
        };
        res.json(rowdata);
      }
    } else {
      res.json(validation);
      break;
    }
  }

}

exports.post_statusPayment = async function (req, res) {
  var start = new Date();
  var date = moment(start).format('YYYYMMDD')

  logger.info("(IN) Moboay -> Middleware: Payment Status, Body " + JSON.stringify(req.body));
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
    , 'Basic ' + nodeBase64.encode("SYSTEM:PnC$gRP@2018")
    , moment(new Date()).format("yyyyMMDD")
  ]

  // await connection.query('SELECT COUNT(*) "CountError" FROM paymenth2h."BOS_LOG_TRANSACTIONS" WHERE "STATUS" = \'4\' AND "REFERENCY" = $1', [req.body.customerReffNumber], async function (error, result, fields) {
  //   if (error) {
  //     logger.error(error)
  //   } else {
  //     for (var data of result.rows) {
  //       if (parseFloat(data.CountError) < 1) { // jika data tidak failed maka bisa menerima status dari mobopay

  //       }
  //     }
  //   }
  // });

  if (req.body.resultCode == "1") {

    logger.info(req.body.customerReffNumber + " Success: Msg " + req.body.errorMessage)
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
      , 'Basic ' + nodeBase64.encode("SYSTEM:PnC$gRP@2018")
      , moment(new Date()).format("yyyyMMDD")
    ]

    // jika sudah not confirm maka payment dianggap failed walapun ada balasan dari mobopay success

    // INSERT KE LOG, JIKA SUDAH MENDAPATAKN STATUS PEMBAYARAN DARI MOBOPAY
    await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
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
            await connection.query('INSERT INTO paymenth2h."BOS_DO_STATUS" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)', data_transaction, function (error, result1, fields) {
              if (error) {
                logger.error(error)
              }
            });
            let CompanyDb = await connection.query('SELECT "DB", "PAYMENTOUTTYPE", "PAYMENTNO" FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1 limit 1', [req.body.customerReffNumber])
            for (var db of CompanyDb.rows) {
              var getEntry = await getEntryUdo(req.body.customerReffNumber, db.DB, req.body.trxId, db.PAYMENTOUTTYPE, "N")
              if (Object.keys(getEntry).length > 0) {

                let Login = await LoginSL(db.DB)
                if (Login !== "LoginError") {

                  let updateUdo
                  Object.keys(getEntry).forEach(async function (key) {
                    if (getEntry[key].DocEntry != "") {
                      updateUdo = await updateLineUdo(getEntry[key].DocEntry, getEntry[key].LineId, getEntry[key].TrxId, getEntry[key].Auth
                        , getEntry[key].DbName, getEntry[key].Referency, getEntry[key].OutType, "N", db.PAYMENTNO, db.PAYMENTOUTTYPE)
                      if (updateUdo === "success") {
                        logger.info("Transaction (" + db.DB + "): " + req.body.customerReffNumber + " Has Finished");
                      } else {
                        logger.error("Error");
                      }
                    }

                  })

                } else {
                  logger.error(Login);
                }
              } else {
                logger.error("Get DocEntry Error (" + db.DB + "): " + req.body.customerReffNumber);
              }
            }

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
    logger.info(req.body.customerReffNumber + " Error: Msg " + req.body.errorMessage)
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
      , 'Basic ' + nodeBase64.encode("SYSTEM:PnC$gRP@2018")
    ]

    //logger.info(data_transaction)

    let cekError = await connection.query('SELECT COUNT(*) "errorCount" FROM paymenth2h."BOS_LOG_TRANSACTIONS" WHERE "REFERENCY" = $1 AND "ERRORCODE" = $2', [req.body.customerReffNumber, req.body.resultCode])
    if (cekError.rowCount > 0) {
      for (var data of cekError.rows) {
        if (data.errorCount == "0") {
          await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
            'SELECT "PAYMENTOUTTYPE", "PAYMENTNO",$6, "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 4, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
            'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [req.body.errorMessage, req.body.customerReffNumber, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), req.body.resultCode, req.body.trxId], async function (error, result, fields) {
              if (error) {
                logger.error(error)
              }
            });
          var trx_id = "";
          if (req.body.trxId != "") {
            trx_id = req.body.trxId;
            await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "TRXID" = $2, "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [req.body.customerReffNumber, req.body.trxId])
          }
          await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [req.body.customerReffNumber])
          let CompanyDb = await connection.query('SELECT "DB", "PAYMENTOUTTYPE", "PAYMENTNO" FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1 limit 1', [req.body.customerReffNumber])
          for (var db of CompanyDb.rows) {
            //await getEntryUdoOut(req.body.customerReffNumber, db.DB, trx_id, db.PAYMENTOUTTYPE, "Y")
            var getEntry = await getEntryUdo(req.body.customerReffNumber, db.DB, trx_id, db.PAYMENTOUTTYPE, "Y")
            if (Object.keys(getEntry).length > 0) {

              let Login = await LoginSL(db.DB)
              if (Login !== "LoginError") {

                let updateUdo
                Object.keys(getEntry).forEach(async function (key) {
                  if (getEntry[key].DocEntry != "") {
                    updateUdo = await updateLineUdo(getEntry[key].DocEntry, getEntry[key].LineId, getEntry[key].TrxId, getEntry[key].Auth
                      , getEntry[key].DbName, getEntry[key].Referency, getEntry[key].OutType, "Y", db.PAYMENTNO, db.PAYMENTOUTTYPE)
                    if (updateUdo === "success") {
                      logger.info("Transaction (" + db.DB + "): " + req.body.customerReffNumber + " Has Finished");
                    } else {
                      logger.error("Error");
                    }
                  }

                })

              } else {
                logger.error(Login);
              }
            } else {
              logger.error("Get DocEntry Error (" + db.DB + "): " + req.body.customerReffNumber);
            }
          }
        } else {
          await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
            'SELECT "PAYMENTOUTTYPE", "PAYMENTNO",$6, "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 4, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
            'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [req.body.errorMessage, req.body.customerReffNumber, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), req.body.resultCode, req.body.trxId], async function (error, result, fields) {
              if (error) {
                logger.error(error)
              }
            });
          var trx_id = "";
          if (req.body.trxId != "") {
            trx_id = req.body.trxId;
            await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "TRXID" = $2, "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [req.body.customerReffNumber, req.body.trxId])
          }
          await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [req.body.customerReffNumber])
          let CompanyDb = await connection.query('SELECT "DB", "PAYMENTOUTTYPE", "PAYMENTNO" FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1 limit 1', [req.body.customerReffNumber])
          for (var db of CompanyDb.rows) {
            //await getEntryUdoOut(req.body.customerReffNumber, db.DB, trx_id, db.PAYMENTOUTTYPE, "Y")
            var getEntry = await getEntryUdo(req.body.customerReffNumber, db.DB, trx_id, db.PAYMENTOUTTYPE, "Y")
            if (Object.keys(getEntry).length > 0) {

              let Login = await LoginSL(db.DB)
              if (Login !== "LoginError") {

                let updateUdo
                Object.keys(getEntry).forEach(async function (key) {
                  if (getEntry[key].DocEntry != "") {
                    updateUdo = await updateLineUdo(getEntry[key].DocEntry, getEntry[key].LineId, getEntry[key].TrxId, getEntry[key].Auth
                      , getEntry[key].DbName, getEntry[key].Referency, getEntry[key].OutType, "Y", db.PAYMENTNO, db.PAYMENTOUTTYPE)
                    if (updateUdo === "success") {
                      logger.info("Transaction (" + db.DB + "): " + req.body.customerReffNumber + " Has Finished");
                    } else {
                      logger.error("Error");
                    }
                  }

                })

              } else {
                logger.error(Login);
              }
            } else {
              logger.error("Get DocEntry Error (" + db.DB + "): " + req.body.customerReffNumber);
            }
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
      await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID")' +
        'SELECT "PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 0, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID"' +
        'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', ['Generate Outgoing by user', req.body.referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss")], async function (error, result, fields) {
          if (error) {
            logger.error(error)
          }
        });

      let update = await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = $2 WHERE "REFERENCY" = $1', [req.body.referency, 'Y'])
      await connection.query('UPDATE paymenth2h."BOS_DO_STATUS" SET "flag" = \'1\' WHERE "customerReffNumber" = $1', [req.body.referency], async function (error, result, fields) {
        if (error) {
          logger.error(error)
        }
      });
    }
  }
}

const LoginSL = async (dbName) => {
  return new Promise((resolve, reject) => {
    var myHeaders = new nodeFetch.Headers();
    //var retries = 3;
    var backoff = 300;
    myHeaders.append("Authorization", "Basic " + config.auth_basic);
    myHeaders.append("Content-Type", "application/json");
    logger.info("(OUT) Middleware - Service Layer (" + dbName + "), Login : " + config.base_url_SL + "/b1s/v2/Login")
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
    var url = config.base_url_SL + "/b1s/v2/Login";
    const retryCodes = [408, 500, 502, 503, 504, 522, 524]
    return fetchWithRetry(url, requestOptions, config.timeOut, config.max_retries)
      .then(res => {
        //logger.debug(res.text())

        if (res.status == 200) return res.text()

      })
      .then(result => {
        if (result !== null) {
          const resultBody = JSON.parse(result)
          if (!resultBody.error) {
            Session = resultBody.SessionId
            logger.info("Login Service Layer: (" + dbName + ") Success")
            resolve(Session);
          } else {
            logger.info("Error Login (" + dbName + ")", result);
            resolve("LoginError")
          }
        }

      })
      .catch(error => {
        logger.info('Error Login : ' + error)
        resolve("LoginError")
      });
  });
};

async function Logout() {
  fetch(config.base_url_SL + "/b1s/v2/Login",
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Basic ' + config.auth_basic
      },
      headers: { 'Content-Type': 'application/json' },
      withCredentials: true,
      xhrFields: {
        'withCredentials': true
      }
    }
  )
    .then(async res => {
      return res.text()
    })
    .then(result => {
    })
    .catch(error => {
      logger.info("Logout Service Layer Error: " + error)
    });
}

exports.post_logTransactions = async function (req, res) {
  var start = new Date();
  var date = moment(start).format('YYYYMMDD')

  //jobs.stop();

  //logger.info(req.body) 

  logger.info("(IN) SAP - Middleware: Insert Data Transaksi, Body: ", req.body)
  let custreff = ""
  //await getConfigDB(req.body.db);
  custreff = req.body.referency
  var data_transaction = [];
  var dataDetail = [];
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


  for await (let index of req.body.details) {
    dataDetail = [
      index.preferredTransferMethodId,
      index.debitAccount,
      index.creditAccount,
      index.customerReferenceNumber,
      index.chargingModelId,
      index.defaultCurrencyCode,
      index.debitCurrency,
      index.creditCurrency,
      index.chargesCurrency,
      index.remark1,
      index.remark2,
      index.remark3,
      index.remark4,
      index.paymentMethod,
      index.extendedPaymentDetail,
      index.preferredCurrencyDealId,
      index.destinationBankCode,
      index.beneficiaryBankName,
      index.switcher,
      index.beneficiaryBankAddress1,
      index.beneficiaryBankAddress2,
      index.beneficiaryBankAddress3,
      index.valueDate,
      index.debitAmount,
      index.beneficiaryName,
      index.beneficiaryEmailAddress,
      index.beneficiaryAddress1,
      index.beneficiaryAddress2,
      index.beneficiaryAddress3,
      index.debitAccount2,
      index.debitAmount2,
      index.debitAccount3,
      index.debitAmount3,
      index.creditAccount2,
      index.creditAccount3,
      index.creditAccount4,
      index.instructionCode1,
      index.instructionRemark1,
      index.instructionCode2,
      index.instructionRemark2,
      index.instructionCode3,
      index.instructionRemark3,
      index.paymentRemark1,
      index.paymentRemark2,
      index.paymentRemark3,
      index.paymentRemark4,
      index.reservedAccount1,
      index.reservedAmount1,
      index.reservedAccount2,
      index.reservedAmount2,
      index.reservedAccount3,
      index.reservedAmount3,
      index.reservedField1,
      index.reservedField2,
      index.reservedField3,
      index.reservedField4,
      index.reservedField5,
      index.reservedField6,
      index.reservedField7,
      index.reservedField8,
      index.reservedField9,
      index.reservedField10,
      index.reservedField11,
      index.reservedField12,
      index.reservedField13,
      index.reservedField14,
      index.reservedField15,
      index.reservedField16,
      index.reservedField17,
      index.reservedField18,
      index.reservedField19,
      index.reservedField20,
    ]
  }

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
                        await connection.query('DELETE FROM paymenth2h."BOS_TRANSACTIONS_D" WHERE "customerReferenceNumber" = $1', [req.body.referency])
                        await connection.query('INSERT INTO paymenth2h."BOS_TRANSACTIONS_D" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72);', dataDetail, async function (error, result, fields) {
                          if (error) {
                            logger.error(error)
                          }
                        });
                        var data1 = {
                          'result': "0",
                          'msgCode': "0",
                          'msgDscriptions': "Success"
                        }
                        res.json(data1);

                        // jika berhasil di add maka insert ke log
                        await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
                          'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, $1, $6, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
                          'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [1, req.body.referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), '', ''], async function (error, result, fields) {
                            if (error) {
                              logger.error(error)
                            }
                            //await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [referency])
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
            let cekError = await connection.query('SELECT CAST(coalesce(MAX("RESENDCOUNT"),\'0\') as INT) + 1 as "RESENDCOUNT" FROM  paymenth2h."BOS_TRANSACTIONS"  WHERE "REFERENCY"=$1', [req.body.referency])
            if (cekError.rowCount > 0) {
              for (var data of cekError.rows) {
                logger.info("check resend count (" + req.body.db + "): " + req.body.referency + ", " + parseFloat(data.RESENDCOUNT))
                if (parseFloat(data.RESENDCOUNT) <= config.resend_payment_retries) {
                  //await connection.query('UPDATE  paymenth2h."BOS_TRANSACTIONS_D" SET "preferredTransferMethodId" = $1  WHERE "customerReferenceNumber" = $2', [req.body.chargingModelId, req.body.referency])


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
                          await connection.query('DELETE FROM paymenth2h."BOS_TRANSACTIONS_D" WHERE "customerReferenceNumber" = $1', [req.body.referency])
                          await connection.query('INSERT INTO paymenth2h."BOS_TRANSACTIONS_D" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72);', dataDetail, async function (error, result, fields) {
                            if (error) {
                              logger.error(error)
                            }
                          });
                          await connection.query('UPDATE  paymenth2h."BOS_TRANSACTIONS" SET "RESENDCOUNT" = $1  WHERE "REFERENCY" = $2', [data.RESENDCOUNT, req.body.referency])

                          await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
                            'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, $1, $6, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
                            'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [3, req.body.referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), '', ''], async function (error, result, fields) {
                              if (error) {
                                logger.error(error)
                              }
                              //await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [referency])
                            });
                        }
                      });
                    }
                  });
                } else {

                  //CEK JIKA MASIH ERROR CODE MAX RESED
                  await connection.query('UPDATE  paymenth2h."BOS_TRANSACTIONS_D" SET "preferredTransferMethodId" = $1  WHERE "customerReferenceNumber" = $2', [req.body.chargingModelId, req.body.referency])


                  let cekError = await connection.query('SELECT COUNT(*) "errorCount", "TRXID"  FROM paymenth2h."BOS_LOG_TRANSACTIONS" WHERE "REFERENCY" = $1 AND "ERRORCODE" = $2 AND "TRXID" != \'\' GROUP BY "TRXID"', [req.body.referency, 'SAP-MR01'])

                  var trxid;
                  if (cekError.rowCount >= 0 || cekError.rowCount == "") {
                    for (var data of cekError.rows) {

                      trxid = data.TRXID

                      if (data.errorCount == "0" || data.errorCount == "") {
                        await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
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
                  //await getEntryUdoOut(req.body.referency, req.body.db, trxid, req.body.paymentOutType, "Y")
                  var getEntry = await getEntryUdo(req.body.referency, req.body.db, trxid, req.body.paymentOutType, "Y")
                  if (Object.keys(getEntry).length > 0) {

                    let Login = await LoginSL(db)
                    if (Login !== "LoginError") {

                      let updateUdo
                      Object.keys(getEntry).forEach(async function (key) {
                        if (getEntry[key].DocEntry != "") {
                          updateUdo = await updateLineUdo(getEntry[key].DocEntry, getEntry[key].LineId, getEntry[key].TrxId, getEntry[key].Auth
                            , getEntry[key].DbName, getEntry[key].Referency, getEntry[key].OutType, "Y", req.body.paymentNo, req.body.paymentOutType)
                          if (updateUdo === "success") {
                            logger.info("Transaction (" + db + "): " + referency + " Has Finished");
                          } else {
                            logger.error("Error");
                          }
                        }
                      })



                    } else {
                      logger.error(Login);
                    }
                  } else {
                    logger.error("Get DocEntry Error (" + db + "): " + reference);
                  }
                }
              }
            }
          }
        } else {
          await connection.query('UPDATE paymenth2h."BOS_DO_STATUS" SET "flag" = \'1\' WHERE "customerReffNumber" = $1', [req.body.referency], async function (error, result, fields) {
            if (error) {
            }
          });
          await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
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

  await connection.query('DELETE FROM paymenth2h."BOS_TRANSACTIONS_D" WHERE "customerReferenceNumber" = $1', [req.body.customerReferenceNumber])

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
      //await connection.query('UPDATE  paymenth2h."BOS_TRANSACTIONS_D" SET "preferredTransferMethodId" = $1  WHERE "customerReferenceNumber" = $2', [req.body.preferredTransferMethodId, req.body.customerReferenceNumber])
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
        logger.info("Kode Database: " + dbkode)

        var fs = require('fs');
        fs.writeFile('configDB.txt', req.body.dbName, function (err) {
          if (err) throw err;
          console.log('Saved!');
        });
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
          // await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
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
async function interfacing_mobopay_TransactionBakcUp(referency, payment, outType, trxId) {

  // untk format tanggal
  var start = new Date();
  var date = moment(start).format("yyyy-MM-DDTHH:mm:ss.SSS") + "Z"

  try {
    // ambil data jika masih ada yg belom berhasil diinsterfacing
    let result = await connection.query('SELECT "preferredTransferMethodId", "debitAccount", "creditAccount", "customerReferenceNumber", "chargingModelId"' +
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

      for (var data of result.rows) {


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
        prosesInterfacing(bodyPayment, _token, _clientId, date, _signature, data.customerReferenceNumber, data.NM_DB, payment, outType, trxId)
      }
    }
  } catch (error) {
    logger.error(error)
  }

}

const interfacing_mobopay_Transaction = async (referency, payment, outType, trxId, dbName) => {
  return new Promise((resolve, reject) => {
    logger.info('Proses payment to Mobopay (' + dbName + '), Referency: ' + referency);
    // untk format tanggal
    var start = new Date();
    var date = moment(start).format("yyyy-MM-DDTHH:mm:ss.SSS") + "Z"

    try {
      // ambil data jika masih ada yg belom berhasil diinsterfacing
      connection.query('SELECT "preferredTransferMethodId", "debitAccount", "creditAccount", "customerReferenceNumber", "chargingModelId"' +
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
        'WHERE X1."REFERENCY" = $1 ORDER BY X1."INTERFACING" ASC limit 1', [referency], async (error, result, field) => {
          if (!error) {

            // deklarasi
            bodyPayment = {};
            _signature = "";
            _token = "";
            _base64key = "";
            _clientId = "";
            _bodyToken = "";

            for (var data of result.rows) {
              _clientId = data.ID_DB
              _bodyToken = base64encode(data.CUST_KEY + ":" + data.CUST_SECRET);
              _token = await getToken(_bodyToken)
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
              resolve({
                BodyPayment: bodyPayment, Token: _token, ClientId: _clientId, Date: date, Signature: _signature, DbName: data.NM_DB, Amount: data.debitAmount
              })
              //prosesInterfacing(bodyPayment, _token, _clientId, date, _signature, data.customerReferenceNumber, data.NM_DB, payment, outType, trxId)
            }
          }else(
            logger.error("Get Details Transaction Error: " + error)
          )
        })

    } catch (error) {
      logger.error(error)
    }

  });
};


const prosesInterfacing = async (body, token, clientId, date, signature, referency, dbName, payment, outType, trxId) => {
  return new Promise((resolve, reject) => {

    var start = new Date();
    var myHeaders = new nodeFetch.Headers();
    try {
      myHeaders.append("Authorization", "Bearer " + token);
      myHeaders.append("X-Client-Id", clientId);
      myHeaders.append("X-Timestamp", date);
      myHeaders.append("X-Signature", signature);
      myHeaders.append("Content-Type", "application/json");
      myHeaders.append("X-Action", payment);

      var raw = JSON.stringify(body);

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


      const fetchWithRetry = async (input, init, timeout, retries) => {
        let increseTimeOut = config.interval_retries;
        let count = retries;

        while (count > 0) {
          try {
            if (count == 3) {
              // untuk pengiriman pertama kali
            } else if (count <= 2 && count >= 1) {
              myHeaders.set("X-Action", payment); // jika resend payment
            } else if (count == 0) {
              await updateTableRetry(referency, "4", "Resend Payment")
              //await getEntryUdoOut(referency, dbName, trxId, outType, "N") // jika mencapai maksimum retry, maka update detail UDO menjadi failed
              var getEntry = await getEntryUdo(referency, dbName, trxId, outType, "Y")
              if (Object.keys(getEntry).length > 0) {

                let Login = await LoginSL(db)
                if (Login !== "LoginError") {

                  let updateUdo
                  Object.keys(getEntry).forEach(async function (key) {
                    if (getEntry[key].DocEntry != "") {
                      updateUdo = await updateLineUdo(getEntry[key].DocEntry, getEntry[key].LineId, getEntry[key].TrxId, getEntry[key].Auth
                        , getEntry[key].DbName, getEntry[key].Referency, getEntry[key].OutType, "Y", "", outType)
                      if (updateUdo === "success") {
                        logger.info("Transaction (" + db + "): " + referency + " Has Finished");
                      } else {
                        logger.error("Error");
                      }
                    }
                  })


                } else {
                  logger.error(Login);
                }
              } else {
                logger.error("Get DocEntry Error (" + db + "): " + reference);
              }
            }
            //logger.info(myHeaders)
            return await fetchWithTimeout(input, init, timeout);
          } catch (e) {
            if (e.name !== "AbortError") throw e;
            count--;
            logger.error(
              `fetch failed Cusreff: ${referency}, retrying in ${increseTimeOut}s, ${count} retries left`
            );
          }
        }
      }
      logger.info("(OUT) Middleware - Mobopay (" + dbName + "): Do Payment : " + referency + " -> Url: " + config.base_url_payment + "/mobopay/mandiri/payment/do-payment")

      return fetchWithRetry(config.base_url_payment + "/mobopay/mandiri/payment/do-payment", requestOptions, config.timeOut, config.max_retries)
        .then(response => response.text())
        .then(async result => {
          const resultBody = JSON.parse(result)
          logger.info("Response Mobopay: " + result)
          logger.info("Result Code Response: " + resultBody.resultCode)
          if (resultBody.resultCode == "0") {
            await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [referency])

            await logger.info("(IN) Mobopay - Middleware  (" + dbName + "): Do Payment Error : Payment Failed: " + referency + " , Error Code: " + resultBody.errorCode + " Msg Error: " + resultBody.message)

            await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
                      'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 4, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID",  $5' +
                      'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [resultBody.message, referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), resultBody.errorCode]);

            resolve({ success: "error", trxId })
            
            //await connection.query('SELECT COUNT(*) "CountError" FROM paymenth2h."BOS_LOG_TRANSACTIONS" WHERE "REFERENCY" = $1 AND "STATUS" = \'4\'', [referency], async function (error, result, fields) {
              //if (error) {
                //logger.error(error)
              //} else {
               // for (var data of result.rows) {
               //   if (data.CountError != "0") {
                    
                //  }
               // }
             // }
           // });
          }
          else if (resultBody.resultCode == "1") {

            // JIKA TIDAK ERROR MAKA INSERT KE LOG
            await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'Y\', "TRXID" = $2 WHERE "REFERENCY" = $1', [referency, resultBody.data.trxId])

            await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID")' +
              'SELECT "PAYMENTOUTTYPE", "PAYMENTNO", $1, "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 2, $5, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID"' +
              'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [resultBody.data.trxId, referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), resultBody.message]);

            resolve({ success: "success", trxid: resultBody.data.trxId })
          }
        })
        .catch(err => {
          logger.error(err)
        }
        )
        .finally(() => {
          clearTimeout(timeout);
        });
    } catch (error) {
      logger.error(err)
    }

  });
};

async function updateTableRetry(reference, status, reason) {
  var start = new Date();
  //logger.info(reference + status + reason)
  // CEK JIKA RTO nya sudah lebih dari 3
  let result = await connection.query('SELECT COUNT(*) "rtoCount" FROM paymenth2h."BOS_LOG_TRANSACTIONS"  WHERE "REFERENCY" = $1 AND "ERRORCODE" = \'RTO\'', [reference])
  //logger.info(result)

  if (result.rowCount > 0) {
    for (var data of result.rows) {
      if (data.rtoCount < config.max_retries) {
        await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
          'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, $1, $6, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
          'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [status, reference, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), "RTO", reason], async function (error, result, fields) {
            if (error) {
              logger.error(error)
            } else {
              if (status == 4) {
                await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [reference])
              }
            }

          });
      }
    }
  }
}

const updateLineUdo = async (DocEntry, Lines, trxid, auth, dbName, referency, outType, error, docnumOut, typePayment) => {
  return new Promise((resolve, reject) => {
    // mendapatakan detail dari pym out
    //let login = LoginSL(dbName);
    var start = new Date();
    let status = ""


    if (outType == "OUT") {
      //logger.info("Line Out: " + Lines + ", Entry: " + DocEntry)
      logger.info("(OUT) Mobopay - SAP  (" + dbName + "): GET Lines Detail UDO Payment Out" + config.base_url_xsjs + "/PaymentService/get_lines_out.xsjs?Entry=" + DocEntry + "&LineId=" + Lines + "&dbName=" + dbName)
      fetch(config.base_url_xsjs + "/PaymentService/get_lines_out.xsjs?Entry=" + DocEntry + "&LineId=" + Lines + "&dbName=" + dbName, {
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
          var row = {}
          details = []
          Object.keys(data.LINES).forEach(async function (key) {
            var d = data.LINES[key];
            if (typePayment === "OUT") {
              if (error == "Y") {
                row = {
                  'LineId': parseFloat(Lines),
                  'U_TRXID_SAP': trxid,
                  'U_STATUS': 'Failed',
                  'U_PROSES_TYPE': d.ProcessType,
                  'U_ACCT_PYM_REQ': d.PaymentMeans,
                  'U_BANK_PYM_REQ': d.BankPaymentMeans,
                  'U_SOURCEACCT': d.SourcePayment,
                  'U_PYMOUT_STATUS': 'H2H',
                  'U_CUST_REFF': referency
                }
              } else {
                row = {
                  'LineId': parseFloat(Lines),
                  'U_TRXID_SAP': trxid,
                  'U_PROSES_TYPE': d.ProcessType,
                  'U_ACCT_PYM_REQ': d.PaymentMeans,
                  'U_BANK_PYM_REQ': d.BankPaymentMeans,
                  'U_SOURCEACCT': d.SourcePayment,
                  'U_PYMOUT_STATUS': 'H2H',
                  'U_CUST_REFF': referency
                }
              }
            } else {
              if (error == "Y") {
                row = {
                  'LineId': parseFloat(Lines),
                  'U_TRXID_SAP': trxid,
                  'U_STATUS': 'Failed',
                  'U_PROSES_TYPE': d.ProcessType,
                  'U_ACCT_PYM_REQ': d.PaymentMeans,
                  'U_BANK_PYM_REQ': d.BankPaymentMeans,
                  'U_SOURCEACCT': d.SourcePayment,
                  'U_PYMOUT_STATUS': docnumOut + ' - Open',
                  'U_CUST_REFF': referency
                }
              } else {
                row = {
                  'LineId': parseFloat(Lines),
                  'U_TRXID_SAP': trxid,
                  'U_PROSES_TYPE': d.ProcessType,
                  'U_ACCT_PYM_REQ': d.PaymentMeans,
                  'U_BANK_PYM_REQ': d.BankPaymentMeans,
                  'U_SOURCEACCT': d.SourcePayment,
                  'U_PYMOUT_STATUS': docnumOut + ' - Open',
                  'U_CUST_REFF': referency
                }
              }
            }

            details.push(row)
          })
          logger.info(row)

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
          logger.info("(OUT) Mobopay - SAP  (" + dbName + "): Update Flag UDO Payment Out " + config.base_url_SL + "/b1s/v2/PYM_OUT(" + parseFloat(DocEntry) + ")")

          return fetchWithRetry(config.base_url_SL + "/b1s/v2/PYM_OUT(" + parseFloat(DocEntry) + ")", requestOptions, 60000, config.max_retries)
            .then(response => {
              if (response.status == 204) {
                response.text()
              }
            }).then(result => {
              resolve("success")
            }).catch(error => logger.error('error', error));
        })
        .catch(err => {
          logger.error("Get Error: " + err.message)
          resolve("Update Error")
        });
    } else {

      logger.info("(OUT) Mobopay - SAP  (" + dbName + "): GET Lines Detail UDO Payment Out Status" + config.base_url_xsjs + "/PaymentService/get_line_outstatus.xsjs?Entry=" + DocEntry + "&LineId=" + Lines + "&dbName=" + dbName)

      fetch(config.base_url_xsjs + "/PaymentService/get_line_outstatus.xsjs?Entry=" + DocEntry + "&LineId=" + Lines + "&dbName=" + dbName, {
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
          var row = {}
          details = []
          Object.keys(data.LINES).forEach(function (key) {
            var d = data.LINES[key];
            if (error == "Y") {
              row = {
                'LineId': parseFloat(Lines),
                'U_STATUS': 'Failed',
                'U_TRXID_SAP': trxid,
                'U_PROSES_TYPE': d.ProcessType,
                'U_ACCT_PYM_REQ': d.PaymentMeans,
                'U_BANK_PYM_REQ': d.BankPaymentMeans,
                'U_SOURCEACCT': d.SourcePayment,
                'U_PYMOUT_STATUS': docnumOut + ' - Open',
                'U_CUST_REFF': referency
              }
            } else {
              row = {
                'LineId': parseFloat(Lines),
                'U_TRXID_SAP': trxid,
                'U_PROSES_TYPE': d.ProcessType,
                'U_ACCT_PYM_REQ': d.PaymentMeans,
                'U_BANK_PYM_REQ': d.BankPaymentMeans,
                'U_SOURCEACCT': d.SourcePayment,
                'U_PYMOUT_STATUS': docnumOut + ' - Open',
                'U_CUST_REFF': referency
              }
            }
            details.push(row)
          })

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
                if (e.name !== "AbortError") throw e;
                count--;
                logger.error(
                  `fetch patch, retrying patch in ${increseTimeOut}s, ${count} retries left`
                );
                await wait(increseTimeOut)
              }
            }
          }
          logger.info("(OUT) Mobopay - SAP  (" + dbName + "): Update Flag UDO Payment Out Status" + config.base_url_SL + "/b1s/v2/PYMOUT_STATUS(" + parseFloat(DocEntry) + ")")
          return fetchWithRetry(config.base_url_SL + "/b1s/v2/PYMOUT_STATUS(" + parseFloat(DocEntry) + ")", requestOptions, 60000, config.max_retries)
            .then(response => {
              if (response.status == 204) {
                // connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "FLAGUDO" = \'1\' WHERE "REFERENCY" = $1', [referency], async function (error, result, fields) {
                //   if (error) {
                //     logger.error(error)
                //   }
                // });
                response.text()
              }
            }).then(result => {
              resolve("success")
            }).catch(error => {
              logger.error('error', error)
              reject("error")
            });
        })
        .catch(err => {
          logger.error("Get Error: " + err.message)
          reject("Update Error")
        });
    }
  });
};


const getToken = (base64Key) => {
  return new Promise((resolve, reject) => {
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

    fetch(config.base_url_mobopay + "/token?grant_type=client_credentials", requestOptions)
      .then(response => response.text())
      .then(result => {
        const resultToken = JSON.parse(result)
        logger.info("Result Token:  " + resultToken.access_token)
        stringToken = resultToken.access_token
        resolve(resultToken.access_token)
      }).catch(error => logger.info('Error Token', error));
  });
};

function token(base64Key) {
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

  fetch(config.base_url_mobopay + "/token?grant_type=client_credentials", requestOptions)
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
