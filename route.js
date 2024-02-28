module.exports = function (app) {
    var ctrl = require('./controller');

    // get signature
    app.route('/SE/getSignature').post(ctrl.get_Signature);

    // get Toen
    app.route('/SE/getToken').get(ctrl.get_Token);

    // get verification
    app.route('/payment/Verification').post(ctrl.get_Verification);

    //get verification
    app.route('/payment/do_Status').post(ctrl.post_statusPayment);


    // ------------------------- dari sap ------------------------ //
    // get tets8ing
    //app.route('/SE/Testing').get(ctrl.get_testing);

     // Insert transaction dari SAP
     app.route('/SE/insertTransactions').post(ctrl.post_logTransactions);

     // Insert transaction dari SAP detail
     app.route('/SE/do_payment_SAP').post(ctrl.post_detailTrasactions);

     // get kode DB
    app.route('/SE/kodeDb').post(ctrl.get_kodeDb);

    // get kode DB
    app.route('/SE/getHistory').post(ctrl.get_History);

    // Update Flag h2h ke manual jika terjadi kondisi pembayaran percepatan
    app.route('/SE/updateFlag').post(ctrl.post_pdateFlag);

    // Close untuk pembayran yg dilakukan, dari H2H ke Manual
    app.route('/SE/close_Payment').post(ctrl.post_closePayment);

    // untuk mendaftarkan Database
    app.route('/SE/configDB').post(ctrl.post_configDb)

}