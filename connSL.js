// TRANSAKSI
exports.post_Transaksi_Penjualan = function (req, res) {
    // LOGIN SERVICE LAYER
    fetch("https://hanacomp.beonesolution.com:50000/b1s/v1/Login",
        {
            method: 'POST',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ CompanyDB: "ATALIVE1", UserName: "Beonesolution\\C1953107", Password: "m@Maroz2019" }),
            crossDomain: true,
            credentials: "include",
            withCredentials: true,
            xhrFields: {
                'withCredentials': true
            }

        }
    )
        .then(Response => Response.json())
        .then(data => {
        })
        .catch(err => {
            var Status = [];
            var s = {
                'ErrorMsg': '[300]',
                'Description': 'Failed, Error: ' + err.message.value,
                'SuccessMsg': ''
            }
            Status.push(s)
            console.log(Status)
            res.json(Status);
        });
    connection.query('DELETE FROM bos_penjualan_to', function (error, rows, fields) {
        if (error) {
            var Status = [];
            var s = {
                'ErrorMsg': '[300]',
                'Description': 'Failed, Error: ' + error.value,
                'SuccessMsg': ''
            }
            Status.push(s)
            console.log(Status)
            res.json(Status);
        } else {

        }
    });

    req.body.forEach(d => {
        // INSERT KE TABLE TEMPORARY
        connection.query('INSERT INTO bos_penjualan_to values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            [d.docnumber, d.moduletype, d.specialprice, d.customercode, d.employeecode, d.transdate, d.transtime, d.remarks
                , d.subtotal, d.discountpercent, d.discountvalue, d.coupon, d.discountcoupon, d.taxpercent, d.taxvalue
                , d.additionalcost, d.rounding, d.grandtotal, d.cash, d.card, d.extracharge, d.voucher, d.transfer, d.giro
                , d.changed, d.totalpayment, d.edc, d.edcCode, d.cardtype, d.cardno, d.cardholder, d.bank, d.transferdate, d.girono, d.duedate
                , d.imei, d.store, d.tokenid, d.rownumber, d.itemcode, d.quantity, d.sellingprice, d.ddiscountpercent, d.ddiscountvalue, d.totalamount
                , d.taxdetail, d.promotiontype, d.promotionid, d.remarks_d, d.cogs, d.promotionpoint, d.point, d.deliverydate, d.specialpoint
                , d.returType, d.userid, d.username, d.top, d.billto, d.shipto, d.sendtogether],
            function (error, rows, fields) {
                if (error) {
                    var Status = [];
                    var s = {
                        'ErrorMsg': '[300]',
                        'Description': 'Failed, Error: ' + error.value,
                        'SuccessMsg': ''
                    }
                    Status.push(s)
                    console.log(Status)
                    res.json(Status);
                } else {
                }
            });

        connection.query('INSERT INTO bos_penjualan_to_temp values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            [d.docnumber, d.moduletype, d.specialprice, d.customercode, d.employeecode, d.transdate, d.transtime, d.remarks
                , d.subtotal, d.discountpercent, d.discountvalue, d.coupon, d.discountcoupon, d.taxpercent, d.taxvalue
                , d.additionalcost, d.rounding, d.grandtotal, d.cash, d.card, d.extracharge, d.voucher, d.transfer, d.giro
                , d.changed, d.totalpayment, d.edc, d.edcCode, d.cardtype, d.cardno, d.cardholder, d.bank, d.transferdate, d.girono, d.duedate
                , d.imei, d.store, d.tokenid, d.rownumber, d.itemcode, d.quantity, d.sellingprice, d.ddiscountpercent, d.ddiscountvalue, d.totalamount
                , d.taxdetail, d.promotiontype, d.promotionid, d.remarks_d, d.cogs, d.promotionpoint, d.point, d.deliverydate, d.specialpoint
                , d.returType, d.userid, d.username, d.top, d.billto, d.shipto, d.sendtogether],
            function (error1, rows, fields) {
                if (error1) {
                    var Status = [];
                    var s = {
                        'ErrorMsg': '[300]',
                        'Description': 'Failed, Error: ' + error1.value,
                        'SuccessMsg': ''
                    }
                    Status.push(s)
                    console.log(Status)
                    res.json(Status);
                } else {
                }
            });

    });
    // AMBIL DISTINCT DARI DOCNUMBER
    var result = []
    connection.query('SELECT DISTINCT DOCNUMBER, STORE, CUSTOMERCODE FROM bos_penjualan_to', function (error1, rows1, fields1) {
        if (error1) {
            var Status = [];
            var s = {
                'ErrorMsg': '[300]',
                'Description': 'Failed',
                'SuccessMsg': ''
            }
            Status.push(s)
            console.log(Status)
            res.json(Status);
        } else {
            rows1.forEach(function (item) {
                var data1 = {
                    'docnumber': item.DOCNUMBER,
                    'store': item.STORE,
                    'customercode': item.CUSTOMERCODE
                }
                result.push(data1);
            })
        }
        // mengambil data untuk header
        var dataDetail = []
        var docnumber, customercode, employeecode, transdate, remarks, discountpercent, taxpercent
            , rounding, specialprice, grandtotal, imei, store, coupon, userid, username, subtotal
            , discountcoupon, top, billto, shipto, sendtogether, cardtype

        result.forEach(function (docitem) {
            var tglJE = '';
            let store1 = '';
            var query = "select distinct docnumber, customercode, employeecode, edccode, transdate, remarks, discountpercent, taxpercent, rounding, cardtype, specialprice, grandtotal , imei, store, coupon, edc,userid, username, subtotal, discountcoupon, top, billto, shipto, sendtogether from bos_penjualan_to where docnumber = '" + docitem.docnumber + "'"
            connection.query(query, function (error2, rows2, fields1) {
                if (error2) {
                    console.log(error2)
                } else {

                    for(var x = 0;x < rows2.length;x++){

                    }
                    rows2.forEach(function (item) {
                        docnumber = item.docnumber,
                            customercode = item.customercode,
                            cardcode = item.customercode,
                            employeecode = item.employeecode,
                            transdate = formatDate(item.transdate),
                            tglJE = formatDate(item.transdate),
                            remarks = item.remarks,
                            discountpercent = item.discountpercent,
                            taxpercent = item.taxpercent,
                            rounding = item.rounding,
                            specialprice = item.specialprice,
                            grandtotal = item.grandtotal,
                            imei = item.imei,
                            store = item.store,
                            store1 = item.store,
                            coupon = item.coupon,
                            userid = item.userid,
                            username = item.username,
                            subtotal = item.subtotal,
                            discountcoupon = item.discountcoupon,
                            top = item.top,
                            billto = item.billto,
                            shipto = item.shipto,
                            sendtogether = item.sendtogether
                            cardtype = item.cardtype,
                            edc = item.edc

                        // untuk mendapatakan MOP EDC
                        var MOP = '';
                        if (cardtype == 'Tunai' && edc == '') {
                            console.log(cardtype)
                            MOP = '';
                            fetch("https://hanacomp.beonesolution.com:50000/b1s/v1/BusinessPartners?$filter=contains(CardName,'MOP CASH')",
                                {
                                    method: 'GET',
                                    headers: {
                                        'Accept': 'application/json',
                                        'Content-Type': 'application/json'
                                    },
                                    crossDomain: true,
                                    credentials: "include",
                                    withCredentials: true,
                                    xhrFields: {
                                        'withCredentials': true
                                    }
                                }
                            ).then((response) =>
                                response.json()
                            ).then(datamop => {
                                MOP = datamop.value[0].CardCode
                                console.log(MOP)
                            }).catch(errormop => {
                                var Status = [];
                                var s = {
                                    'ErrorMsg': '[300]',
                                    'Description': 'Failed ' + docitem.docnumber + ", Error: " + errormop.message.value,
                                    'SuccessMsg': ''
                                }
                                Status.push(s)
                                console.log(Status)
                                res.json(Status);
                            });

                        } else if ((edc == 'GOPAY' || edc == 'Go-Pay' || edc == 'MOP GOJEK') && cardtype != 'Tunai') {
                            MOP = 'M.5700001'
                        } else {
                            MOP = '';
                            fetch("https://hanacomp.beonesolution.com:50000/b1s/v1/BusinessPartners?$filter=contains(CardName,'" + edc + "')",
                                {
                                    method: 'GET',
                                    headers: {
                                        'Accept': 'application/json',
                                        'Content-Type': 'application/json'
                                    },
                                    crossDomain: true,
                                    credentials: "include",
                                    withCredentials: true,
                                    xhrFields: {
                                        'withCredentials': true
                                    }
                                }
                            ).then((response) => response.json()
                            ).then(datamop => {
                                if (!datamop.value) {
                                    // MOP = datamop.value[0].CardCode
                                    // console.log(MOP)
                                } else {
                                    MOP = datamop.value[0].CardCode
                                    console.log(MOP)
                                }
                            }).catch(errormop => {
                                timeout = true;
                                var Status = [];
                                var s = {
                                    'ErrorMsg': '[300]',
                                    'Description': 'Failed ' + docitem.docnumber + ", Error: " + errormop.message,
                                    'SuccessMsg': ''
                                }
                                Status.push(s)
                                res.json(Status)
                                console.log(Status)
                            });
                        }

                        // medapatkan store
                        fetch("https://hanacomp.beonesolution.com:50000/b1s/v1/BusinessPartners?$filter=CardCode eq '" + docitem.store + "'",
                            {
                                method: 'GET',
                                headers: {
                                    'Accept': 'application/json',
                                    'Content-Type': 'application/json'
                                },
                                crossDomain: true,
                                credentials: "include",
                                withCredentials: true,
                                xhrFields: {
                                    'withCredentials': true
                                }
                            }
                        ).then((response) => response.json()
                        ).then(datawhs => {
                            whs = datawhs.value[0].U_WHSSTORE
                            connection.query('SELECT * FROM bos_penjualan_to WHERE docnumber = ?', docitem.docnumber, function (error3, rows3, fields1) {
                                if (error3) {
                                    var Status = [];
                                    var s = {
                                        'ErrorMsg': '[300]',
                                        'Description': 'Failed ' + docitem.docnumber + ", Error: " + error3.message,
                                        'SuccessMsg': ''
                                    }
                                    Status.push(s)
                                    res.json(Status)
                                    console.log(Status)
                                } else {
                                    var counting = 0;
                                    // connection.query('SELECT DISTINCT COUNT(*) as Counting FROM bos_penjualan_to_temp WHERE docnumber = ?', docitem.docnumber, function (error3, rows3, fields1) {
                                    //     if (error3) {
                                    //         var Status = [];
                                    //         var s = {
                                    //             'ErrorMsg': '[300]',
                                    //             'Description': 'Failed ' + docitem.docnumber + ", Error: " + error3.message,
                                    //             'SuccessMsg': ''
                                    //         }
                                    //         Status.push(s)
                                    //         res.json(Status)
                                    //         console.log(Status)
                                    //     } else {
                                    //         console.log(rows3)
                                    //         counting = rows3[0].Counting
                                    //     }
                                    // });
                                    for(var i = 0;i < rows3.length;i++){
                                        var data1 = {
                                            'ItemCode': rows3[i].ITEMCODE,
                                            'Quantity': rows3[i].QUANTITY,
                                            'PriceAfterVAT': rows3[i].SELLINGPRICE,
                                            'WarehouseCode': whs,
                                            'ShipDate': formatDate(rows3[i].DELIVERYDATE),
                                            'DiscountPercent': rows3[i].DDISCOUNTPERCENT,
                                            'FreeText': rows3[i].REMARKS_D
                                        }
                                        dataDetail.push(data1);
                                    }

                                    

                                    // proses DI KE SAP Sales Order
                                    //https://hanadbccc:4300/b1s/v1/Orders
                                    console.log(dataDetail)
                                    if (dataDetail.length != counting) {
                                        var jeDetail = ''
                                        let total = '';

                                        fetch("https://hanacomp.beonesolution.com:50000/b1s/v1/Invoices?$filter=NumAtCard eq '" + docitem.docnumber + "'",
                                            {
                                                method: 'GET',
                                                headers: {
                                                    'Accept': 'application/json',
                                                    'Content-Type': 'application/json'
                                                },
                                                crossDomain: true,
                                                credentials: "include",
                                                withCredentials: true,
                                                xhrFields: {
                                                    'withCredentials': true
                                                }
                                            }
                                        ).then((response) => response.json()
                                        ).then(dataready => {
                                            if (dataready.value.length > 0) {

                                                // cek ke JE
                                                fetch("https://hanacomp.beonesolution.com:50000/b1s/v1/JournalEntries?$select=Reference3,JdtNum&$filter=Reference3 eq '" + docitem.docnumber + "'",
                                                    {
                                                        method: 'GET',
                                                        headers: {
                                                            'Accept': 'application/json',
                                                            'Content-Type': 'application/json'
                                                        },
                                                        crossDomain: true,
                                                        credentials: "include",
                                                        withCredentials: true,
                                                        xhrFields: {
                                                            'withCredentials': true
                                                        }
                                                    }
                                                ).then((response) => response.json()
                                                ).then(dataje => {
                                                    if (dataje.value.length > 0) {
                                                        var Status = [];
                                                        var s = {
                                                            'SuccessMsg': '',
                                                            'Description': 'AlReady Exist ' + docitem.docnumber,
                                                            'ErrorMsg': '[301]'
                                                        }
                                                        Status.push(s)
                                                        console.log(Status)
                                                        res.json(Status);
                                                    } else {
                                                        // ----------------------- baris awal dari create journal ---------------- //
                                                        var bodyjeEx = ''
                                                        // mengambil docnum dan doctotal dari invoice yg sudah di buat
                                                        fetch("https://hanacomp.beonesolution.com:50000/b1s/v1/Invoices?$select=DocNum,DocTotal&$filter=NumAtCard eq ' " + docitem.docnumber + "' and DocumentStatus eq 'O'",
                                                            {
                                                                method: 'GET',
                                                                headers: {
                                                                    'Accept': 'application/json',
                                                                    'Content-Type': 'application/json'
                                                                },
                                                                crossDomain: true,
                                                                credentials: "include",
                                                                withCredentials: true,
                                                                xhrFields: {
                                                                    'withCredentials': true
                                                                }
                                                            }
                                                        ).then(res2 => res2.json()
                                                        ).then(datainvJE => {
                                                            if (datainvJE.value.length > 0) {
                                                                bodyjeEx = [{
                                                                    ContraAccount: store1,
                                                                    Credit: 0,
                                                                    Debit: datainvJE.value[0].DocTotal,
                                                                    ShortName: MOP
                                                                },
                                                                {
                                                                    ContraAccount: MOP,
                                                                    Credit: datainvJE.value[0].DocTotal,
                                                                    Debit: 0,
                                                                    ShortName: store1
                                                                }]
                                                                console.log(bodyjeEx)
                                                                if (!datainvJE.error) {
                                                                    // jika invoice sudah terbentuk maka mmbuat journal entry
                                                                    fetch('https://hanacomp.beonesolution.com:50000/b1s/v1/JournalEntries', {
                                                                        method: 'POST',
                                                                        body: JSON.stringify({
                                                                            Reference: docitem.store,
                                                                            Reference3: docitem.docnumber,
                                                                            ReferenceDate: tglJE,
                                                                            DueDate: tglJE,
                                                                            TaxDate: tglJE,
                                                                            JournalEntryLines: bodyjeEx
                                                                        }),
                                                                        headers: { 'Content-Type': 'application/json' },
                                                                    }).then(res1 => res1.json()
                                                                    ).then(data => {
                                                                        timeout = false;
                                                                        if (!data.error) {
                                                                            connection.query('DELETE FROM bos_log_penjualan_to Where docnumber = ?', docitem.docnumber, function (error, rows, fields) {
                                                                                if (error) {
                                                                                    var Status = [];
                                                                                    var s = {
                                                                                        'ErrorMsg': '[300]',
                                                                                        'Description': 'Failed ' + docitem.docnumber + ", Error: " + error,
                                                                                        'SuccessMsg': ''
                                                                                    }
                                                                                    Status.push(s)
                                                                                    console.log(Status)
                                                                                    res.json(Status);
                                                                                } else {
                                                                                }
                                                                            });
                                                                            var Status = [];
                                                                            var s = {
                                                                                'SuccessMsg': '[200]',
                                                                                'Description': 'Journal Entry Success ' + docitem.docnumber,
                                                                                'ErrorMsg': ''
                                                                            }
                                                                            Status.push(s)
                                                                            console.log(Status)
                                                                            res.json(Status);


                                                                        } else {
                                                                            var Status = [];
                                                                            var s = {
                                                                                'ErrorMsg': '[300]',
                                                                                'Description': 'Journal Entry Failed ' + docitem.docnumber + ", Error: " + data.error.message.value,
                                                                                'SuccessMsg': ''
                                                                            }
                                                                            Status.push(s)
                                                                            console.log(Status)
                                                                            res.json(Status);
                                                                        }
                                                                    }).catch(err1 => {
                                                                        timeout = true;
                                                                        var Status = [];
                                                                        var s = {
                                                                            'ErrorMsg': '[300]',
                                                                            'Description': 'Journal Entry Failed ' + docitem.docnumber + ", Error: " + err1.message,
                                                                            'SuccessMsg': ''
                                                                        }
                                                                        Status.push(s)
                                                                        res.json(Status)
                                                                        console.log(Status)
                                                                    });
                                                                }
                                                            }

                                                        }).catch(err2 => {
                                                            var Status = []
                                                            var s = {
                                                                'ErrorMsg': '[300]',
                                                                'Description': 'Failed ' + docitem.docnumber + ", Error: " + err2.message,
                                                                'SuccessMsg': ''
                                                            }
                                                            Status.push(s)
                                                            res.json(Status)
                                                            console.log(Status)
                                                        });

                                                        // ----------------------- baris akhir dari create journal ---------------- //
                                                    }
                                                }
                                                ).catch(err1 => {
                                                    timeout = true;
                                                    var Status = [];
                                                    var s = {
                                                        'ErrorMsg': '[300]',
                                                        'Description': 'Journal Entry Failed ' + docitem.docnumber + ", Error: " + err1.message,
                                                        'SuccessMsg': ''
                                                    }
                                                    Status.push(s)
                                                    res.json(Status)
                                                    console.log(Status)
                                                });

                                            } else {
                                                // DI API KE SERVICE LAYER
                                                fetch('https://hanacomp.beonesolution.com:50000/b1s/v1/Invoices', {
                                                    method: 'POST',
                                                    body: JSON.stringify(
                                                        {
                                                            CardCode: store,
                                                            NumAtCard: docitem.docnumber,
                                                            Address: billto,
                                                            Address2: shipto,
                                                            DocDueDate: transdate,
                                                            DocDate: transdate,
                                                            Comments: remarks,
                                                            VatPercent: taxpercent,
                                                            DiscountPercent: discountpercent,
                                                            RoundingDiffAmount: rounding,
                                                            // udf di disable dulu
                                                            // 'CardCode' : customercode,
                                                            // 'CardCode' : customercode,
                                                            // 'CardCode' : customercode,
                                                            U_IMEI_POS: imei,
                                                            U_USERID: userid,
                                                            U_MEMBER: docitem.customercode,
                                                            // U_MEMBER : docitem.customercode,
                                                            // 'U_SPECPRICE' : specialprice
                                                            DocumentLines: dataDetail
                                                        }),
                                                    headers: { 'Content-Type': 'application/json' },
                                                }).then(res3 => {
                                                    return res3.json()
                                                }).then(data => {
                                                    timeout = false;
                                                    if (!data.error) {
                                                        var bodyje = ''
                                                        // mengambil docnum dan doctotal dari invoice yg sudah di buat
                                                        fetch("https://hanacomp.beonesolution.com:50000/b1s/v1/Invoices?$select=DocNum,DocTotal&$filter=NumAtCard eq ' " + docitem.docnumber + "' and DocumentStatus eq 'O'",
                                                            {
                                                                method: 'GET',
                                                                headers: {
                                                                    'Accept': 'application/json',
                                                                    'Content-Type': 'application/json'
                                                                },
                                                                crossDomain: true,
                                                                credentials: "include",
                                                                withCredentials: true,
                                                                xhrFields: {
                                                                    'withCredentials': true
                                                                }
                                                            }
                                                        ).then(res2 => res2.json()
                                                        ).then(datatinv => {
                                                            if (datatinv.value.length > 0) {
                                                                bodyje = [{
                                                                    ContraAccount: store1,
                                                                    Credit: 0,
                                                                    Debit: datatinv.value[0].DocTotal,
                                                                    ShortName: MOP
                                                                },
                                                                {
                                                                    ContraAccount: MOP,
                                                                    Credit: datatinv.value[0].DocTotal,
                                                                    Debit: 0,
                                                                    ShortName: store1
                                                                }]
                                                                console.log(bodyje)
                                                                if (!datatinv.error) {
                                                                    // jika invoice sudah terbentuk maka mmbuat journal entry
                                                                    fetch('https://hanacomp.beonesolution.com:50000/b1s/v1/JournalEntries', {
                                                                        method: 'POST',
                                                                        body: JSON.stringify({
                                                                            Reference: docitem.store,
                                                                            Reference3: docitem.docnumber,
                                                                            ReferenceDate: tglJE,
                                                                            DueDate: tglJE,
                                                                            TaxDate: tglJE,
                                                                            JournalEntryLines: bodyje
                                                                        }),
                                                                        headers: { 'Content-Type': 'application/json' },
                                                                    }).then(res1 => res1.json()
                                                                    ).then(data => {
                                                                        timeout = false;
                                                                        if (!data.error) {
                                                                            connection.query('DELETE FROM bos_log_penjualan_to Where docnumber = ?', docitem.docnumber, function (error, rows, fields) {
                                                                                if (error) {
                                                                                    var Status = [];
                                                                                    var s = {
                                                                                        'ErrorMsg': '[300]',
                                                                                        'Description': 'Failed ' + docitem.docnumber + ", Error: " + error,
                                                                                        'SuccessMsg': ''
                                                                                    }
                                                                                    Status.push(s)
                                                                                    console.log(Status)
                                                                                    res.json(Status);
                                                                                } else {
                                                                                }
                                                                            });

                                                                            var Status = [];
                                                                            var s = {
                                                                                'SuccessMsg': '[200]',
                                                                                'Description': 'Success ' + docitem.docnumber,
                                                                                'ErrorMsg': ''
                                                                            }
                                                                            Status.push(s)
                                                                            console.log(Status)
                                                                            res.json(Status);

                                                                        } else {
                                                                            var Status = [];
                                                                            var s = {
                                                                                'ErrorMsg': '[300]',
                                                                                'Description': 'Failed ' + docitem.docnumber + ", Error: " + data.error.message.value,
                                                                                'SuccessMsg': ''
                                                                            }
                                                                            console.log(docitem.docnumber)
                                                                            // INSERT KE LOG
                                                                            connection.query('INSERT INTO bos_log_penjualan_to (DOCNUMBER,REASON) values (?,?)',
                                                                                [docitem.docnumber, data.error.message.value],
                                                                                function (error, rows, fields) {
                                                                                    if (error) {
                                                                                        console.log(error)
                                                                                    } else {
                                                                                    }
                                                                                });

                                                                            Status.push(s)
                                                                            console.log(Status)
                                                                            res.json(Status);
                                                                        }
                                                                    })
                                                                        .catch(err1 => {
                                                                            timeout = true;
                                                                            var Status = [];
                                                                            var s = {
                                                                                'ErrorMsg': '[300]',
                                                                                'Description': 'Failed ' + docitem.docnumber + ", Error: " + err1.message,
                                                                                'SuccessMsg': ''
                                                                            }

                                                                            // INSERT KE LOG
                                                                            connection.query('INSERT INTO bos_log_penjualan_to (DOCNUMBER,REASON) values (?,?)',
                                                                                [docitem.docnumber, err1.message],
                                                                                function (error, rows, fields) {
                                                                                    if (error) {
                                                                                        console.log(error)
                                                                                    } else {
                                                                                    }
                                                                                });
                                                                            Status.push(s)
                                                                            res.json(Status)
                                                                            console.log(Status)
                                                                        });
                                                                }
                                                            }

                                                        }).catch(err2 => {
                                                            var Status = []
                                                            var s = {
                                                                'ErrorMsg': '[300]',
                                                                'Description': 'Failed ' + docitem.docnumber + ", Error: " + err2.message,
                                                                'SuccessMsg': ''
                                                            }

                                                            // INSERT KE LOG
                                                            connection.query('INSERT INTO bos_log_penjualan_to (DOCNUMBER,REASON) values (?,?)',
                                                                [docitem.docnumber, err2.message],
                                                                function (error, rows, fields) {
                                                                    if (error) {
                                                                        console.log(error)
                                                                    } else {
                                                                    }
                                                                });

                                                            Status.push(s)
                                                            res.json(Status)
                                                            console.log(Status)
                                                        });
                                                        // res.json(status); // akhirdari if error add invoice
                                                    } else {
                                                        var Status = []
                                                        var s = {
                                                            'ErrorMsg': '[300]',
                                                            'Description': 'Failed ' + docitem.docnumber + ", Error: " + data.error.message.value,
                                                            'SuccessMsg': ''
                                                        }

                                                        // INSERT KE LOG
                                                        connection.query('INSERT INTO bos_log_penjualan_to (DOCNUMBER,REASON) values (?,?)',
                                                            [docitem.docnumber, data.error.message.value],
                                                            function (error, rows, fields) {
                                                                if (error) {
                                                                    console.log(error)
                                                                } else {
                                                                }
                                                            });

                                                        Status.push(s)
                                                        res.json(Status);
                                                        console.log(Status)
                                                    }
                                                }).catch(err => {
                                                    timeout = true;
                                                    var Status = []
                                                    var s = {
                                                        'ErrorMsg': '[300]',
                                                        'Description': 'Failed ' + docitem.docnumber + ", Error: " + err.message,
                                                        'SuccessMsg': ''
                                                    }
                                                    Status.push(s)
                                                    res.json(Status);
                                                    console.log(Status)
                                                });
                                            }
                                        }).catch(errormop => {
                                            timeout = true;
                                            var Status = []
                                            var s = {
                                                'ErrorMsg': '[300]',
                                                'Description': 'Failed ' + docitem.docnumber + ", Error: " + errormop.message,
                                                'SuccessMsg': ''
                                            }
                                            Status.push(s)
                                            res.json(Status);
                                            console.log(Status)
                                        });
                                    }
                                }
                            });
                        }).catch(errorwhs => {
                            timeout = true;
                            var Status = []
                            var s = {
                                'ErrorMsg': '[300]',
                                'Description': 'Failed ' + docitem.docnumber + ", Error: " + errorwhs.message,
                                'SuccessMsg': ''
                            }
                            Status.push(s)
                            res.json(Status);
                            console.log(Status)
                        });
                    })
                }
            });
        })
    });
}