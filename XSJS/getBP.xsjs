function showData(updateDate){
    $.response.contentType = "application/json";
    
    var connection = $.hdb.getConnection();
    
    //Load procedure of specified schema
    var getBP = connection.loadProcedure('ATA', 'GETBP');

    //The getBpByTypeProc object act as proxy to the procedure 
    var results = getBP(updateDate); 

    //Build the response
    $.response.status = $.net.http.OK;
    $.response.contentType = "application/json";
    $.response.setBody(JSON.stringify(results));

    connection.close();    
}

var updateDate = $.request.parameters.get("updateDate");

showData(updateDate);