function showData(date){
    $.response.contentType = "application/json";
    
    var connection = $.hdb.getConnection();
    
    //Load procedure of specified schema
    var getWhs = connection.loadProcedure('ATA', 'GETSTORE');

    //The getBpByTypeProc object act as proxy to the procedure 
    var results = getWhs(date); 

    //Build the response
    $.response.status = $.net.http.OK;
    $.response.contentType = "application/json";
    $.response.setBody(JSON.stringify(results));

    connection.close();    
}

var date = $.request.parameters.get("date");

showData(date);