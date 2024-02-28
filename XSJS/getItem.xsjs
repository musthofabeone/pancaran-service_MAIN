function showData(){
    $.response.contentType = "application/json";
    
    var connection = $.hdb.getConnection();
    
    //Load procedure of specified schema
    var getItem = connection.loadProcedure('ATA', 'GETITEM');

    //The getBpByTypeProc object act as proxy to the procedure 
    var results = getItem(); 

    //Build the response
    $.response.status = $.net.http.OK;
    $.response.contentType = "application/json";
    $.response.setBody(JSON.stringify(results));

    connection.close();    
}

showData();