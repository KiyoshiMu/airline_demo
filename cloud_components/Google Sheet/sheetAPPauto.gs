function beLate() {
  var headers = { "Accept":"application/json", 
              "Content-Type":"application/json", 
             };
  var url = 'https://us-central1-airlinegcp.cloudfunctions.net/call-wdl'
  var threshold = 0.9676245450973511
  var activeSheet = SpreadsheetApp.getActiveSheet();
  var selection = activeSheet.getSelection();

  var ranges =  selection.getActiveRangeList().getRanges();
  for (var i = 0; i < ranges.length; i++) {
    
    Logger.log(ranges[i]);
    var range = ranges[i].getA1Notation();
    Logger.log(range);
    var rows = range.split(':');
    var row1 = activeSheet.getRange(rows[0]).getRow()
    var row2 = activeSheet.getRange(rows[1]).getRow()
    var rangeName = 'A'+row1+':I'+row2;
    var ret_range = 'J'+row1+':J'+row2;
    
    // # TODO seprate the two functions
    var values = activeSheet.getRange(rangeName).getValues();
    var instances = [];
    for (var row = 0; row < values.length; row++) {
      var csv_row = values[row].join(',');
      var key = row.toString();
      instances.push({"csv_row":csv_row, "key":key});
           };
      var data = {"instances":instances};
      var payload = JSON.stringify(data);
      var options = { "method":"POST",
                     "contentType" : "application/json",
                     "headers": headers,
                     "payload" : payload};
    
    var response = JSON.parse(UrlFetchApp.fetch(url, options).getContentText());
    var preds = response.predictions;
    var results = [];
    for (var idx = 0; idx < preds.length; idx++) {
      results.push([preds[idx].logistic[0]>threshold]); // the higher the value, the more recommend you cancel a meeting
     }
    SpreadsheetApp.getActiveSpreadsheet().getRange(ret_range).setValues(results);
    
  }
  
}
