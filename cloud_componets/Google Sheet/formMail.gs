function onFormSubmit(e) {
  var values = e.namedValues;
//  for (Key in values) {
//    var label = Key;
//    var data = values[Key];
//    Logger.log(data);
//  };
    var data = {'MKT_UNIQUE_CARRIER':values['Your Carrier'][0],
                'DEP_DELAY':getMin(values['How long Department Delay?'][0]), 
              'DISTANCE':values['The flights Distance (KM)'][0], 
               'dep_lat':values['Start Latitude'][0], 
              'dep_lng':values['Start Longitude'][0], 
              'arr_lat':values['Destination Latitude'][0], 
              'arr_lng':values['Destination Longitude'][0], 
              'month':values['Department Date'][0].split('/')[0], 
               'hour':handelTime(values['Department Time'][0])
               };
  var line = [];
  for (Key in data) {
    var row = data[Key];
    line.push(row);
  };
  Logger.log(data);
  var belate = beLateCus(line.join(','));
  Logger.log(belate);
  sendMail(values['Email Address'][0], belate)
}

//function start() {
//var line='G4,2,100,22,22,22,22,10,2';
//var ret = beLateCus(line);
//  Logger.log(ret);
//}

function sendMail(emailAddress, belate) {
  if (belate) {
    var subject = 'We Suggest You Cancel Your Meeting';
    var message = 'You are very likely not able to have your meeting in time. DO IT in next time. Good Luck!';
  }
  else{
    var subject = 'We Suggest You Keep Your Meeting';
    var message = 'You are very likely to have your meeting in time. Enjoy this Meeting! Good Luck!';
  }
  MailApp.sendEmail(emailAddress, subject, message)
}

function beLateCus(line) {
  var headers = { "Accept":"application/json", 
              "Content-Type":"application/json", 
             };
  var url = 'https://us-central1-airlinegcp.cloudfunctions.net/call_cusml';
  var threshold = 0.8899832367897034;
    var instances = [line];
    var data = {"instances":instances};
    var payload = JSON.stringify(data);
    var options = { "method":"POST",
                     "contentType" : "application/json",
                     "headers": headers,
                     "payload" : payload};

    var response = JSON.parse(UrlFetchApp.fetch(url, options).getContentText());
    var preds = response.predictions;
    var result = preds[0].scores[1]>threshold
    return result
  }


function handelTime(n) {
  var time = n.split(' ');
  if (time[1] == 'PM') {
  var addtime = 12}
  else{
  var addtime = 0};
  var hour = Number(time[0].split(':')[0]) + addtime;
  return hour;
}
function getMin(t) {
  return String(Number(t.split(':')[1]))}
