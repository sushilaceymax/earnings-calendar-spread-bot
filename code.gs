function doGet(e) {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Earnings");
  var data = sheet.getDataRange().getValues();
  var headers = data[0];
  var rows = data.slice(1).map(function(row) {
    var obj = {};
    headers.forEach(function(header, i) {
      obj[header] = row[i];
    });
    return obj;
  });
  // Server-side filter for "OPEN" if requested
  if (e.parameter.status && e.parameter.status == "OPEN") {
    rows = rows.filter(function(row) { return row["Result"] == "OPEN"; });
  }
  return ContentService.createTextOutput(JSON.stringify(rows))
    .setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Earnings");
  // Return early if the sheet is not found
  if (sheet == null) {
    return ContentService.createTextOutput("None active").setMimeType(ContentService.MimeType.TEXT);
  }
  var data = JSON.parse(e.postData.contents);
  var headers = sheet.getDataRange().getValues()[0];
  var row = headers.map(function(header) { return data[header] || ""; });
  sheet.appendRow(row);
  return ContentService.createTextOutput("OK")
    .setMimeType(ContentService.MimeType.TEXT);
}

// Update a row by Ticker+Open Date (or any unique key)
function doPut(e) {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Earnings"); // Change to your sheet name
  var data = JSON.parse(e.postData.contents);
  var headers = sheet.getDataRange().getValues()[0];
  var values = sheet.getDataRange().getValues();
  var keyTicker = data["Ticker"];
  var keyOpenDate = data["Open Date"];
  for (var i = 1; i < values.length; i++) {
    if (values[i][headers.indexOf("Ticker")] == keyTicker &&
        values[i][headers.indexOf("Open Date")] == keyOpenDate) {
      // Update all fields provided in data
      for (var j = 0; j < headers.length; j++) {
        if (data[headers[j]] !== undefined) {
          sheet.getRange(i+1, j+1).setValue(data[headers[j]]);
        }
      }
      return ContentService.createTextOutput("Updated")
        .setMimeType(ContentService.MimeType.TEXT);
    }
  }
  return ContentService.createTextOutput("Not found")
    .setMimeType(ContentService.MimeType.TEXT);
}

// CORS preflight
function doOptions(e) {
  return ContentService.createTextOutput("")
    .setMimeType(ContentService.MimeType.TEXT);
}