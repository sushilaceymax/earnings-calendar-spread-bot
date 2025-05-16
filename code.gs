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
  if (!sheet) {
    return ContentService.createTextOutput("None active").setMimeType(ContentService.MimeType.TEXT);
  }
  var data = JSON.parse(e.postData.contents);

  // Handle update action via JSON 'action' flag
  if (data.action === "update") {
    var values = sheet.getDataRange().getValues();
    var headers = values[0];
    var keyTicker = data["Ticker"];
    var keyOpenDate = data["Open Date"];
    var updated = false;
    var logMessage = "doPost update called. Received data: " + JSON.stringify(data);
    logMessage += ". Attempting to update using Ticker: '" + keyTicker + "' and Open Date: '" + keyOpenDate + "'";
    for (var i = 1; i < values.length; i++) {
      if (values[i][headers.indexOf("Ticker")] == keyTicker &&
          values[i][headers.indexOf("Open Date")] == keyOpenDate) {
        // Only update columns A–L (first 12 headers)
        var colsToUpdate = Math.min(headers.length, 12);
        for (var j = 0; j < colsToUpdate; j++) {
          var header = headers[j];
          if (data[header] !== undefined) {
            sheet.getRange(i + 1, j + 1).setValue(data[header]);
          }
        }
        logMessage += ". Match found at row " + (i + 1) + ". Row updated.";
        updated = true;
        break;
      }
    }
    if (!updated) {
      logMessage += ". No matching row found to update.";
    }
    return ContentService.createTextOutput(logMessage).setMimeType(ContentService.MimeType.TEXT);
  }

  // Handle create/append action
  var values = sheet.getDataRange().getValues();
  var headers = values[0];
  var colsToWrite = Math.min(headers.length, 12);
  var tickerColIndex = headers.indexOf("Ticker");
  if (tickerColIndex === -1) {
    var rowData = headers.slice(0, colsToWrite).map(function(header) { return data[header] || ""; });
    sheet.appendRow(rowData);
    return ContentService.createTextOutput("OK - Appended (Ticker header not found)").setMimeType(ContentService.MimeType.TEXT);
  }
  var targetRowIndex = -1;
  for (var i = 1; i < values.length; i++) {
    if (!values[i][tickerColIndex]) {
      targetRowIndex = i + 1;
      break;
    }
  }
  var newRowData = headers.slice(0, colsToWrite).map(function(header) { return data[header] || ""; });
  if (targetRowIndex !== -1) {
    sheet.getRange(targetRowIndex, 1, 1, colsToWrite).setValues([newRowData]);
    return ContentService.createTextOutput("OK - Updated row " + targetRowIndex).setMimeType(ContentService.MimeType.TEXT);
  } else {
    sheet.appendRow(newRowData);
    return ContentService.createTextOutput("OK - Appended (no empty row found)").setMimeType(ContentService.MimeType.TEXT);
  }
}

// CORS preflight
function doOptions(e) {
  var logMessage = "doOptions called.";
  return ContentService.createTextOutput(logMessage)
    .setMimeType(ContentService.MimeType.TEXT);
}