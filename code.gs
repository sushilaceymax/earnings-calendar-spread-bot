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
  var values = sheet.getDataRange().getValues(); // Get all existing data
  var headers = values[0]; // Get headers from the first row
  var tickerColIndex = headers.indexOf("Ticker"); // Find the 0-based index of the 'Ticker' column

  if (tickerColIndex === -1) {
    // If 'Ticker' column doesn't exist, maybe append or return error
    // For safety, let's append if header isn't found
    var rowData = headers.map(function(header) { return data[header] || ""; });
    sheet.appendRow(rowData);
    return ContentService.createTextOutput("OK - Appended (Ticker header not found)")
      .setMimeType(ContentService.MimeType.TEXT);
  }

  var targetRowIndex = -1; // 1-based sheet row index
  // Start searching from the second row (index 1 in the array)
  for (var i = 1; i < values.length; i++) {
    if (!values[i][tickerColIndex]) { // Check if the Ticker cell in this row is empty
      targetRowIndex = i + 1; // Found the row (1-based index)
      break;
    }
  }

  // Prepare the new row data based on headers
  var newRowData = headers.map(function(header) { return data[header] || ""; });

  if (targetRowIndex !== -1) {
    // Found an empty row, update it
    // getRange(row, column, numRows, numColumns)
    sheet.getRange(targetRowIndex, 1, 1, newRowData.length).setValues([newRowData]);
    return ContentService.createTextOutput("OK - Updated row " + targetRowIndex)
      .setMimeType(ContentService.MimeType.TEXT);
  } else {
    // No empty row found, append as a fallback
    sheet.appendRow(newRowData);
    return ContentService.createTextOutput("OK - Appended (no empty row found)")
      .setMimeType(ContentService.MimeType.TEXT);
  }
}

// Update a row by Ticker+Open Date (or any unique key)
function doPut(e) {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Earnings"); // Change to your sheet name
  var logMessage = ""; // Initialize log message

  if (sheet == null) {
    logMessage = "doPut called. Error: Sheet 'Earnings' not found.";
    return ContentService.createTextOutput(logMessage).setMimeType(ContentService.MimeType.TEXT);
  }

  try {
    var data = JSON.parse(e.postData.contents);
    logMessage = "doPut called. Received data: " + JSON.stringify(data);

    var headers = sheet.getDataRange().getValues()[0];
    var values = sheet.getDataRange().getValues();
    var keyTicker = data["Ticker"];
    var keyOpenDate = data["Open Date"];
    var updated = false; // Flag to track if update happened

    logMessage += ". Attempting to update using Ticker: '" + keyTicker + "' and Open Date: '" + keyOpenDate + "'";

    for (var i = 1; i < values.length; i++) {
      if (values[i][headers.indexOf("Ticker")] == keyTicker &&
          values[i][headers.indexOf("Open Date")] == keyOpenDate) {
        // Update all fields provided in data
        for (var j = 0; j < headers.length; j++) {
          if (data[headers[j]] !== undefined) {
            sheet.getRange(i+1, j+1).setValue(data[headers[j]]);
          }
        }
        logMessage += ". Match found at row " + (i+1) + ". Row updated.";
        updated = true;
        break; // Exit loop once updated
      }
    }

    if (!updated) {
        logMessage += ". No matching row found to update.";
    }

  } catch (error) {
     logMessage = "doPut called. Error processing request: " + error;
  }

  // Return log message instead of "Updated" or "Not found"
  return ContentService.createTextOutput(logMessage)
    .setMimeType(ContentService.MimeType.TEXT);
}

// CORS preflight
function doOptions(e) {
  var logMessage = "doOptions called.";
  // Return log message instead of empty string
  return ContentService.createTextOutput(logMessage)
    .setMimeType(ContentService.MimeType.TEXT);
}