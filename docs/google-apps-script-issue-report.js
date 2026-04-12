/**
 * Google Apps Script for receiving Vireo issue reports.
 *
 * Setup:
 * 1. Go to https://script.google.com and create a new project.
 * 2. Paste this code into Code.gs.
 * 3. Replace YOUR_EMAIL@gmail.com with your email address.
 * 4. Click Deploy > New deployment > Web app.
 *    - Execute as: Me
 *    - Who has access: Anyone
 * 5. Copy the deployment URL and set it as "report_url" in Vireo's
 *    Settings page or in ~/.vireo/config.json.
 */

function doPost(e) {
  var data = JSON.parse(e.postData.contents);
  var desc = (data.description || '').substring(0, 80);
  var subject = 'Vireo Issue Report: ' + desc;

  // Format body as readable text
  var lines = [
    'DESCRIPTION',
    data.description || '(none)',
    '',
    'VIREO VERSION: ' + (data.vireo_version || 'unknown'),
    'TIMESTAMP: ' + (data.timestamp || 'unknown'),
    '',
    'SYSTEM',
    JSON.stringify(data.system || {}, null, 2),
    '',
    'APP STATE',
    JSON.stringify(data.app_state || {}, null, 2),
    '',
    'RECENT JOBS',
    JSON.stringify(data.recent_jobs || [], null, 2),
    '',
    'CONFIG',
    JSON.stringify(data.config || {}, null, 2),
    '',
    'RECENT LOGS (last 200 entries)',
    JSON.stringify(data.logs || [], null, 2),
  ];

  GmailApp.sendEmail('YOUR_EMAIL@gmail.com', subject, lines.join('\n'));
  return ContentService.createTextOutput('ok');
}
