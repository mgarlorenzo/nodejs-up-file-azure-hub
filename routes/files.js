require('console-stamp')(console, { pattern: 'dd/mm/yyyy HH:MM:ss.l' });


const express = require('express');
const router = express.Router();
const path = require('path');
const fs = require('fs');
const cron = require("node-cron");
const mqtt = require('azure-iot-device-mqtt').Mqtt;
const clientFromConnectionString = require('azure-iot-device-mqtt').clientFromConnectionString;
const filePropeties = require('../functions') 


const PropertiesReader = require('properties-reader');
const properties = PropertiesReader(filePropeties);


//Files Path
const directoryPath = properties.getRaw('files.in.path');
const directoryProcessPath = properties.getRaw('files.processed.path');
const directoryErrorPath = properties.getRaw('files.error.path');
//Azure IoT HUB
const connectionString = properties.getRaw('azure.iot.connection.string');

var taskStatus = false;

console.log(directoryPath);

function getThroughPut(initDate, endDate, bytes){ 
    var differenceMs = endDate - initDate;
    //Minute
    console.log('File Metrics: ' + 'size bytes {'  + bytes + '}, duration ms {' + differenceMs +   '} bytes/ms {' + Math.round(bytes/differenceMs) + '}');
}

async function loadFile(file){
    var client = clientFromConnectionString(connectionString);
    var initDate = new Date().getTime();
    var filePath = directoryPath + '/' + file;
    var fileProcessPath = directoryProcessPath + '/' + file;
    var fileErrorPath = directoryErrorPath + '/' + file;
    fs.stat(filePath, function (err, stats) {
        const rr = fs.createReadStream(filePath);
        client.uploadToBlob(file, rr, stats.size, function (err) {
            if (err) {
                console.error('Error uploading file: ' + file);
                console.error('Error:  ' + file + err.toString());
                fs.rename(filePath, fileErrorPath, (err) => {
                    if (err) throw err;
                    console.log('File move to error:' + file);
                });
            } else {
                console.log('File uploaded: ' + file);
                fs.rename(filePath, fileProcessPath, (err) => {
                    if (err) throw err;
                    console.log('File move to processed: ' + file);
                });
                getThroughPut(initDate,new Date().getTime(),stats.size);
            }
        });
    });
}

async function checkFiles(){
    var initDate = new Date().getTime();
    fs.readdir(directoryPath, function (err, files) {
        //handling error
        if (err) {
            return console.log('Unable to scan directory: ' + err);
        } 
        
        if(files.length <1){
            console.log('No new files')
        }else{
            console.log('Totals files: ' + files.length);
            //listing all files using forEach
            //Azure Connection
            console.log('Client connected');
            files.forEach(function (file) {
                loadFile(file)
            });
        }
    });
}


var task = cron.schedule("* * * * *", () => {
  console.log("Running Cron Job - Every minute");
  checkFiles();
}, {
  scheduled: false
});

/* Stop cron task */
router.delete('/', function(req, res, next) {
  if(taskStatus)
    task.stop();
  taskStatus=false;
  res.send('{ "Status" : "Stopped" }');
});

/* Get status cron task */
router.get('/', function(req, res, next) {
  if(taskStatus){
    res.send('{ "Status" : "Running" }');
  }else{
    res.send('{ "Status" : "Stopped" }');
  }
});

/* Start cron task */
router.post('/', function(req, res, next) {
  // Uploading Files every minute
  if(taskStatus){
    res.send('{ "Status" : "Started" }');
  }else{
    task.start();
    taskStatus=true;
    res.send('{ "Status" : "Started" }');
  }
});

module.exports = router;
