// const {Bigtable} = require('@google-cloud/bigtable');
const prompt = require('prompt-sync')();
const readline = require('readline').createInterface({
  input: process.stdin,
  output: process.stdout
});
const task1 = require("./queries_s1");
const task2 = require("./queries_s2");
const fs = require("fs");
const { parse } = require("csv-parse");
require('dotenv').config();
const Bigtable = require('@google-cloud/bigtable');

const bigtableOptions = {
  projectId: process.env.GCP_PROJECT_ID,
  keyFilename: process.env.CBT_KEY
};

/**
 * TASK 1:
 * SCHEMA
 * row key: device-timestamp, columns: [lf-sensors, mf-sensors]
 *                            lf-sensors:[co, light, lpg, motion]
 *                            mf-sensors:[humidity, smoke, temp]
 * e.g.
 * b8:27:eb:bf:9d:51-1.5945120943859746E9: ("0.004955938648391245","51.0","false","0.00765082227055719","false","0.02041127012241292","22.7")
 * 
 * TASK 2:
 * SCHEMA
 * row key: device, column families: [lf-sensors, mf-sensors][ts]
 *                            lf-sensors:[co, light, lpg, motion]
 *                            mf-sensors:[humidity, smoke, temp]
 * 
 * Columns:
 *  - CO: last N
 *  - humidity: latest (in timeframe t)
 *  - light: last N
 *  - lpg: last N
 *  - motion: last N
 *  - smoke: latest (in timeframe t)
 *  - temp:  latest (in timeframe t)
 */


TABLE_ID = 'SensorsTable1';
const COLUMN_FAMILY_ID = ['lf-sensors', 'mf-sensors'];
const COLUMN_QUALIFIERS_F1 = ['co', 'light', 'lpg', 'motion'];
const COLUMN_QUALIFIERS_F2 = ['humidity', 'smoke', 'temp'];


(async () => {
  const task = prompt('Input task number: ');
  console.log(`-> ${task}`);

  if(task == 1){
    console.log("Task 1")
    try {
      const bigtableClient = new Bigtable(bigtableOptions);
      const instance = bigtableClient.instance(process.env.CBT_INSTANCE);
  
      const table = instance.table(TABLE_ID);
      const [tableExists] = await table.exists();
  
      if (!tableExists) {
        console.log(`Creating table ${TABLE_ID}`);
        const options = {
          families: [
            {
              name: COLUMN_FAMILY_ID[0],
              rule: {
                versions: 5,
              },
            },
            {
              name: COLUMN_FAMILY_ID[1],
              rule: {
                versions: 5,
              },
            },
          ],
        };
        await table.create(options);
      }
  
      async function insertData(dataset){
        const rowsToInsert = dataset.map((data, index) => ({
          key: `${data['device']}#${data['ts']}`,
          data: {
            [COLUMN_FAMILY_ID[0]]: {
              ['co']: {
                value: data['co'],
              },
              ['light']: {
                value: data['light'],
              },
              ['lpg']: {
                value: data['lpg'],
              },
              ['motion']: {
                value: data['motion'],
              },
            },
            [COLUMN_FAMILY_ID[1]]: {
              ['humidity']: {
                value: data['humidity'],
              },
              ['smoke']: {
                value: data['smoke'],
              },
              ['temp']: {
                value: data['temp'],
              },
            },
          },
        }));
        await table.insert(rowsToInsert);
      }
      
      dataset = [];
      function readData(){
        fs.createReadStream("iot_telemetry_data.csv")
          .pipe(parse({ delimiter: ",", from_line: 2, to_line: 200 }))
          .on("data", function (row) {
              dataset.push(row);
          })
          .on("end", function () {
              getValues(dataset)
          })
          .on("error", function (error) {
              console.log(error.message);
          });
      }
  
      async function getValues(data){
        const keys = ['ts', 'device', 'co', 'humidity', 'light', 'lpg', 'motion', 'smoke', 'temp'];
    
        const dataset = data.map(values =>
            Object.fromEntries(
                values.map((value, index) => [keys[index], value])
            )
        );
  
        console.log('Inserting Data');
        await insertData(dataset)
  
        // await task1.query_1(table, "temp", "1.5945120943859746E9", "1.5945123089071703E9")
  
        await task1.query_2(table, "humidity", "00:0f:00:70:91:0a", "1.5945120943859746E9", "1.5945124248014004E9")
  
        // await task1.query_3(table, "00:0f:00:70:91:0a", "humidity")
  
        // await task1.query_4(table, "humidity")
        
        // console.log('Delete the table');
        // await table.delete();
       
      }
  
      readData()
  
    } catch (error) {
      console.error('Something went wrong:', error);
    }
  }

  else if(task == 2){
    TABLE_ID = "SensorsTable2";
    try {
      const bigtableClient = new Bigtable(bigtableOptions);
      const instance = bigtableClient.instance(process.env.CBT_INSTANCE);
  
      const table = instance.table(TABLE_ID);
      const [tableExists] = await table.exists();
  
      if (!tableExists) {
        console.log(`Creating table ${TABLE_ID}`);
        const options = {
          families: [
            {
              name: COLUMN_FAMILY_ID[0],
              rule: {
                versions: 5,
              },
            },
            {
              name: COLUMN_FAMILY_ID[1],
              rule: {
                age: {
                  hours: 5,
                },
              },
            },
          ],
        };
        await table.create(options);
      }
  
      async function insertData(dataset){
        const rowsToInsert = dataset.map((data, index) => ({
          key: `${data['device']}`,
          data: {
            [COLUMN_FAMILY_ID[0]]: {
              ['co']: {
                timestamp: new Date(parseFloat(data['ts'])),
                value: data['co'],
              },
              ['light']: {
                timestamp: new Date(parseFloat(data['ts'])),
                value: data['light'],
              },
              ['lpg']: {
                timestamp: new Date(parseFloat(data['ts'])),
                value: data['lpg'],
              },
              ['motion']: {
                timestamp: new Date(parseFloat(data['ts'])),
                value: data['motion'],
              },
            },
            [COLUMN_FAMILY_ID[1]]: {
              ['humidity']: {
                timestamp: new Date(parseFloat(data['ts'])),
                value: data['humidity'],
              },
              ['smoke']: {
                timestamp: new Date(parseFloat(data['ts'])),
                value: data['smoke'],
              },
              ['temp']: {
                timestamp: new Date(parseFloat(data['ts'])),
                value: data['temp'],
              },
            },
          },
        }));
        await table.insert(rowsToInsert);
      }
      
      dataset = [];
      function readData(){
        fs.createReadStream("iot_telemetry_data.csv")
          .pipe(parse({ delimiter: ",", from_line: 2, to_line: 200 }))
          .on("data", function (row) {
              dataset.push(row);
          })
          .on("end", function () {
              getValues(dataset)
          })
          .on("error", function (error) {
              console.log(error.message);
          });
      }
  
      async function getValues(data){
        const keys = ['ts', 'device', 'co', 'humidity', 'light', 'lpg', 'motion', 'smoke', 'temp'];
    
        const dataset = data.map(values =>
            Object.fromEntries(
                values.map((value, index) => [keys[index], value])
            )
        );
  
        console.log('Inserting Data');
        await insertData(dataset)

        console.log(dataset.length)

        // await task2.query_1(table, "humidity", "00:0f:00:70:91:0a", "1.5945120943859746E9", "1.5945124248014004E9")

        // await task2.query_2(table, "humidity", "00:0f:00:70:91:0a")

        // await task2.query_3(table, "humidity")
        
        console.log('Delete the table');
        await table.delete();
       
      }
  
      readData()
  
    } catch (error) {
      console.error('Something went wrong:', error);
    }
  }

  else if(task == 3){

    // JobTitle#


    TABLE_ID = "SensorsTable2";
    try {
      const bigtableClient = new Bigtable(bigtableOptions);
      const instance = bigtableClient.instance(process.env.CBT_INSTANCE);
  
      const table = instance.table(TABLE_ID);
      const [tableExists] = await table.exists();
  
      if (!tableExists) {
        console.log(`Creating table ${TABLE_ID}`);
        const options = {
          families: [
            {
              name: COLUMN_FAMILY_ID[0],
              rule: {
                versions: 5,
              },
            },
            {
              name: COLUMN_FAMILY_ID[1],
              rule: {
                age: {
                  hours: 5,
                },
              },
            },
          ],
        };
        await table.create(options);
      }
  
      async function insertData(dataset){
        const rowsToInsert = dataset.map((data, index) => ({
          key: `${data['device']}`,
          data: {
            [COLUMN_FAMILY_ID[0]]: {
              ['co']: {
                timestamp: new Date(parseFloat(data['ts'])),
                value: data['co'],
              },
              ['light']: {
                timestamp: new Date(parseFloat(data['ts'])),
                value: data['light'],
              },
              ['lpg']: {
                timestamp: new Date(parseFloat(data['ts'])),
                value: data['lpg'],
              },
              ['motion']: {
                timestamp: new Date(parseFloat(data['ts'])),
                value: data['motion'],
              },
            },
            [COLUMN_FAMILY_ID[1]]: {
              ['humidity']: {
                timestamp: new Date(parseFloat(data['ts'])),
                value: data['humidity'],
              },
              ['smoke']: {
                timestamp: new Date(parseFloat(data['ts'])),
                value: data['smoke'],
              },
              ['temp']: {
                timestamp: new Date(parseFloat(data['ts'])),
                value: data['temp'],
              },
            },
          },
        }));
        await table.insert(rowsToInsert);
      }
      
      dataset = [];
      function readData(){
        fs.createReadStream("iot_telemetry_data.csv")
          .pipe(parse({ delimiter: ",", from_line: 2, to_line: 200 }))
          .on("data", function (row) {
              dataset.push(row);
          })
          .on("end", function () {
              getValues(dataset)
          })
          .on("error", function (error) {
              console.log(error.message);
          });
      }
  
      async function getValues(data){
        const keys = ['ts', 'device', 'co', 'humidity', 'light', 'lpg', 'motion', 'smoke', 'temp'];
    
        const dataset = data.map(values =>
            Object.fromEntries(
                values.map((value, index) => [keys[index], value])
            )
        );
  
        console.log('Inserting Data');
        await insertData(dataset)

        console.log(dataset.length)

        // await task2.query_1(table, "humidity", "00:0f:00:70:91:0a", "1.5945120943859746E9", "1.5945124248014004E9")

        // await task2.query_2(table, "humidity", "00:0f:00:70:91:0a")

        // await task2.query_3(table, "humidity")
        
        console.log('Delete the table');
        await table.delete();
       
      }
  
      readData()
  
    } catch (error) {
      console.error('Something went wrong:', error);
    }
  }
})();