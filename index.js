// const {Bigtable} = require('@google-cloud/bigtable');
const prompt = require('prompt-sync')();
const readline = require('readline').createInterface({
  input: process.stdin,
  output: process.stdout
});
const task1 = require("./queries_s1");
const task2 = require("./queries_s2");
const task3 = require("./queries_s3");

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
 * 
 * 
 * TASK 3:
 * [job_title, salary_estimate, job_desc, rating, company_name, location, hq, size, ownership, industry]
 * SCHEMA
 * row key: job_title, location, industry, company_name
 * column families: jd, cd, skls
 * ['Job Title','Salary Estimate','Job Description','Rating','Company Name','Location','Headquarters','Size','Type of ownership','Industry','Sector','Revenue','min_salary','max_salary','avg_salary','job_state','same_state','company_age','python','excel','hadoop','spark','aws','tableau','big_data','job_simp','seniority']
 * 
 * Column [jd]: 
 * 'Job Title','Salary Estimate','Job Description', 'min_salary','max_salary','avg_salary','job_state','same_state','job_simp','seniority', 
 * 
 * Column [cd]: 
 * 'Rating','Company Name','Location','Headquarters','Size','Type of ownership','Industry','Sector','Revenue','company_age',
 * 
 * Column [skls]
 *  'python','excel','hadoop','spark','aws','tableau','big_data'
 */


TABLE_ID = 'SensorsTable1';
COLUMN_FAMILY_ID = ['lf-sensors', 'mf-sensors'];
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
  
        // await task1.query_2(table, "humidity", "00:0f:00:70:91:0a", "1.5945120943859746E9", "1.5945124248014004E9")
  
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

    TABLE_ID = "JobPostings";
    COLUMN_FAMILY_ID = ['jd', 'cd', 'skls']
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
                versions: 5
              },
            },
            {
              name: COLUMN_FAMILY_ID[1],
              rule: {
                versions: 5
              },
            },
            {
              name: COLUMN_FAMILY_ID[2],
              rule: {
                versions: 5
              },
            },
          ],
        };
        await table.create(options);
      }
  
      async function insertData(dataset){
        const rowsToInsert = dataset.map((data, index) => ({
          // job_title, location, Industry, Company Name
          key: `${data['Job Title']}#${data['Location']}#${data['Industry']}#${data['Company Name']}`,
          data: {
            [COLUMN_FAMILY_ID[0]]: {
              // 'Job Title','Salary Estimate','Job Description', 'min_salary','max_salary','avg_salary','job_state','same_state','job_simp','seniority',               
              ['Job Title']: {
                value: data['Job Title'],
              },
              ['Salary Estimate']: {
                value: data['Salary Estimate'],
              },
              ['Job Description']: {
                value: data['Job Description'],
              },
              ['min_salary']: {
                value: data['min_salary'],
              },
              ['max_salary']: {
                value: data['max_salary'],
              },
              ['avg_salary']: {
                value: data['avg_salary'],
              },
              ['job_state']: {
                value: data['job_state'],
              },
              ['same_state']: {
                value: data['same_state'],
              },
              ['job_simp']: {
                value: data['job_simp'],
              },
              ['seniority']: {
                value: data['seniority'],
              },
            },
            [COLUMN_FAMILY_ID[1]]: {
              // 'Rating','Company Name','Location','Headquarters','Size','Type of ownership','Industry','Sector','Revenue','company_age',
              ['Rating']: {
                value: data['Rating'],
              },
              ['Company Name']: {
                value: data['Company Name'],
              },
              ['Location']: {
                value: data['Location'],
              },
              ['Headquarters']: {
                value: data['Headquarters'],
              },
              ['Size']: {
                value: data['Size'],
              },
              ['Type of ownership']: {
                value: data['Type of ownership'],
              },
              ['Industry']: {
                value: data['Industry'],
              },
              ['Sector']: {
                value: data['Sector'],
              },
              ['Revenue']: {
                value: data['Revenue'],
              },
              ['company_age']: {
                value: data['company_age'],
              },
            },
            [COLUMN_FAMILY_ID[2]]: {
              // 'python','excel','hadoop','spark','aws','tableau','big_data'
              ['python']: {
                value: data['python'],
              },
              ['excel']: {
                value: data['excel'],
              },
              ['hadoop']: {
                value: data['hadoop'],
              },
              ['spark']: {
                value: data['spark'],
              },
              ['aws']: {
                value: data['aws'],
              },
              ['tableau']: {
                value: data['tableau'],
              },
              ['big_data']: {
                value: data['big_data'],
              },
            },
          },
        }));
        await table.insert(rowsToInsert);
      }
      
      dataset = [];
      function readData(){
        fs.createReadStream("Cleaned_DS_Jobs.csv")
          .pipe(parse({ delimiter: ",", from_line: 2}))
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
        const keys = ['Job Title','Salary Estimate','Job Description','Rating','Company Name','Location','Headquarters','Size','Type of ownership','Industry','Sector','Revenue','min_salary','max_salary','avg_salary','job_state','same_state','company_age','python','excel','hadoop','spark','aws','tableau','big_data','job_simp','seniority'];
    
        const dataset = data.map(values =>
          Object.fromEntries(
            values.map((value, index) => [keys[index], value])
          )
        );
  
        console.log('Inserting Data');
        await insertData(dataset)

        console.log(dataset.length + " values inserted")

        // await task3.query_1(table, "Data Scientist")

        // await task3.query_2(table, "Data Scientist", "Philadelphia, PA")

        // await task3.query_3(table)

        // await task3.query_4(table, "Insurance Carriers")

        await task3.query_5(table)

        console.log('Delete the table');
        await table.delete();
       
      }
  
      readData()
  
    } catch (error) {
      console.error('Something went wrong:', error);
    }
  }
})();