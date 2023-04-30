// const {Bigtable} = require('@google-cloud/bigtable');

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


const TABLE_ID = 'SensorsTable1';
const COLUMN_FAMILY_ID = ['lf-sensors', 'mf-sensors'];
const COLUMN_QUALIFIERS_F1 = ['co', 'light', 'lpg', 'motion'];
const COLUMN_QUALIFIERS_F2 = ['humidity', 'smoke', 'temp'];

const getRowGreeting = row => {
  return row.data[COLUMN_FAMILY_ID][COLUMN_QUALIFIERS_F1][0].value;
};

(async () => {
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
              versions: 1,
            },
          },
          {
            name: COLUMN_FAMILY_ID[1],
            rule: {
              versions: 1,
            },
          },
        ],
      };
      await table.create(options);
    }

    async function insertData(dataset){
      const rowsToInsert = dataset.map((data, index) => ({
        key: `${data['device']}-${data['ts']}`,
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
  
      // console.log(dataset)
      console.log('Inserting Data');
      insertData(dataset)

      console.log('Reading a single row by row key');
      const [singleRow] = await table.row(`${dataset[0]['device']}-${dataset[0]['ts']}`).get();
      console.log(`\tRead: ${parseFloat(singleRow.data['lf-sensors'].co[0].value)}`);

      // console.log('Delete the table');
      // await table.delete();
    }

    readData()

    

    // const filter = [
    //   {
    //     column: {
    //       cellLimit: 1, // Only retrieve the most recent version of the cell.
    //     },
    //   },
    // ];

    // const filter = [
    //   {
    //     valueTransformer: {
    //       encode: (floatValue) => {
    //         const buffer = new ArrayBuffer(4);
    //         const view = new DataView(buffer);
    //         view.setFloat32(0, floatValue);
    //         return Buffer.from(buffer);
    //       },
    //       decode: (bytes) => {
    //         const buffer = new ArrayBuffer(4);
    //         const view = new DataView(buffer);
    //         for (let i = 0; i < 4; i++) {
    //           view.setUint8(i, bytes[i]);
    //         }
    //         return view.getFloat32(0);
    //       },
    //     },
    //   }
    // ];

    

    // console.log('Reading the entire table');
    // const [allRows] = await table.getRows({filter});
    // for (const row of allRows) {
    //   console.log(`\tRead: ${getRowGreeting(row)}`);
    // }
    

    
    


  } catch (error) {
    console.error('Something went wrong:', error);
  }
  
})();