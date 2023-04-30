// const {Bigtable} = require('@google-cloud/bigtable');

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

    console.log('Write some rows to the table');
    const dataset = [
      {
        'ts': 1.5945120943859746E9, 
        'device': 'b8:27:eb:bf:9d:51', 
        'co': 0.004955938648391245,
        'humidity': 51.0,
        'light': "false",
        'lpg': 0.00765082227055719,
        'motion': "false",
        'smoke': 0.02041127012241292,
        'temp': 22.7, 
      },
    ];
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

    // const filter = [
    //   {
    //     column: {
    //       cellLimit: 1, // Only retrieve the most recent version of the cell.
    //     },
    //   },
    // ];

    console.log('Reading a single row by row key');
    const [singleRow] = await table.row(`${dataset[0]['device']}-${dataset[0]['ts']}`).get();
    console.log(`\tRead: ${singleRow.data['lf-sensors'].light[0].value}`);

    // console.log('Reading the entire table');
    // const [allRows] = await table.getRows({filter});
    // for (const row of allRows) {
    //   console.log(`\tRead: ${getRowGreeting(row)}`);
    // }

    console.log('Delete the table');
    await table.delete();

  } catch (error) {
    console.error('Something went wrong:', error);
  }
  
})();
