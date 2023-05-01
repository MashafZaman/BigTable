module.exports = { query_1, query_2, query_3, query_4 };

async function query_1(table, sensor, time_start, time_end) {
  const filter = {
    column: {
      cellLimit: 1,
      familyName: 'mf-sensors',
      columnQualifier: sensor
    },
    valueRange: {
      startValue: time_start,
      endValue: time_end,
    },
  };
  let total = 0;
  let count = 0;
  const [rows] = await table.getRows({filter});
  
  for (const row of rows) {
    const cells = row.data['mf-sensors'][sensor];
    for (const cell of cells) {
      console.log(cell.value)
      total += parseFloat(cell.value);
      count++;
    }
  }
  const avg_value = total / count;
  console.log(`Average: ${avg_value}`);
}

async function query_2(table, sensor, device, time_start, time_end){
  await table
  .createReadStream({
    ranges: [
      {
        start: `${device}#${time_start}`,
        end: `${device}#${time_end}`,
      }
    ],
  })
  .on('error', err => {
    // Handle the error.
    console.log(err);
  })
  .on('data', row => getValues(row, sensor)) 
  .on('end', () => {
    console.log(`\nQuery 2: Displaying average of ${sensor} Sensor for Device ${device}`)
    takeAverage()
  });
}

values = []
async function getValues(row, sensor){
  console.log(row.id + " - " + row.data['mf-sensors'][sensor][0].value)
  values.push(parseFloat(row.data['mf-sensors'][sensor][0].value))
}

avg = 0
async function takeAverage(){
  values.forEach(item => {
    avg += item;
  });
  avg = avg/values.length
  console.log("Average: " + avg + "\n")
}

async function query_3(table, device, sensor){
  const prefix = `${device}#`
  await table
  .createReadStream({
    prefix,
  })
  .on('error', err => {
    console.log(err);
  })
  .on('data', row =>  console.log(row.id + " - " + row.data['mf-sensors'][sensor][0].value))
  .on('end', () => {
    console.log("Completed\n")
  });
}

async function query_4(table, sensor) {
  const deviceStats = {};
  
  // query the table for all rows
  const [allRows] = await table.getRows();
  
  // iterate through each row and update the stats for each device
  for (const row of allRows) {
    const [device, timestamp] = row.id.split('#');
    const sensor_value = row.data['mf-sensors'][sensor][0].value;
    // update the stats for this device
    if (!deviceStats[device]) {
      deviceStats[device] = { sum: 0, count: 0 };
    }
    deviceStats[device].sum += parseFloat(sensor_value);
    deviceStats[device].count++;
  }
  
  // find the device with the higher average temp readings
  let maxDevice = null;
  let maxAvgTemp = -Infinity;
  console.log("Averages:")
  for (const device in deviceStats) {
    const avgTemp = deviceStats[device].sum / deviceStats[device].count;
    console.log(device+ " --> "+ avgTemp)
    if (avgTemp > maxAvgTemp) {
      maxDevice = device;
      maxAvgTemp = avgTemp;
    }
  }
  
  console.log( `\nDevice with the higher average readings of ${sensor} sensor: ` + maxDevice + "\n");
}
