module.exports = { query_1, query_2, query_3, query_4 };

async function query_1(table, sensor, time_start, time_end) {
    
  console.log(`\n Query 1: Average reading of ${sensor} sensor within a certain time range (${time_start} - ${time_end})`)
  const [allRows] = await table.getRows();
  sum = 0
  counter = 0
  for (const row of allRows) {
    const [device, timestamp] = row.id.split('#');
    const sensor_value = row.data['mf-sensors'][sensor][0].value;

    if(parseFloat(timestamp) >= parseFloat(time_start) && parseFloat(timestamp) <= parseFloat(time_end) ){
      console.log("timestamp: " + timestamp + " -> "+ sensor_value)
      sum += parseFloat(sensor_value)
      counter += 1
    }    
  }

  avg = sum/counter
  console.log("\nAverage: " + avg + "\n")
}

async function query_2(table, sensor, device, time_start, time_end){
  console.log(`\nQuery 2: Average reading of ${sensor} sensor by a device ${device} within a certain time range (${time_start} - ${time_end})`)
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
  console.log("\nAverage: " + avg + "\n")
}

async function query_3(table, device, sensor){

  console.log(`\nMost recent 5 readings of ${sensor} sensor by device ${device}`)
  const prefix = device + '#';
  const options = {
    prefix,
    filter: {
      
      column: {
        cellLimit: 1,
        familyName: 'mf-sensors',
        columnQualifier: sensor
      },
    },
    
  };
  const [rows] = await table.getRows(options);
  sensorValue = []
  for (const row of rows) {
    const timestamp = row.id.split('#')[1];
    const value = row.data['mf-sensors'][sensor][0].value;
    sensorValue.push(value)
  }
  i = 0;
  for(i = sensorValue.length-1 ; i >= sensorValue.length - 5; i--){
    console.log(`Sensor: ${sensor}  Value: ${sensorValue[i]}`);
  }

}

async function query_4(table, sensor) {

  console.log(`Device with the higher average readings of a ${sensor} sensor`)
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
  console.log("\nAverages:")
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
