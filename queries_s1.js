module.exports = { query_2, query_3 };

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
    console.log(`Query 2: Displaying average of ${sensor} Sensor for Device ${device}`)
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
  
  console.log(avg)
}

async function query_3(table, device, sensor){
  const prefix = `${device}#`
  await table
  .createReadStream({
    prefix,
  })
  .on('error', err => {
    // Handle the error.
    console.log(err);
  })
  .on('data', row =>  console.log(row.id + " - " + row.data['mf-sensors'][sensor][0].value))
  .on('end', () => {
    // All rows retrieved.
  });
}

