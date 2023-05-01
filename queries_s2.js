module.exports = { query_1, query_2, query_3 };

async function query_1(table, sensor, device, time_start, time_end){
    console.log(`\nQuery 1: Average reading of ${sensor} sensor by a device ${device} within a certain time range (${time_start} - ${time_end})`)
        
    const prefix = device;
    const options = {
        prefix,
        filter: {
            time: {
                start: new Date(parseFloat(time_start)),
                end: new Date(parseFloat(time_end)),
            },
        },
        
    };
    
    const [rows] = await table.getRows(options);
    console.log(rows.length)
    sensorValue = []
    for (const row of rows) {
        sum = 0;
        avg = 0;
        for(i = 0; i<  row.data['mf-sensors'][sensor].length ; i++){
            sum += parseFloat(row.data['mf-sensors'][sensor][i].value)
        }
        avg = sum/(row.data['mf-sensors'][sensor].length)

        console.log(`${row.id} - Average of ${sensor} sensor: ${avg}`)        
    }

  }

  async function query_2(table, sensor, device){
    console.log(`\nQuery 2: 5 Most recent readings of ${sensor} sensor by a device ${device} `)
        
    const prefix = device;
    const options = {
        prefix,        
    };
        
    const [rows] = await table.getRows(options);
    console.log(rows.length)
    sensorValue = []
    for (const row of rows) {
        sum = 0;
        avg = 0;
        for(i = 0;  i < 5 ; i++){
            console.log(`${device} - ${parseFloat(row.data['mf-sensors'][sensor][i].value)}`)     
        }    
    }

  }

  async function query_3(table, sensor) {

    console.log(`Device with the higher average readings of a ${sensor} sensor`)
    
    // query the table for all rows
    const [allRows] = await table.getRows();
    
    // iterate through each row and update the stats for each device
    deviation = {}
    for (const row of allRows) {
      const deviceStats = [];
      for(i = 0; i< row.data['mf-sensors'][sensor].length; i++){
        deviceStats.push(parseFloat(row.data['mf-sensors'][sensor][i].value))
      }
      deviation[row.id] = dev(deviceStats)
      // update the stats for this device
    }
    console.log(deviation)
    let key = Object.keys(deviation).reduce((key, v) => deviation[v] < deviation[key] ? v : key);
    console.log(key)
  }

  function dev(arr){
    // Creating the mean with Array.reduce
    let mean = arr.reduce((acc, curr)=>{
      return acc + curr
    }, 0) / arr.length;
     
    // Assigning (value - mean) ^ 2 to every array item
    arr = arr.map((k)=>{
      return (k - mean) ** 2
    })
     
    // Calculating the sum of updated array
   let sum = arr.reduce((acc, curr)=> acc + curr, 0);
    
   // Calculating the variance
   let variance = sum / arr.length
    
   // Returning the standard deviation
   return Math.sqrt(sum / arr.length)
  }
  