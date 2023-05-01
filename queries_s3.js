module.exports = { query_1, query_2, query_3, query_4, query_5, query_6 };

async function query_1(table, job_title){
    values = [];
    console.log(`\nQuery 1: Highest Average Salary of Job Title '${job_title}'`)
    const prefix = job_title + '#';
    const options = {
      prefix,
    };
    const [rows] = await table.getRows(options);
    for (const row of rows) {
      const value = parseInt(row.data['jd']['avg_salary'][0].value);
      values.push(value)
    }

    max = values[0]

    for (let i = 1; i < values.length; i++) {
        if (values[i] > max) {
          max = values[i];
        }
      }    
    console.log("Highest Average Salary: " + max)
}

async function query_2(table, job_title, location){
    values = [];
    console.log(`\nQuery 2: Highest Average Salary of Job Title '${job_title}', in Location '${location}'`)
    const prefix = job_title + '#' + location + '#';
    const options = {
      prefix,
    };
    const [rows] = await table.getRows(options);
    for (const row of rows) {
      const value = parseInt(row.data['jd']['avg_salary'][0].value);
      values.push(value)
    }

    max = values[0]

    for (let i = 1; i < values.length; i++) {
        if (values[i] > max) {
            max = values[i];
            console.log(max)
        }
    }    
    console.log("Highest Average Salary: " + max)
}

async function query_3(table){
    console.log(`\nQuery 3: Locations with the lowest average salary`)
    const [allRows] = await table.getRows();
    min = 100000000
    min_location = []
    temp_location = ""

    for (const row of allRows) {
        const [job_title, location, industry, company] = row.id.split('#');
        const avg_salary = parseInt(row.data['jd']['avg_salary'][0].value);
        if (avg_salary < min) {
            min = avg_salary;
            temp_location = location;
            min_location.length = 0;
        }
        else if (avg_salary == min){
            min_location.push(location)
        }
    }
    min_location.push(temp_location)
    console.log(`\nLocation with the lowest average salary (${min}): ${min_location}`)
}

async function query_4(table, job_industry){
    console.log(`\nQuery 4: No of jobs in the ${job_industry} industry`)
    const [allRows] = await table.getRows();
    counter = 0

    for (const row of allRows) {
        const [job_title, location, industry, company] = row.id.split('#');
        if(industry == job_industry){
            counter++
        }
    }

    console.log(`There are ${counter} jobs in the ${job_industry} Industry`)
}

async function query_5(table){
    console.log(`\nQuery 5: Jobs with the highest average salary`)
    const [allRows] = await table.getRows();
    max = 0
    max_job = []
    temp_job = "";

    for (const row of allRows) {
        const [job_title, location, industry, company] = row.id.split('#');
        const avg_salary = parseInt(row.data['jd']['avg_salary'][0].value);
        if (avg_salary > max) {
            max = avg_salary;
            temp_job = job_title;
            max_job.length = 0;
        }
        else if (avg_salary == max){
            max_job.push(job_title)
        }
    }
    max_job.push(temp_job)
    console.log(`\nJob with the highest average salary (${max}): ${max_job}`)
}

async function query_6(table){
    console.log(`\nQuery 6: The most widely used job title`);
  
    const jobTitleCount = {};
  
    const [allRows] = await table.getRows();
  
    for (const row of allRows) {
      const rowKey = row.id;
      const jobTitle = rowKey.split('#')[0];
      jobTitleCount[jobTitle] = (jobTitleCount[jobTitle] || 0) + 1;
    }
  
    let mostCommonJobTitle = null;
    let maxCount = 0;
    for (const jobTitle in jobTitleCount) {
      if (jobTitleCount[jobTitle] > maxCount) {
        maxCount = jobTitleCount[jobTitle];
        mostCommonJobTitle = jobTitle;
      }
    }
  
    console.log(`The most widely used job title is "${mostCommonJobTitle}"`);
  }
