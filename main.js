const fs = require('fs');
const parse =require('csv-parse');

const DATA_FILE = './data/sample.csv';

let csvData = [];
fs.createReadStream(DATA_FILE)
  .pipe(parse({ delimiter: ";" }))
  .on("data", (csvrow) => {
    console.log(csvrow);
    csvData.push(csvrow);
  })
  .on("end", () => {
    console.log(csvData.reduce( () => {
      return csvData[0];
    }));
  });
