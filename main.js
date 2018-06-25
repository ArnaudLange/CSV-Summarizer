const fs = require("fs");
const parse = require("csv-parse");
const moment = require('moment');

const DATA_FILE = "./data/sample.csv";

const transpose = array => {
  // Calculate the width and height of the Array
  var width = array.length || 0;
  var height = array[0] instanceof Array ? array[0].length : 0;

  // In case it is a zero matrix, no transpose routine needed.
  if (height === 0 || width === 0) {
    return [];
  }

  /**
   * @var {Number} i Counter
   * @var {Number} j Counter
   * @var {Array} t Transposed data is stored in this array.
   */
  var i,
    j,
    t = [];

  // Loop through every item in the outer array (height)
  for (i = 0; i < height; i++) {
    // Insert a new row (array)
    t[i] = [];

    // Loop through every item per item in outer array (width)
    for (j = 0; j < width; j++) {
      // Save transposed data.
      t[i][j] = array[j][i];
    }
  }

  return t;
};

const onlyUnique = (value, index, self) => {
  return self.indexOf(value) === index;
}

const csvData = [];
fs.createReadStream(DATA_FILE)
  .pipe(parse({ delimiter: ";" }))
  .on("data", csvrow => {
    console.log(csvrow);
    csvData.push(csvrow);
  })
  .on("end", () => {
    transpose(csvData).forEach((el, i) => {
      const filled = el.filter(String);
      const fillRate = filled.length / el.length;
      const dist = filled.filter(onlyUnique).length;

      const numberRate = filled.filter(Number).length / filled.length;
      const momentRate = filled.reduce((p, c) => {
        return ((moment(c).isValid()) ? p + 1 : 0);
      }, 0) / filled.length;

      console.log('#########################');
      console.log('Column: ' + i);
      console.log('Probable name: ' + el[0]);
      console.log('\nFilling rate: ' + fillRate);
      console.log('Number of distinct values: ' + dist);
      console.log('\n% of numbers: ' + numberRate);
      console.log('% of valid moment: ' + momentRate);
    })
  });
