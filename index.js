const fs = require('fs');
const parse = require('csv-parse');
const math = require('math');
const path = require('path');

const files = process.argv.splice(2);

const transpose = array => {
  const width = array.length || 0;
  const height = array[0] instanceof Array ? array[0].length : 0;

  if (height === 0 || width === 0) {
    return [];
  }

  let i = 0, j = 0, t = [];

  for (i = 0; i < height; i++) {
    t[i] = [];

    for (j = 0; j < width; j++) {
      t[i][j] = array[j][i];
    }
  }
  return t;
};

const onlyUnique = (value, index, self) => {
  return self.indexOf(value) === index;
};

const isValidDate = (str) => !isNaN(Date.parse(str));

const getLogName = (filePath) => path
  .basename(filePath)
  .replace(/\.[^/.]+$/, '.log');

files.forEach((filePath) => {
  const logName = getLogName(filePath);
  const csvData = [];

  fs.createReadStream(filePath)
    .pipe(parse({ delimiter: ';' }))
    .on('data', csvrow => {
      csvData.push(csvrow);
    })
    .on('end', () => {
      var stream = fs.createWriteStream('output/' + logName);
      transpose(csvData).forEach((el, i) => {
        const filled = el.filter(String);
        const fillRate = math.round((filled.length * 100) / el.length);
        const dist = filled.filter(onlyUnique).length;

        const numberRate = math.round(
          (filled.filter(Number).length * 100) / (filled.length || 1)
        );
        const dateRate = math.round(
          (filled.reduce((p, c) => {
            return isValidDate(c) ? p + 1 : 0;
          }, 0) *
            100) /
            (filled.length || 1)
        );

        stream.write('Column: ' + i);
        stream.write('\nProbable name: ' + el[0]);
        stream.write('\n\nNumber of values: ' + el.length);
        stream.write('\nNumber of filled values: ' + filled.length);
        stream.write('\nFilling rate: ' + fillRate + '%');
        stream.write('\nNumber of distinct values: ' + dist);
        stream.write('\n\n% of numbers: ' + numberRate + '%');
        stream.write('\n% of valid date: ' + dateRate + '%');
        stream.write('\n\n#########################################\n\n');
      });
    });
});
