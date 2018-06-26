const fs = require("fs");
const parse = require("csv-parse");
const moment = require("moment");
const math = require("math");
const path = require("path");

const args = process.argv.splice(2);

const transpose = array => {
  const width = array.length || 0;
  const height = array[0] instanceof Array ? array[0].length : 0;

  if (height === 0 || width === 0) {
    return [];
  }

  const i, j, t = [];

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

args.forEach(dataFile => {
  const logName = path.basename(dataFile).replace(/\.[^/.]+$/, '.log');
  const csvData = [];
  fs.createReadStream(dataFile)
    .pipe(parse({ delimiter: ";" }))
    .on("data", csvrow => {
      csvData.push(csvrow);
    })
    .on("end", () => {
      var stream = fs.createWriteStream('output/' + logName);
      transpose(csvData).forEach((el, i) => {
        const filled = el.filter(String);
        const fillRate = math.round((filled.length * 100) / el.length);
        const dist = filled.filter(onlyUnique).length;

        const numberRate = math.round(
          (filled.filter(Number).length * 100) / (filled.length || 1)
        );
        const momentRate = math.round(
          (filled.reduce((p, c) => {
            return moment(c).isValid() ? p + 1 : 0;
          }, 0) *
            100) /
            (filled.length || 1)
        );

        stream.write("Column: " + i);
        stream.write("\nProbable name: " + el[0]);
        stream.write("\n\nNumber of values: " + el.length);
        stream.write("\nNumber of filled values: " + filled.length);
        stream.write("\nFilling rate: " + fillRate + "%");
        stream.write("\nNumber of distinct values: " + dist);
        stream.write("\n\n% of numbers: " + numberRate + "%");
        stream.write("\n% of valid moment: " + momentRate + "%");
        stream.write("\n\n#########################################\n\n");
      });
    });
});
