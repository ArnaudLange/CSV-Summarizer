const fs = require('fs');
const parse = require('csv-parse');
const math = require('math');
const path = require('path');
const {
  first,
  times,
  isEmpty,
} = require('lodash/fp');

const files = process.argv.splice(2);

const isValidDate = (str) => !isNaN(Date.parse(str));

const isValidNumber = (str) => !isNaN(parseFloat(str));

const getLogName = (filePath) => path
  .basename(filePath)
  .replace(/\.[^/.]+$/, '.log');

const extractData = (filePath) => new Promise((resolve, reject) => {
  const data = [];

  fs.createReadStream(filePath)
    .pipe(parse({ delimiter: ';' }))
    .on('data', row => {
      data.push(row);
    })
    .on('end', () => {
      resolve(data);
    })
    .on('error', reject);
});

const analyseFile = (data) => {
  const nbColumns = first(data).length;

  // One analyser for each column
  const columnAnalysers = times(
    () => ({
      name: null,
      distinctValues: new Set(),
      nbValues: data.length,
      nbFilledValues: 0,
      nbNumbers: 0,
      nbDates: 0,
    }),
    nbColumns,
  );

  data.forEach((line, lineIndex) => {
    line.forEach((cell, columnIndex) => {
      const analyser = columnAnalysers[columnIndex];

      if (lineIndex === 0) {
        analyser.name = cell;
      }

      analyser.distinctValues.add(cell);

      if (!isEmpty(cell)) {
        analyser.nbFilledValues += 1;
      }

      if (isValidNumber(cell)) {
        analyser.nbNumbers += 1;
      }

      if (isValidDate(cell)) {
        analyser.nbDates += 1;
      }
    });
  });

  return columnAnalysers;
};

const writeResult = (outputFile, infos) => {
  const file = fs.createWriteStream(outputFile);

  infos.forEach((columnInfo, columnIndex) => {
    const fillRate = math.round(
      (columnInfo.nbFilledValues * 100) / columnInfo.nbValues
    );

    const numberRate = math.round(
      (columnInfo.nbNumbers * 100) / columnInfo.nbValues
    );

    const dateRate = math.round(
      (columnInfo.nbDates * 100) / columnInfo.nbValues
    );

    file.write(`Column: ${columnIndex}\n`);
    file.write(`Probable name: ${columnInfo.name}\n\n`);

    file.write(`Number of values: ${columnInfo.nbValues}\n`);
    file.write(`Number of filled values: ${columnInfo.nbFilledValues}\n`);

    file.write(`Filling rate: ${fillRate}%\n`);
    file.write(`Number of distinct values: ${columnInfo.distinctValues.size}\n\n`);

    file.write(`% of valid numbers: ${numberRate}%\n`);
    file.write(`% of valid dates: ${dateRate}%\n\n`);
    file.write('#########################################\n\n');
  });
};

(async () => {
  for (filePath of files) {
    const logName = getLogName(filePath);

    const csvData = await extractData(filePath);

    const columnsInfos = analyseFile(csvData);

    writeResult('output/' + logName, columnsInfos);
  }
})();
