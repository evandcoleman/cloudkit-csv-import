import fetch from 'node-fetch';
import csv from 'csvtojson';

export class CloudKit {
  constructor(options) {
    this.keyId = options.keyId;
    this.privateKey = options.privateKey;
  }

  _createOperations(records, options) {
    return records
      .map((row) => {
        const fields = Object.keys(row)
          .reduce((accumulator, key) => {
            let acc = accumulator;

            acc[key] = {
              value: row[key]
            };

            return acc;
          }, {});

        return {
          operationType: "create",
          record: {
            recordType: options.recordType,
            fields
          }
        };
      });
  }

  async importCSVFile(csvPath, options) {
    return this.importRecords(await csv().fromFile(csvPath), options);
  }

  async importCSV(csvString, options) {
    return this.importRecords(await csv().fromString(csvString), options);
  }

  async importRecords(records, options) {
    const allOperations = this._createOperations(records, options);
    const chunkSize = 50;
    const numberOfChunks = Math.ceil(allOperations.length / chunkSize);
    const operationCount = allOperations.length;

    for (let i = 0; i < numberOfChunks; i += 1) {
      const fromIndex = i * chunkSize;
      const toIndex = Math.min(fromIndex + chunkSize, operationCount - 1)
      const operations = allOperations.slice(fromIndex, toIndex);

      console.log(`Writing ${fromIndex}-${toIndex} of ${operationCount} ${options.recordType}...`);

      const requestOptions = {
        method: "POST",
        headers: {
          "X-Apple-CloudKit-Request-KeyID": this.keyId,
          "Content-Type": "application/json; charset=utf-8"
        },
        body: JSON.stringify({
          operations
        }),
      }
      const response = await fetch(`https://api.apple-cloudkit.com/database/1/${options.container}/${options.environment}/public/records/modify`, requestOptions);
      const data = await response.json();

      console.log(data);
    }
  }
}