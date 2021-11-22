import fetch from 'node-fetch';
import csv from 'csvtojson';
import * as crypto from 'crypto';
import moment from 'moment';

export class CloudKit {
  constructor(options) {
    this.keyId = options.keyId;
    this.privateKey = options.privateKey;
  }

  _createFields(record, options) {
    return Object.keys(record)
      .reduce((accumulator, key) => {
        let acc = accumulator;

        acc[key] = {
          value: record[key]
        };

        return acc;
      }, {});
  }

  _createOperations(records, options) {
    const mappedRecords = records
      .map((row) => {
        return Object.keys(row)
          .reduce((accumulator, key) => {
            let acc = accumulator;
            let mappedKey = options.keyMap ? options.keyMap(key) : key;
            let mappedValue = options.valueMap ? options.valueMap(mappedKey, row[key]) : row[key];

            acc[mappedKey] = mappedValue;

            return acc;
          }, {});
      });

    return (options.prepare ? options.prepare(mappedRecords) : { recordType: options.recordType, records: mappedRecords })
      .map(({ recordType, records }) => {
        return records
          .map((record) => {
            const fields = this._createFields(record, options);
            let recordName = null;
            if (options.recordName && typeof options.recordName === "object") {
              recordName = record[options.recordName[recordType]]
            } else if (options.recordName) {
              recordName = record[options.recordName];
            }

            return {
              operationType: "create",
              record: {
                recordName: recordName,
                recordType: recordType,
                fields
              }
            };
          });
      });
  }

  async _writeOperations(allOperations, options) {
    const chunkSize = options.chunkSize || 200;
    const numberOfChunks = Math.ceil(allOperations.length / chunkSize);
    const operationCount = allOperations.length;
    let responses = [];

    for (let i = 0; i < numberOfChunks; i += 1) {
      const fromIndex = i * chunkSize;
      const toIndex = Math.min(fromIndex + chunkSize, operationCount)
      const operations = allOperations.slice(fromIndex, toIndex);

      console.log(`Writing ${fromIndex}-${toIndex} of ${operationCount} ${operations[0].record.recordType}...`);

      const requestBody = JSON.stringify({
        operations
      });
      const requestPath = `/database/1/${options.container}/${options.environment}/public/records/modify`;
      const date = moment().utc().format('YYYY-MM-DD[T]HH:mm:ss[Z]');
      const message = [
        date,
        crypto.createHash('sha256').update(requestBody).digest('base64'),
        requestPath,
      ].join(":");
      const signature = crypto.createSign('RSA-SHA256').update(message).sign(this.privateKey, 'base64');

      const requestOptions = {
        method: "POST",
        headers: {
          "X-Apple-CloudKit-Request-KeyID": this.keyId,
          "X-Apple-CloudKit-Request-ISO8601Date": date,
          "X-Apple-CloudKit-Request-SignatureV1": signature,
          "Content-Type": "application/json; charset=utf-8"
        },
        body: requestBody,
      }
      const requestUrl = `https://api.apple-cloudkit.com${requestPath}`;
      // console.log(requestUrl, JSON.stringify(requestOptions, null, 2));

      const response = await fetch(requestUrl, requestOptions);
      const data = await response.json();

      if (!response.ok) {
        console.log(data);
      } else {
        responses = responses.concat(data.records);
        console.log(JSON.stringify(data.records, null, 2));
      }
    }

    return responses;
  }

  async importCSVFile(csvPath, options) {
    return this.importRecords(await csv().fromFile(csvPath), options);
  }

  async importCSV(csvString, options) {
    return this.importRecords(await csv().fromString(csvString), options);
  }

  async importRecords(records, options) {
    const operationGroups = this._createOperations(records, options);

    let responses = [];

    for (let operations of operationGroups) {
      responses.push(await this._writeOperations(operations, options));
    }

    // console.log(JSON.stringify(responses, null, 2));
    return responses;
  }
}