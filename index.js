import fetch from 'node-fetch';
import csv from 'csvtojson';
import * as crypto from 'crypto';
import moment from 'moment';
import PQueue from 'p-queue';
import pRetry from 'p-retry';

export class CloudKit {
  constructor(options) {
    this.keyId = options.keyId;
    this.privateKey = options.privateKey;
    this.queue = new PQueue(options.pQueue || {
      concurrency: 200,
      interval: 2000,
      intervalCap: 400,
    });
  }

  _createFields(record, options) {
    return Object.keys(record)
      .reduce((accumulator, key) => {
        let acc = accumulator;

        if (key !== "recordName") {
          acc[key] = {
            value: record[key]
          };
        }

        return acc;
      }, {});
  }

  _createOperations(records, operationType, options) {
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

    return (options.prepare ? options.prepare(mappedRecords) : mappedRecords)
      .map((record) => {
        const fields = operationType.toLowerCase().indexOf("delete") < 0 ? this._createFields(record, options) : null;
        let recordName = null;
        if (options.recordName && typeof options.recordName === "object") {
          recordName = record[options.recordName[options.recordType]];
        } else if (options.recordName && typeof options.recordName === "function") {
          recordName = options.recordName(record);
        } else if (options.recordName) {
          recordName = record[options.recordName];
        } else if (record.recordName) {
          recordName = record.recordName;
        }

        return {
          operationType,
          record: {
            recordName: recordName,
            recordType: options.recordType,
            fields: fields
          }
        };
      });
  }

  async _postOperations(operations, options) {
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
      throw new Error(`Failed writing (${response.status}): ` + JSON.stringify(data, null, 2));
    } else {
      for (let response of data.records) {
        if (response.serverErrorCode) {
          throw new Error("Failed writing: " + JSON.stringify(response, null, 2));
        }
      }

      return data.records;
    }
  }

  async _enqueueWriteOperations(allOperations, options) {
    const chunkSize = options.chunkSize || 200;
    const numberOfChunks = Math.ceil(allOperations.length / chunkSize);
    const operationCount = allOperations.length;
    let promises = []

    console.log(`Writing ${operationCount} records...`);

    for (let i = 0; i < numberOfChunks; i += 1) {
      const fromIndex = i * chunkSize;
      const toIndex = Math.min(fromIndex + chunkSize, operationCount)
      const operations = allOperations.slice(fromIndex, toIndex);

      const work = async function() {
        if (process.env.DEBUG) {
          console.log(`Writing ${fromIndex}-${toIndex} of ${operationCount} ${operations[0].record.recordType}...`);
        }

        return this._postOperations(operations, options);
      };

      promises.push(this.queue.add(() => pRetry(work.bind(this), { retries: 2 })));
    }

    return Promise.all(promises)
      .then((values) => values.flat());
  }

  async importCSVFile(csvPath, options) {
    return this.importRecords(await csv().fromFile(csvPath), options);
  }

  async importCSV(csvString, options) {
    return this.importRecords(await csv().fromString(csvString), options);
  }

  async importRecords(records, options) {
    const operations = this._createOperations(records, "forceUpdate", options);

    const responsesPromise = this._enqueueWriteOperations(operations, options);

    await this.queue.onIdle();

    return responsesPromise;
  }

  async removeRecords(records, options) {
    const operations = this._createOperations(records, "forceDelete", options);

    const responsesPromise = this._enqueueWriteOperations(operations, options);

    await this.queue.onIdle();

    return responsesPromise;
  }
}