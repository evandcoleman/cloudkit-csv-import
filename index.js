import fetch from 'node-fetch';
import csv from 'csvtojson';
import * as crypto from 'crypto';
import * as EC from 'elliptic';

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
    const chunkSize = options.chunkSize || 200;
    const numberOfChunks = Math.ceil(allOperations.length / chunkSize);
    const operationCount = allOperations.length;

    for (let i = 0; i < numberOfChunks; i += 1) {
      const fromIndex = i * chunkSize;
      const toIndex = Math.min(fromIndex + chunkSize, operationCount)
      const operations = allOperations.slice(fromIndex, toIndex);

      console.log(`Writing ${fromIndex}-${toIndex} of ${operationCount} ${options.recordType}...`);

      const requestBody = JSON.stringify({
        operations
      });
      const requestPath = `/database/1/${options.container}/${options.environment}/public/records/modify`;
      const date = new Date();
      const rawSignature = [
        date.toISOString(),
        crypto.createHash('sha256').update(requestBody).digest('base64'),
        requestPath,
      ].join(":");
      const ecdsa = new EC.ec("secp256k1");
      const signature = ecdsa.sign(rawSignature, this.privateKey, { canonical: true });

      const requestOptions = {
        method: "POST",
        headers: {
          "X-Apple-CloudKit-Request-KeyID": this.keyId,
          "X-Apple-CloudKit-Request-ISO8601Date": date.toISOString(),
          "X-Apple-CloudKit-Request-SignatureV1": signature,
          "Content-Type": "application/json; charset=utf-8"
        },
        body: requestBody,
      }
      const response = await fetch(`https://api.apple-cloudkit.com${requestPath}`, requestOptions);
      const data = await response.json();

      console.log(data);
    }
  }
}