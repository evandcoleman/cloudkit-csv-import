import { CloudKit } from './index';

test('creates proper operations', () => {
  const cloudkit = new CloudKit({
    keyId: "mockKeyId",
    privateKey: "mockPrivateKey",
  });
  const records = [
    {
      key1: "value1",
      key2: "value2",
      key3: 5,
    },
    {
      key1: "value3",
      key2: "value4",
      key3: 15,
    }
  ];
  const body = cloudkit._createOperations(records, {
    recordType: "RecordType",
  });

  expect(body).toStrictEqual([
    {
      "operationType": "create",
      "record": {
        "recordType": "RecordType",
        "fields": {
          "key1": {
            "value": "value1"
          },
          "key2": {
            "value": "value2"
          },
          "key3": {
            "value": 5
          }
        }
      }
    },
    {
      "operationType": "create",
      "record": {
        "recordType": "RecordType",
        "fields": {
          "key1": {
            "value": "value3"
          },
          "key2": {
            "value": "value4"
          },
          "key3": {
            "value": 15
          }
        }
      }
    }
  ]);
});