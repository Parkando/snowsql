use snowsql::FromRow;

#[allow(dead_code)]
#[derive(Debug, FromRow)]
struct SnowyRow {
    client_id: String,
    client_name: String,
    site_id: String,
    site_name: String,
    num_permits: Option<i64>,
}

#[test]
fn deserialize_example() {
    let response = serde_json::from_str::<snowsql::Response<snowsql::RawRow>>(EXAMPLE)
        .expect("deserializing response");

    response
        .rows::<SnowyRow>()
        .expect("deserializing snowy rows");
}

static EXAMPLE: &str = r#"
{
  "resultSetMetaData": {
    "numRows": 2,
    "format": "jsonv2",
    "partitionInfo": [
      {
        "rowCount": 2,
        "uncompressedSize": 201
      }
    ],
    "rowType": [
      {
        "name": "CLIENT_ID",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "SPOTS_AND_AGREEMENTS",
        "byteLength": null,
        "type": "fixed",
        "scale": 0,
        "precision": 38,
        "nullable": false,
        "collation": null,
        "length": null
      },
      {
        "name": "CLIENT_NAME",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "SPOTS_AND_AGREEMENTS",
        "byteLength": 16777216,
        "type": "text",
        "scale": null,
        "precision": null,
        "nullable": true,
        "collation": null,
        "length": 16777216
      },
      {
        "name": "SITE_ID",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "SPOTS_AND_AGREEMENTS",
        "byteLength": 144,
        "type": "text",
        "scale": null,
        "precision": null,
        "nullable": true,
        "collation": null,
        "length": 36
      },
      {
        "name": "SITE_NAME",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "SPOTS_AND_AGREEMENTS",
        "byteLength": 16777216,
        "type": "text",
        "scale": null,
        "precision": null,
        "nullable": true,
        "collation": null,
        "length": 16777216
      },
      {
        "name": "NUM_PERMITS",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "SPOTS_AND_AGREEMENTS",
        "byteLength": null,
        "type": "fixed",
        "scale": 0,
        "precision": 38,
        "nullable": true,
        "collation": null,
        "length": null
      }
    ]
  },
  "data": [
    [
      "3",
      "Parkando",
      "7a7cb2b5-8f4b-4f49-9875-32576d808de2",
      "Grev Turegatan 29 - TCO-garaget",
      null
    ],
    [
      "3",
      "Parkando",
      "7a7cb2b5-8f4b-4f49-9875-32576d808de2",
      "Grev Turegatan 29 - TCO-garaget",
      null
    ]
  ],
  "code": "090001",
  "statementStatusUrl": "/api/v2/statements/01ad9ea3-3201-dca3-0000-a219000bb062?requestId=0a404baa-8f14-45f1-894c-a4f8ab7ca9de",
  "requestId": "0a404baa-8f14-45f1-894c-a4f8ab7ca9de",
  "sqlState": "00000",
  "statementHandle": "01ad9ea3-3201-dca3-0000-a219000bb062",
  "message": "Statement executed successfully.",
  "createdOn": 1689333321982
}

"#;
