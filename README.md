# push-to-bigquery
Push JSON documents to BigQuery

## Overview

Manages schema while pushing random documents to Google's BigQuery

## Benefits

1. Allow numerous independent processes to insert data into a "table", while avoid the per-table BQ insert limits
2. Expands the schema to fit the JSON documents provided; this includes handling of change-in-datatype, allowing deeply nested JSON arrays, and multidimensional arrays.


## Details




## Installation

Clone from Github


## Configuration

* `account_info` - The BigQuery [Service Account Info](https://cloud.google.com/bigquery/docs/authentication/service-account-file)
* `dataset` - The BigQuery dataset to place tables  
* `table` - The name of the BigQuery table to fill
* `partition` - BigQuery can partition a table based on a `TIMESTAMP` field
  * `field` - The `TIMESTAMP` field to determine the partitions
  * `expire` - Age of partition when it is removed, as determined by the `field` value
* `cluster` - array of field names to sort table
* `id` - 
  * `field` - field used to determine document uniqueness
  * `version` - version number to know which document takes precedence when removing duplicates: largest is chosen.  Unix timestamps works well.
* `top_level_fields` - Map from full path name to top-level field name. BigQuery demands you include the partition and cluster fields if they are not already top-level 
* `sharded` - if `true`, then multiple tables (aka "shards") are allowed, but they must be merged before part of the primary table 
* `read_only` - set to `false` if you are planning to add records
* `schema` - fields that must exist



#### Example config file

```json
{
    "account_info": {
        "$ref": "file:///e:/moz-fx-dev-ekyle-treeherder-a838a7718652.json"
    },
    "dataset": "treeherder",
    "table": "jobs",
    "top_level_fields": {
        "job.id": "_job_id",
        "last_modified": "_last_modified",
        "action.request_time": "_request_time"
    },
    "partition": {
        "field": "action.request_time",
        "expire": "2year"
    },
    "id":{
        "field": "job.id",
        "version": "last_modified"
    },
    "cluster": [
        "job.id",
        "last_modified"
    ],
    "sharded": true
}
```

## Usage



```python
    container = bigquery.Dataset(config)
    index = container.get_or_create_table(config)
    index.extend(documents)
```
