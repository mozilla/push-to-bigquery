# `jx_elasticsearch`

This library implements [JSON Query Expressions]() atop an Elasticsearch instance.


## Contribution

New, or old, versions of Elasticsearch should be added by copying the `es52` subdirectory, and altering the implementation to deal with the differences.

There are two directories in the git history that may help for old versions.

1. `es09` for Elasticsearch version 0.9.x (with MVEL scripting)
2. `es14` is for any version 1.x variant of Elasticsearch (with Groovy scripting)

Both of these directories are too old to be used directly, but they do have code templates for their respective scripting language, and they do have other hints about how to construct queries with the limitations of the older versions.



## elasticsearch.py

This module handles the lifecycle of an Elasticsearch index in the context of
ETL. You only need this module if you are creating and retiring indexes. You
do not need this module for simply searching; for that I suggest using the
rest API directly.

### Settings

Both ```Cluster``` and ```Index``` objects accept the same settings dict,
selecting only the properties it requires.

	{
		"host" : "http://192.168.0.98",
		"port" : 9200,
		"index" : "b2g_tests",
		"type" : "test_result",
		"debug" : true,
		"limit_replicas" : true,
		"schema_file" : "resources/schema/test_schema.json"
	},



