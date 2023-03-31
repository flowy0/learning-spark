


## What is a metastore


### Metastore Persistence

- To have a persistent metastore, we can set the following config in spark
`spark.sql.catalogImplementation: hive` 
- if this is not set, the catalog will only persist for the session (in-memory)












## Ref:
- https://urlit.me/blog/pyspark-implementing-persisting-metastore/
