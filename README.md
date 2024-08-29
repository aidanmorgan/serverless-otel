# serverless-otel

A collection of different experiements for ingesting high-cardinality data (e.g. observability) data at a high frequency.

General Approach:
* Interface based on the [Honeycomb storage engine](https://www.honeycomb.io/blog/virtualizing-storage-engine)
* Interface based on above, but using SQLite databases for the segment storage
* Atomic locking controlling access to a EFS mount in the lambda using:
  * NFS symlink and unlink (appears to be the most efficient atomic operation to perform against the NFS filesystem)
  * Using s3's new [conditional writes](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/)
  * Just not doing any locking and seeing how "badly" appending to a file corrupts under high load (hard to detect)
    * Some research indicates that an append is atomic when local, but is implementation dependent for NFS as to whether it is or isn't


Other experiments to consider:
  * Sharding by using multiple EFS mounts to reduce iops at high load
  * Replication of EFS mounts between regions as failover/high-availability support
  * Using lambda streaming rather than batching to improve throughput

Creating a bifurcated query manager that:
* uses [duckdb](http://www.duckdb.org) to load the 'hot' otel data for querying
  * Will need to be a separate lambda, accessing the EFS as a 'read only' mount
* a data pipeline that takes the last hour of data and converts it to parquet for storage in a datalake (using duckdb)
  * need to handle "late arriving data" scenarios and either re-generate the parquet, or reject the data?
  * format the parquet appropriately to allow athena to query