# serverless-otel

A collection of different experiements for ingesting high-cardinality data (e.g. observability) data at a high frequency.

Approaches:
* Interface based on the [Honeycomb storage engine](https://www.honeycomb.io/blog/virtualizing-storage-engine)
* Interface based on above, but using SQLite databases for the segment storage

* Atomic locking using:
  * NFS symlink and unlink (appears to be the most efficient atomic operation to perform against the NFS filesystem)
  * Using s3's new [conditional writes](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/)