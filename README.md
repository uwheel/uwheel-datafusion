# µWheel indexing for DataFusion

This repo contains code for evaluating µWheel as a DataFusion index for temporal aggregation queries:

```sql
SELECT SUM(fare_amount) FROM yellow_tripdata
WHERE tpep_dropoff_datetime >= '?' and < '?'
```

Created for the following blog [post](https://maxmeldrum.com/docs/posts/2024-05-14-uwheel-datafusion.html).

How to run it:

```bash
./fetch_data.sh
cargo run --release --features "mimalloc" -- --queries 20000
```
