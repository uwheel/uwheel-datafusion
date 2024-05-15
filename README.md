# µWheel indexing for DataFusion

This repo contains code for evaluating µWheel as a DataFusion index for temporal aggregation queries:

```sql
SELECT SUM(fare_amount) FROM yellow_tripdata
WHERE tpep_dropoff_datetime >= '?' and < '?'
```

How to run it:

```bash
./fetch_data.sh
cargo run --release --features "mimalloc" -- --queries 20000
```
