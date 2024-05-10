use std::cmp;
use std::fs::File;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use clap::Parser;
use datafusion::arrow::array::{AsArray, Float64Array, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::Float64Type;
use datafusion::error::Result;
use datafusion::prelude::*;
use hdrhistogram::Histogram;
use minstant::Instant;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use uwheel::aggregator::sum::F64SumAggregator;
use uwheel::wheels::read::ReaderWheel;
use uwheel::{Conf, Entry, HawConf, RwWheel};
use uwheel::{NumericalDuration, WheelRange};

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 5)]
    iterations: usize,
    #[clap(short, long, value_parser, default_value_t = 1000)]
    queries: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    println!("Running with {:#?}", args);
    // create local session context
    let ctx = SessionContext::new();

    let filename = "yellow_tripdata_2022-01.parquet";

    // register parquet file with the execution context
    ctx.register_parquet("yellow_tripdata", &filename, ParquetReadOptions::default())
        .await?;

    // // execute the query
    // let df = ctx
    //     .sql("SELECT SUM(fare_amount) FROM yellow_tripdata")
    //     .await?;

    // // print the results
    // df.show().await?;

    // let now = Instant::now();
    // let df = ctx
    //     .sql(
    //         "SELECT SUM(fare_amount) FROM yellow_tripdata \
    //         WHERE tpep_dropoff_datetime >= '2022-01-30 00:00:00' \
    //         AND tpep_dropoff_datetime < '2022-01-31 00:00:00'",
    //     )
    //     .await?;

    // let res = df.collect().await;
    // println!("Ran DF query in {:?}", now.elapsed());
    // assert!(res.is_ok());
    // // print the results
    // // df.show().await?;

    // let df = ctx.sql("SELECT * FROM yellow_tripdata LIMIT 1").await?;

    // print the results
    // df.show().await?;

    // let df = ctx
    //     .sql("SELECT MIN(tpep_dropoff_datetime), MAX(tpep_dropoff_datetime) FROM yellow_tripdata")
    //     .await?;

    // print the results
    // df.show().await?;

    let now = Instant::now();
    let wheel = build_fare_wheel(filename);
    println!("Prepared wheel in {:?}", now.elapsed());

    // Generate time ranges to query between 2022-01-01 and 2022-01-31
    let start_date = NaiveDate::from_ymd_opt(2022, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis() as u64;

    let end_date = NaiveDate::from_ymd_opt(2022, 1, 31)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis() as u64;

    let ranges = generate_time_ranges(args.queries, start_date, end_date);

    // Bench SUM(fare_amount) BETWEEN timestamp and timestamp
    bench_fare_wheel(wheel.read(), &ranges);
    bench_fare_datafusion(&ctx, &ranges).await;

    Ok(())
}

fn build_fare_wheel(path: &str) -> RwWheel<F64SumAggregator> {
    let file = File::open(path).unwrap();

    let start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let date = Utc.from_utc_datetime(&start.and_hms_opt(0, 0, 0).unwrap());
    let start_ms = date.timestamp_millis() as u64;
    dbg!(start_ms);

    let mut conf = HawConf::default().with_watermark(start_ms);

    conf.seconds
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.minutes
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.hours
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.days
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    let mut wheel: RwWheel<F64SumAggregator> = RwWheel::with_conf(
        Conf::default()
            .with_haw_conf(conf)
            .with_write_ahead(64000usize.next_power_of_two()),
    );

    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(8192)
        .build()
        .unwrap();

    for batch in parquet_reader {
        let b = batch.unwrap();
        let dropoff_array = b
            .column_by_name("tpep_dropoff_datetime")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        let fare_array = b
            .column_by_name("fare_amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for (date, fare) in dropoff_array
            .values()
            .iter()
            .zip(fare_array.values().iter())
        {
            let timestamp_ms = DateTime::from_timestamp_micros(*date as i64)
                .unwrap()
                .timestamp_millis() as u64;
            let entry = Entry::new(*fare, timestamp_ms);
            wheel.insert(entry);
        }
    }
    wheel.advance(31.days());
    dbg!(wheel.size_bytes());
    wheel
}

fn generate_time_ranges(total: usize, start_date: u64, end_date: u64) -> Vec<(u64, u64)> {
    (0..total)
        .map(|_| generate_seconds_range(start_date, end_date))
        .collect()
}

pub fn generate_seconds_range(start_date: u64, end_date: u64) -> (u64, u64) {
    // Specify the date range (2023-10-01 to watermark)
    let start_date = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(start_date);
    let end_date = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(end_date);

    // Convert dates to Unix timestamps
    let start_timestamp = start_date
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let end_timestamp = end_date
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Randomly generate a start time within the specified date range
    let random_start = fastrand::u64(start_timestamp..end_timestamp);

    // Generate a random duration between 1 and (watermark - random_start_seconds) seconds
    let max_duration = end_timestamp - random_start;
    let duration_seconds = fastrand::u64(1..=max_duration);

    (random_start, random_start + duration_seconds)
}

fn bench_fare_wheel(reader: &ReaderWheel<F64SumAggregator>, ranges: &[(u64, u64)]) {
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let full = Instant::now();

    for (start, end) in ranges.into_iter().copied() {
        let now = Instant::now();
        let _res = reader.combine_range_and_lower(WheelRange::new_unchecked(start, end));
        hist.record(now.elapsed().as_nanos() as u64).unwrap();
        #[cfg(feature = "debug")]
        dbg!(_res);
    }
    let runtime = full.elapsed();
    println!("µWheel Executed {} queries in {:?}", ranges.len(), runtime);

    print_hist("wheel fare", &hist);
}

async fn bench_fare_datafusion(ctx: &SessionContext, ranges: &[(u64, u64)]) {
    let queries: Vec<_> = ranges
        .iter()
        .copied()
        .map(|(start, end)| {
            let start = DateTime::from_timestamp_millis(start as i64)
                .unwrap()
                .to_utc()
                .naive_utc()
                .to_string();
            let end = DateTime::from_timestamp_millis(end as i64)
                .unwrap()
                .to_utc()
                .naive_utc()
                .to_string();
            format!(
                "SELECT SUM(fare_amount) FROM yellow_tripdata \
            WHERE tpep_dropoff_datetime >= '{}' \
            AND tpep_dropoff_datetime < '{}'",
                start, end
            )
        })
        .collect();
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let full = Instant::now();

    for query in queries {
        // dbg!(&query);
        let now = Instant::now();
        let df = ctx.sql(&query).await.unwrap();
        let res = df.collect().await.unwrap();
        hist.record(now.elapsed().as_nanos() as u64).unwrap();
        let _fare_sum: f64 = res[0]
            .project(&[0])
            .unwrap()
            .column(0)
            .as_primitive::<Float64Type>()
            .value(0);
        #[cfg(feature = "debug")]
        dbg!(_fare_sum);
    }
    let runtime = full.elapsed();
    println!(
        "Datafusion Executed {} queries in {:?}",
        ranges.len(),
        runtime
    );

    print_hist("datafusion fare", &hist);
}

fn print_hist(id: &str, hist: &Histogram<u64>) {
    println!(
        "{} latencies:\t\tmin: {: >4}ns\tp50: {: >4}ns\tp99: {: \
         >4}ns\tp99.9: {: >4}ns\tp99.99: {: >4}ns\tp99.999: {: >4}ns\t max: {: >4}ns \t count: {}",
        id,
        Duration::from_nanos(hist.min()).as_nanos(),
        Duration::from_nanos(hist.value_at_quantile(0.5)).as_nanos(),
        Duration::from_nanos(hist.value_at_quantile(0.99)).as_nanos(),
        Duration::from_nanos(hist.value_at_quantile(0.999)).as_nanos(),
        Duration::from_nanos(hist.value_at_quantile(0.9999)).as_nanos(),
        Duration::from_nanos(hist.value_at_quantile(0.99999)).as_nanos(),
        Duration::from_nanos(hist.max()).as_nanos(),
        hist.len(),
    );
}
