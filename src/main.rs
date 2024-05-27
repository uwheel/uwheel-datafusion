use std::fs::File;
use std::time::Duration;

use human_bytes::human_bytes;

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
use uwheel::wheels::read::aggregation::conf::WheelMode;
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
    #[clap(short, long, value_parser, default_value_t = 1000)]
    queries: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    println!("Running with {:#?}", args);
    // create local session context
    let ctx = SessionContext::new();

    let filename = "yellow_tripdata_2022-01.parquet";

    // register parquet file with the execution context
    ctx.register_parquet("yellow_tripdata", &filename, ParquetReadOptions::default())
        .await?;

    let now = Instant::now();
    let wheel = build_fare_wheel(filename);
    println!("Prepared wheel in {:?}", now.elapsed());

    let start_date = NaiveDate::from_ymd_opt(2022, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();

    let end_date = NaiveDate::from_ymd_opt(2022, 1, 31)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();

    println!("===== MINUTE RANGES =====");
    let min_ranges = generate_minute_time_ranges(start_date, end_date, args.queries);

    bench_fare_wheel(wheel.read(), &min_ranges);
    bench_fare_datafusion(&ctx, &min_ranges).await;

    println!("===== HOUR RANGES =====");

    let hr_ranges = generate_hour_time_ranges(start_date, end_date, args.queries);
    bench_fare_wheel(wheel.read(), &hr_ranges);
    bench_fare_datafusion(&ctx, &hr_ranges).await;

    Ok(())
}

fn build_fare_wheel(path: &str) -> RwWheel<F64SumAggregator> {
    let file = File::open(path).unwrap();

    let start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let date = Utc.from_utc_datetime(&start.and_hms_opt(0, 0, 0).unwrap());
    let start_ms = date.timestamp_millis() as u64;

    let mut conf = HawConf::default()
        .with_watermark(start_ms)
        .with_mode(WheelMode::Index);

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
        // dbg!(b.schema());
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
    dbg!(wheel.read().as_ref().minutes_unchecked().len());
    dbg!(wheel.read().as_ref().hours_unchecked().len());
    dbg!(wheel.read().as_ref().days_unchecked().len());
    dbg!(human_bytes(wheel.size_bytes() as u32));

    // If SIMD is enabled then we make sure the wheels are SIMD compatible after building the index
    #[cfg(feature = "simd")]
    wheel.read().to_simd_wheels();

    wheel
}

pub fn generate_minute_time_ranges(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    count: usize,
) -> Vec<(u64, u64)> {
    // Calculate total minutes within the date range
    let total_minutes = (end - start).num_minutes() as u64;

    let mut ranges = Vec::with_capacity(count);
    for _ in 0..count {
        // Randomly select start and end minutes
        let start_minute = fastrand::u64(0..total_minutes - 1); // exclude last min
        let end_minute = fastrand::u64(start_minute + 1..total_minutes);

        // Construct DateTime objects with minute alignment
        let start_time = start + chrono::Duration::minutes(start_minute as i64);
        let end_time = start + chrono::Duration::minutes(end_minute as i64);

        ranges.push((
            start_time.timestamp_millis() as u64,
            end_time.timestamp_millis() as u64,
        ));
    }
    ranges
}

pub fn generate_hour_time_ranges(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    count: usize,
) -> Vec<(u64, u64)> {
    // Calculate total hours within the date range
    let total_hours = (end - start).num_hours() as u64;

    let mut ranges = Vec::with_capacity(count);
    for _ in 0..count {
        // Randomly select start and end hours
        let start_hour = fastrand::u64(0..total_hours - 1); // exclude last hour
        let end_hour = fastrand::u64(start_hour + 1..total_hours);

        // Construct DateTime objects with minute alignment
        let start_time = start + chrono::Duration::minutes(start_hour as i64);
        let end_time = start + chrono::Duration::minutes(end_hour as i64);

        ranges.push((
            start_time.timestamp_millis() as u64,
            end_time.timestamp_millis() as u64,
        ));
    }
    ranges
}

fn bench_fare_wheel(reader: &ReaderWheel<F64SumAggregator>, ranges: &[(u64, u64)]) {
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let full = Instant::now();

    for (start, end) in ranges.into_iter().copied() {
        let now = Instant::now();

        let _res = reader.combine_range_and_lower(WheelRange::new_unchecked(start, end));
        let elapsed = now.elapsed().as_micros() as u64;
        #[cfg(feature = "debug")]
        println!(
            "{:#?}",
            reader
                .as_ref()
                .explain_combine_range(WheelRange::new_unchecked(start, end))
        );
        hist.record(elapsed).unwrap();

        #[cfg(feature = "debug")]
        dbg!(_res);
    }
    let runtime = full.elapsed();
    println!(
        "µWheel Executed {} queries with {:.2}QPS took {:?}",
        ranges.len(),
        (ranges.len() as f64 / runtime.as_secs_f64()),
        runtime
    );

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
        hist.record(now.elapsed().as_micros() as u64).unwrap();
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
        "DataFusion Executed {} queries with {:.2}QPS took {:?}",
        ranges.len(),
        (ranges.len() as f64 / runtime.as_secs_f64()),
        runtime
    );

    print_hist("datafusion fare", &hist);
}

fn print_hist(id: &str, hist: &Histogram<u64>) {
    println!(
        "{} latencies:\t\tmin: {: >4}µs\tp50: {: >4}µs\tp99: {: \
         >4}µs\tp99.9: {: >4}µs\tp99.99: {: >4}µs\tp99.999: {: >4}µs\t max: {: >4}µs \t count: {}",
        id,
        Duration::from_micros(hist.min()).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.5)).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.99)).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.999)).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.9999)).as_micros(),
        Duration::from_micros(hist.value_at_quantile(0.99999)).as_micros(),
        Duration::from_micros(hist.max()).as_micros(),
        hist.len(),
    );
}
