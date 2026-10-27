//! Reading columns of the data host's Parquet files.

use arrow_array::{
    Array, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
    UInt64Array,
};
use bytes::Bytes;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::{Result, bad};

pub(crate) fn read_batches(file: Bytes, what: &str) -> Result<Vec<RecordBatch>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| bad(what, e.to_string()))
}

/// Like [`read_batches`], decoding only `columns`; a column the file lacks is simply absent.
pub(crate) fn read_columns(file: Bytes, what: &str, columns: &[&str]) -> Result<Vec<RecordBatch>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mask = ProjectionMask::columns(builder.parquet_schema(), columns.iter().copied());
    builder
        .with_projection(mask)
        .build()?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| bad(what, e.to_string()))
}

pub(crate) fn col<'a, T: 'static>(b: &'a RecordBatch, name: &str, what: &str) -> Result<&'a T> {
    b.column_by_name(name)
        .ok_or_else(|| bad(what, format!("no column {name}")))?
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| {
            bad(
                what,
                format!(
                    "column {name} has type {:?}",
                    b.column_by_name(name).map(|c| c.data_type().clone())
                ),
            )
        })
}

pub(crate) fn ts(b: &RecordBatch, name: &str, what: &str) -> Result<Vec<i64>> {
    let c = col::<TimestampMillisecondArray>(b, name, what)?;
    Ok(c.values().to_vec())
}

pub(crate) fn u64s(b: &RecordBatch, name: &str, what: &str) -> Result<Vec<Option<u64>>> {
    let c = col::<UInt64Array>(b, name, what)?;
    Ok((0..c.len())
        .map(|i| c.is_valid(i).then(|| c.value(i)))
        .collect())
}

pub(crate) fn i64s(b: &RecordBatch, name: &str, what: &str) -> Result<Vec<Option<i64>>> {
    let c = col::<Int64Array>(b, name, what)?;
    Ok((0..c.len())
        .map(|i| c.is_valid(i).then(|| c.value(i)))
        .collect())
}

pub(crate) fn f64s(b: &RecordBatch, name: &str, what: &str) -> Result<Vec<Option<f64>>> {
    let c = col::<Float64Array>(b, name, what)?;
    Ok((0..c.len())
        .map(|i| c.is_valid(i).then(|| c.value(i)))
        .collect())
}

pub(crate) fn strs(b: &RecordBatch, name: &str, what: &str) -> Result<Vec<Option<String>>> {
    let c = col::<StringArray>(b, name, what)?;
    Ok((0..c.len())
        .map(|i| c.is_valid(i).then(|| c.value(i).to_string()))
        .collect())
}
