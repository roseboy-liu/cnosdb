use std::any::Any;
use std::sync::Arc;

pub use binary::BinaryStatistics;
pub use boolean::BooleanStatistics;
use models::ValueType;
pub use primitive::PrimitiveStatistics;

use crate::error::Result;
use crate::tsm2::page::PageStatistics;
use crate::Error;

mod binary;
mod boolean;
mod primitive;

/// A trait used to describe specific statistics. Each physical type has its own struct.
/// Match the [`Statistics::physical_type`] to each type and downcast accordingly.
pub trait Statistics: Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;

    fn physical_type(&self) -> &ValueType;

    fn null_count(&self) -> Option<i64>;
}

impl PartialEq for &dyn Statistics {
    fn eq(&self, other: &Self) -> bool {
        self.physical_type() == other.physical_type() && {
            match self.physical_type() {
                ValueType::Boolean => {
                    self.as_any().downcast_ref::<BooleanStatistics>().unwrap()
                        == other.as_any().downcast_ref::<BooleanStatistics>().unwrap()
                }
                ValueType::Integer => {
                    self.as_any()
                        .downcast_ref::<PrimitiveStatistics<i64>>()
                        .unwrap()
                        == other
                            .as_any()
                            .downcast_ref::<PrimitiveStatistics<i64>>()
                            .unwrap()
                }
                ValueType::Unsigned => {
                    self.as_any()
                        .downcast_ref::<PrimitiveStatistics<u64>>()
                        .unwrap()
                        == other
                            .as_any()
                            .downcast_ref::<PrimitiveStatistics<u64>>()
                            .unwrap()
                }
                ValueType::Float => {
                    self.as_any()
                        .downcast_ref::<PrimitiveStatistics<f64>>()
                        .unwrap()
                        == other
                            .as_any()
                            .downcast_ref::<PrimitiveStatistics<f64>>()
                            .unwrap()
                }
                ValueType::String => {
                    self.as_any().downcast_ref::<BinaryStatistics>().unwrap()
                        == other.as_any().downcast_ref::<BinaryStatistics>().unwrap()
                }
                _ => {
                    panic!("Unexpected data type")
                }
            }
        }
    }
}

/// Deserializes a raw statistics into [`Statistics`].
/// # Error
/// This function errors if it is not possible to read the statistics to the
/// corresponding `physical_type`.
pub fn deserialize_statistics(
    statistics: &PageStatistics,
    primitive_type: ValueType,
) -> Result<Arc<dyn Statistics>> {
    match primitive_type {
        ValueType::Boolean => boolean::read(statistics),
        ValueType::Integer => primitive::read::<i64>(statistics, primitive_type),
        ValueType::Unsigned => primitive::read::<u64>(statistics, primitive_type),
        ValueType::Float => primitive::read::<f64>(statistics, primitive_type),
        ValueType::String => binary::read(statistics),
        _ => {
            return Err(Error::OutOfSpec {
                reason: "unknown data type".to_string(),
            });
        }
    }
}

/// Serializes [`Statistics`] into a raw parquet statistics.
pub fn serialize_statistics(statistics: &dyn Statistics) -> PageStatistics {
    match statistics.physical_type() {
        ValueType::Boolean => boolean::write(statistics.as_any().downcast_ref().unwrap()),
        ValueType::Integer => primitive::write::<i64>(statistics.as_any().downcast_ref().unwrap()),
        ValueType::Unsigned => primitive::write::<u64>(statistics.as_any().downcast_ref().unwrap()),
        ValueType::Float => primitive::write::<f64>(statistics.as_any().downcast_ref().unwrap()),
        ValueType::String => binary::write(statistics.as_any().downcast_ref().unwrap()),
        _ => {
            panic!("Unexpected data type")
        }
    }
}
