use super::{FloatN, IntN, MsSQLBridgeSourceError};
use chrono::{DateTime, Days, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use mssql_tiberius_bridge::{
    writer::{EncodingType, SqlDateTime2, SqlTime, Uuid},
    ColumnValues,
};
use rust_decimal::Decimal;
use std::{borrow::Cow, convert::TryFrom};

/// Owned cells. Borrowed output values remain valid until the next refill.
#[derive(Debug)]
pub(crate) enum Value {
    Null,
    U8(u8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    SmallMoney(f64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    Uuid(Uuid),
    Decimal(Decimal),
    DateTime(NaiveDateTime),
    Date(NaiveDate),
    Time(NaiveTime),
    Offset(DateTime<Utc>),
}

pub(super) fn invalid(message: &str) -> MsSQLBridgeSourceError {
    MsSQLBridgeSourceError::Conversion(message.into())
}

fn date(days: u32) -> Result<NaiveDate, MsSQLBridgeSourceError> {
    if days > 3_652_058 {
        return Err(invalid("date out of SQL Server range"));
    }
    NaiveDate::from_ymd_opt(1, 1, 1)
        .and_then(|d| d.checked_add_days(Days::new(u64::from(days))))
        .ok_or_else(|| invalid("date out of range"))
}

fn time(nanoseconds: u64) -> Result<NaiveTime, MsSQLBridgeSourceError> {
    let seconds =
        u32::try_from(nanoseconds / 1_000_000_000).map_err(|_| invalid("time out of range"))?;
    NaiveTime::from_num_seconds_from_midnight_opt(seconds, (nanoseconds % 1_000_000_000) as u32)
        .ok_or_else(|| invalid("time out of range"))
}

fn datetime2(value: SqlDateTime2) -> Result<NaiveDateTime, MsSQLBridgeSourceError> {
    Ok(date(value.days)?.and_time(sql_time(value.time)?))
}

fn sql_time(value: SqlTime) -> Result<NaiveTime, MsSQLBridgeSourceError> {
    if value.scale > 7 {
        return Err(invalid("time scale out of range"));
    }
    // The bridge's pinned native 0.1.0 decoder normalizes every scale to
    // 100-nanosecond ticks, despite this field's name.
    let nanos = value
        .time_nanoseconds
        .checked_mul(100)
        .ok_or_else(|| invalid("time out of range"))?;
    time(nanos)
}

impl Value {
    pub(crate) fn set_string(
        &mut self,
        bytes: Cow<'_, [u8]>,
        encoding: EncodingType,
    ) -> Result<(), MsSQLBridgeSourceError> {
        if encoding == EncodingType::Utf8 {
            match bytes {
                Cow::Owned(bytes) => {
                    *self = Self::String(
                        String::from_utf8(bytes).map_err(|_| invalid("invalid UTF-8 string"))?,
                    );
                }
                Cow::Borrowed(bytes) => {
                    let text =
                        std::str::from_utf8(bytes).map_err(|_| invalid("invalid UTF-8 string"))?;
                    let target = self.string_buffer();
                    target.clear();
                    target.push_str(text);
                }
            }
            return Ok(());
        }
        let encoding = encoding
            .encoding()
            .ok_or_else(|| invalid("missing string encoding"))?;
        // A SQL string is not a text file: a leading U+FEFF is data.
        let mut decoder = encoding.new_decoder_without_bom_handling();
        let capacity = decoder
            .max_utf8_buffer_length(bytes.len())
            .ok_or_else(|| invalid("decoded string is too large"))?;
        let target = self.string_buffer();
        target.clear();
        target.reserve(capacity);
        let (_, read, errors) = decoder.decode_to_string(&bytes, target, true);
        if errors || read != bytes.len() {
            return Err(invalid("invalid encoded string"));
        }
        Ok(())
    }

    fn string_buffer(&mut self) -> &mut String {
        if !matches!(self, Self::String(_)) {
            *self = Self::String(String::new());
        }
        match self {
            Self::String(text) => text,
            _ => unreachable!(),
        }
    }

    pub(crate) fn set_bytes(&mut self, bytes: Cow<'_, [u8]>) {
        match (self, bytes) {
            (Self::Bytes(target), Cow::Borrowed(bytes)) => {
                target.clear();
                target.extend_from_slice(bytes);
            }
            (target, bytes) => *target = Self::Bytes(bytes.into_owned()),
        }
    }
}

impl TryFrom<ColumnValues> for Value {
    type Error = MsSQLBridgeSourceError;

    fn try_from(value: ColumnValues) -> Result<Self, Self::Error> {
        Ok(match value {
            ColumnValues::Null => Self::Null,
            ColumnValues::TinyInt(v) => Self::U8(v),
            ColumnValues::SmallInt(v) => Self::I16(v),
            ColumnValues::Int(v) => Self::I32(v),
            ColumnValues::BigInt(v) => Self::I64(v),
            ColumnValues::Real(v) => Self::F32(v),
            ColumnValues::Float(v) => Self::F64(v),
            ColumnValues::Bit(v) => Self::Bool(v),
            ColumnValues::Bytes(v) => Self::Bytes(v),
            ColumnValues::Uuid(v) => Self::Uuid(v),
            ColumnValues::String(v) => {
                let (bytes, encoding) = v.into_parts();
                let mut value = Self::Null;
                value.set_string(Cow::Owned(bytes), encoding)?;
                value
            }
            ColumnValues::Decimal(v) | ColumnValues::Numeric(v) => {
                let magnitude = v.magnitude();
                if magnitude > ((1u128 << 96) - 1) || v.scale > 28 {
                    return Err(invalid(
                        "decimal exceeds rust_decimal's 96-bit magnitude or scale 28",
                    ));
                }
                Self::Decimal(Decimal::from_parts(
                    magnitude as u32,
                    (magnitude >> 32) as u32,
                    (magnitude >> 64) as u32,
                    !v.is_positive,
                    u32::from(v.scale),
                ))
            }
            ColumnValues::SmallMoney(v) => Self::SmallMoney(f64::from(v.int_val) / 10_000.0),
            ColumnValues::Money(v) => {
                let scaled = (i64::from(v.msb_part) << 32) | i64::from(v.lsb_part as u32);
                Self::F64(scaled as f64 / 10_000.0)
            }
            ColumnValues::Date(v) => Self::Date(date(v.get_days())?),
            ColumnValues::Time(v) => Self::Time(sql_time(v)?),
            ColumnValues::DateTime2(v) => Self::DateTime(datetime2(v)?),
            ColumnValues::DateTimeOffset(v) => {
                if !(-840..=840).contains(&v.offset) {
                    return Err(invalid("datetimeoffset offset out of range"));
                }
                // TDS sends the date/time component in UTC, not local time.
                Self::Offset(datetime2(v.datetime2)?.and_utc())
            }
            ColumnValues::DateTime(v) => {
                let day = NaiveDate::from_ymd_opt(1900, 1, 1)
                    .unwrap()
                    .checked_add_signed(Duration::days(i64::from(v.days)))
                    .ok_or_else(|| invalid("datetime out of range"))?;
                // Match legacy Tiberius's 1/300-second nanosecond truncation.
                Self::DateTime(day.and_time(time(u64::from(v.time) * 1_000_000_000 / 300)?))
            }
            ColumnValues::SmallDateTime(v) => {
                let day = NaiveDate::from_ymd_opt(1900, 1, 1)
                    .unwrap()
                    .checked_add_days(Days::new(u64::from(v.days)))
                    .ok_or_else(|| invalid("smalldatetime out of range"))?;
                Self::DateTime(day.and_time(time(u64::from(v.time) * 60_000_000_000)?))
            }
            _ => return Err(invalid("unsupported column value")),
        })
    }
}

/// `None` means SQL NULL only; a type mismatch is an error.
pub(crate) trait FromValue<'a>: Sized {
    fn from_value(value: &'a Value) -> Result<Option<Self>, MsSQLBridgeSourceError>;
}

macro_rules! from_value {
    ($($ty:ty => $variant:ident),+ $(,)?) => {$(
        impl<'a> FromValue<'a> for $ty {
            fn from_value(value: &'a Value) -> Result<Option<Self>, MsSQLBridgeSourceError> {
                match value {
                    Value::Null => Ok(None),
                    Value::$variant(v) => Ok(Some(*v)),
                    _ => Err(invalid(concat!("expected ", stringify!($ty)))),
                }
            }
        }
    )+};
}

from_value!(
    u8 => U8, i16 => I16, i32 => I32, i64 => I64,
    bool => Bool, Uuid => Uuid, Decimal => Decimal,
    NaiveDateTime => DateTime, NaiveDate => Date, NaiveTime => Time, DateTime<Utc> => Offset,
);

impl<'a> FromValue<'a> for uuid_old::Uuid {
    fn from_value(value: &'a Value) -> Result<Option<Self>, MsSQLBridgeSourceError> {
        Ok(Uuid::from_value(value)?.map(|v| Self::from_bytes(*v.as_bytes())))
    }
}

impl<'a> FromValue<'a> for f32 {
    fn from_value(value: &'a Value) -> Result<Option<Self>, MsSQLBridgeSourceError> {
        match value {
            Value::Null => Ok(None),
            Value::F32(v) => Ok(Some(*v)),
            Value::SmallMoney(v) => Ok(Some(*v as f32)),
            _ => Err(invalid("expected f32")),
        }
    }
}

impl<'a> FromValue<'a> for f64 {
    fn from_value(value: &'a Value) -> Result<Option<Self>, MsSQLBridgeSourceError> {
        match value {
            Value::Null => Ok(None),
            Value::F64(v) | Value::SmallMoney(v) => Ok(Some(*v)),
            _ => Err(invalid("expected f64")),
        }
    }
}

impl<'a> FromValue<'a> for &'a str {
    fn from_value(value: &'a Value) -> Result<Option<Self>, MsSQLBridgeSourceError> {
        match value {
            Value::Null => Ok(None),
            Value::String(v) => Ok(Some(v)),
            _ => Err(invalid("expected string")),
        }
    }
}

impl<'a> FromValue<'a> for &'a [u8] {
    fn from_value(value: &'a Value) -> Result<Option<Self>, MsSQLBridgeSourceError> {
        match value {
            Value::Null => Ok(None),
            Value::Bytes(v) => Ok(Some(v)),
            _ => Err(invalid("expected binary")),
        }
    }
}

impl<'a> FromValue<'a> for IntN {
    fn from_value(value: &'a Value) -> Result<Option<Self>, MsSQLBridgeSourceError> {
        Ok(match value {
            Value::Null => None,
            Value::U8(v) => Some(IntN(i64::from(*v))),
            Value::I16(v) => Some(IntN(i64::from(*v))),
            Value::I32(v) => Some(IntN(i64::from(*v))),
            Value::I64(v) => Some(IntN(*v)),
            _ => return Err(invalid("expected nullable integer")),
        })
    }
}

impl<'a> FromValue<'a> for FloatN {
    fn from_value(value: &'a Value) -> Result<Option<Self>, MsSQLBridgeSourceError> {
        Ok(match value {
            Value::Null => None,
            Value::F32(v) => Some(FloatN(f64::from(*v))),
            Value::F64(v) => Some(FloatN(*v)),
            _ => return Err(invalid("expected nullable float")),
        })
    }
}

pub(crate) fn range_value(value: &ColumnValues) -> Result<i64, MsSQLBridgeSourceError> {
    Ok(match value {
        ColumnValues::Null => 0,
        ColumnValues::TinyInt(v) => i64::from(*v),
        ColumnValues::SmallInt(v) => i64::from(*v),
        ColumnValues::Int(v) => i64::from(*v),
        ColumnValues::BigInt(v) => *v,
        ColumnValues::Real(v) => *v as i64,
        ColumnValues::Float(v) => *v as i64,
        _ => return Err(invalid("partitioning requires an integer or float column")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mssql_tiberius_bridge::writer::{
        DecimalParts, SqlDate, SqlDateTime, SqlDateTimeOffset, SqlMoney, SqlSmallDateTime,
        SqlSmallMoney, SqlString,
    };

    #[test]
    fn nullable_numbers_widen_without_hiding_type_errors() {
        assert_eq!(
            IntN::from_value(&Value::U8(255)).unwrap().map(|v| v.0),
            Some(255)
        );
        assert_eq!(
            IntN::from_value(&Value::I16(-42)).unwrap().map(|v| v.0),
            Some(-42)
        );
        assert_eq!(
            IntN::from_value(&Value::I32(i32::MIN))
                .unwrap()
                .map(|v| v.0),
            Some(i64::from(i32::MIN))
        );
        assert_eq!(
            IntN::from_value(&Value::I64(i64::MIN))
                .unwrap()
                .map(|v| v.0),
            Some(i64::MIN)
        );
        assert!(IntN::from_value(&Value::Null).unwrap().is_none());
        assert_eq!(
            FloatN::from_value(&Value::F32(1.25)).unwrap().map(|v| v.0),
            Some(1.25)
        );
        assert_eq!(
            FloatN::from_value(&Value::F64(-1.25)).unwrap().map(|v| v.0),
            Some(-1.25)
        );
        assert!(FloatN::from_value(&Value::Null).unwrap().is_none());
        assert!(IntN::from_value(&Value::String("42".into())).is_err());
        assert!(i32::from_value(&Value::I16(42)).is_err());
        assert!(<&str>::from_value(&Value::Bytes(vec![])).is_err());
        assert!(<&[u8]>::from_value(&Value::String(String::new())).is_err());
    }

    #[test]
    fn decimal_limits_are_checked_not_truncated() {
        for numeric in [false, true] {
            let parts = DecimalParts::new(false, 28, 4, 12345);
            let value = if numeric {
                ColumnValues::Numeric(parts)
            } else {
                ColumnValues::Decimal(parts)
            };
            let value = Value::try_from(value).unwrap();
            assert_eq!(
                Decimal::from_value(&value).unwrap().unwrap().to_string(),
                "-1.2345"
            );
            for (magnitude, scale) in [(1u128 << 96, 0), (1, 29)] {
                let parts = DecimalParts::new(true, 38, scale, magnitude);
                let value = if numeric {
                    ColumnValues::Numeric(parts)
                } else {
                    ColumnValues::Decimal(parts)
                };
                assert!(Value::try_from(value).is_err());
            }
        }
        let max = Value::try_from(ColumnValues::Decimal(DecimalParts::new(
            true,
            29,
            0,
            (1u128 << 96) - 1,
        )))
        .unwrap();
        assert_eq!(Decimal::from_value(&max).unwrap(), Some(Decimal::MAX));
    }

    #[test]
    fn temporal_precision_and_utc_are_preserved() {
        for offset in [-840, -330, 0, 330, 840] {
            let value = Value::try_from(ColumnValues::DateTimeOffset(SqlDateTimeOffset {
                datetime2: SqlDateTime2 {
                    days: 1,
                    time: SqlTime {
                        time_nanoseconds: 123_456_789,
                        scale: 7,
                    },
                },
                offset,
            }))
            .unwrap();
            assert_eq!(
                DateTime::<Utc>::from_value(&value)
                    .unwrap()
                    .unwrap()
                    .to_rfc3339(),
                "0001-01-02T00:00:12.345678900+00:00"
            );
        }
        let value =
            Value::try_from(ColumnValues::DateTime(SqlDateTime { days: -1, time: 1 })).unwrap();
        assert_eq!(
            NaiveDateTime::from_value(&value)
                .unwrap()
                .unwrap()
                .to_string(),
            "1899-12-31 00:00:00.003333333"
        );
        let value = Value::try_from(ColumnValues::SmallDateTime(SqlSmallDateTime {
            days: 1,
            time: 1,
        }))
        .unwrap();
        assert_eq!(
            NaiveDateTime::from_value(&value)
                .unwrap()
                .unwrap()
                .to_string(),
            "1900-01-02 00:01:00"
        );
        let value =
            Value::try_from(ColumnValues::Date(SqlDate::create(3_652_058).unwrap())).unwrap();
        assert_eq!(
            NaiveDate::from_value(&value).unwrap().unwrap().to_string(),
            "9999-12-31"
        );
        for scale in 0..=7 {
            let value = Value::try_from(ColumnValues::Time(SqlTime {
                time_nanoseconds: 10_000_000,
                scale,
            }))
            .unwrap();
            assert_eq!(
                NaiveTime::from_value(&value).unwrap().unwrap().to_string(),
                "00:00:01"
            );
        }
        let value = Value::try_from(ColumnValues::DateTime2(SqlDateTime2 {
            days: 0,
            time: SqlTime {
                time_nanoseconds: 863_999_999_999,
                scale: 7,
            },
        }))
        .unwrap();
        assert_eq!(
            NaiveDateTime::from_value(&value)
                .unwrap()
                .unwrap()
                .to_string(),
            "0001-01-01 23:59:59.999999900"
        );
    }

    #[test]
    fn invalid_temporal_values_are_errors_not_panics_or_nulls() {
        for (ticks, scale) in [(864_000_000_000, 7), (u64::MAX, 7), (0, 8)] {
            assert!(Value::try_from(ColumnValues::Time(SqlTime {
                time_nanoseconds: ticks,
                scale
            }))
            .is_err());
        }
        assert!(Value::try_from(ColumnValues::DateTime2(SqlDateTime2 {
            days: 3_652_059,
            time: SqlTime {
                time_nanoseconds: 0,
                scale: 7
            },
        }))
        .is_err());
        assert!(
            Value::try_from(ColumnValues::DateTimeOffset(SqlDateTimeOffset {
                datetime2: SqlDateTime2 {
                    days: 1,
                    time: SqlTime {
                        time_nanoseconds: 0,
                        scale: 7
                    }
                },
                offset: 841,
            }))
            .is_err()
        );
        assert!(Value::try_from(ColumnValues::DateTime(SqlDateTime {
            days: i32::MAX,
            time: 0
        }))
        .is_err());
        assert!(Value::try_from(ColumnValues::DateTime(SqlDateTime {
            days: 0,
            time: 25_920_000
        }))
        .is_err());
        assert!(
            Value::try_from(ColumnValues::SmallDateTime(SqlSmallDateTime {
                days: 0,
                time: 1440
            }))
            .is_err()
        );
    }

    #[test]
    fn text_binary_uuid_and_negative_money() {
        let value = Value::try_from(ColumnValues::String(SqlString::from_utf8_string(
            "héllo 水 🦀".into(),
        )))
        .unwrap();
        assert_eq!(<&str>::from_value(&value).unwrap(), Some("héllo 水 🦀"));
        let value = Value::try_from(ColumnValues::Bytes(vec![0, 255])).unwrap();
        assert_eq!(<&[u8]>::from_value(&value).unwrap(), Some(&[0, 255][..]));
        let uuid = Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap();
        let value = Value::try_from(ColumnValues::Uuid(uuid)).unwrap();
        assert_eq!(Uuid::from_value(&value).unwrap(), Some(uuid));
        assert_eq!(
            uuid_old::Uuid::from_value(&value)
                .unwrap()
                .unwrap()
                .to_string(),
            uuid.to_string()
        );
        assert_eq!(uuid_old::Uuid::from_value(&Value::Null).unwrap(), None);
        assert!(uuid_old::Uuid::from_value(&Value::Bytes(vec![0; 16])).is_err());
        for scaled in [
            -12345i64,
            -4_294_967_297,
            -4_294_967_296,
            i64::MIN,
            i64::MAX,
        ] {
            let value = Value::try_from(ColumnValues::Money(SqlMoney {
                msb_part: (scaled >> 32) as i32,
                lsb_part: scaled as i32,
            }))
            .unwrap();
            assert_eq!(
                f64::from_value(&value).unwrap(),
                Some(scaled as f64 / 10_000.0)
            );
        }
        let value =
            Value::try_from(ColumnValues::SmallMoney(SqlSmallMoney { int_val: -12345 })).unwrap();
        assert_eq!(f64::from_value(&value).unwrap(), Some(-1.2345));
        assert_eq!(f32::from_value(&value).unwrap(), Some(-1.2345f32));
    }

    #[test]
    fn partition_range_preserves_signed_integers_and_legacy_null_behavior() {
        assert_eq!(
            range_value(&ColumnValues::BigInt(i64::MIN)).unwrap(),
            i64::MIN
        );
        assert_eq!(range_value(&ColumnValues::SmallInt(-42)).unwrap(), -42);
        assert_eq!(range_value(&ColumnValues::Float(-42.75)).unwrap(), -42);
        assert_eq!(range_value(&ColumnValues::Null).unwrap(), 0);
        assert!(range_value(&ColumnValues::Bit(true)).is_err());
    }
}
