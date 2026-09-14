use super::{FloatN, IntN, MsSQLBridgeSourceError};
use chrono::{DateTime, Duration, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use mssql_tiberius_bridge::{ColumnValues, DecimalParts, FromSql, Row};
use rust_decimal::{prelude::ToPrimitive, Decimal};

fn invalid(message: &str) -> MsSQLBridgeSourceError {
    MsSQLBridgeSourceError::Conversion(message.into())
}

fn raw(row: &Row, col: usize) -> Result<&ColumnValues, MsSQLBridgeSourceError> {
    row.raw_value(col).ok_or_else(|| {
        mssql_tiberius_bridge::Error::ColumnIndexOutOfBounds {
            index: col,
            count: row.len(),
        }
        .into()
    })
}

fn checked<'r, T: FromSql<'r>>(
    row: &'r Row,
    col: usize,
) -> Result<Option<T>, MsSQLBridgeSourceError> {
    let value = row.try_get::<T, _>(col)?;
    if value.is_none() && !matches!(raw(row, col)?, ColumnValues::Null) {
        return Err(invalid(&format!(
            "column {} cannot be converted to {}",
            col,
            std::any::type_name::<T>()
        )));
    }
    Ok(value)
}

/// `None` means SQL NULL only; type mismatches and missing columns are errors.
pub(crate) trait FromRow<'r>: Sized {
    fn from_row(row: &'r Row, col: usize) -> Result<Option<Self>, MsSQLBridgeSourceError>;
}

macro_rules! from_row {
    ($($ty:ty),+ $(,)?) => {$(
        impl<'r> FromRow<'r> for $ty {
            fn from_row(row: &'r Row, col: usize) -> Result<Option<Self>, MsSQLBridgeSourceError> {
                checked(row, col)
            }
        }
    )+};
}

from_row!(
    u8,
    i16,
    i32,
    i64,
    bool,
    &'r str,
    &'r [u8],
    NaiveDate,
    NaiveTime
);

impl<'r> FromRow<'r> for IntN {
    fn from_row(row: &'r Row, col: usize) -> Result<Option<Self>, MsSQLBridgeSourceError> {
        Ok(checked::<i64>(row, col)?.map(IntN))
    }
}

impl<'r> FromRow<'r> for FloatN {
    fn from_row(row: &'r Row, col: usize) -> Result<Option<Self>, MsSQLBridgeSourceError> {
        Ok(checked::<f64>(row, col)?.map(FloatN))
    }
}

macro_rules! float_from_row {
    ($($ty:ty => $convert:ident),+ $(,)?) => {$(
        impl<'r> FromRow<'r> for $ty {
            fn from_row(row: &'r Row, col: usize) -> Result<Option<Self>, MsSQLBridgeSourceError> {
                match raw(row, col)? {
                    ColumnValues::Money(_) | ColumnValues::SmallMoney(_) => {
                        checked::<Decimal>(row, col)?
                            .and_then(|v| v.$convert())
                            .map(Some)
                            .ok_or_else(|| invalid("money cannot be converted to a float"))
                    }
                    _ => checked(row, col),
                }
            }
        }
    )+};
}

float_from_row!(f32 => to_f32, f64 => to_f64);

impl<'r> FromRow<'r> for uuid_old::Uuid {
    fn from_row(row: &'r Row, col: usize) -> Result<Option<Self>, MsSQLBridgeSourceError> {
        match raw(row, col)? {
            ColumnValues::Null => Ok(None),
            ColumnValues::Uuid(value) => Ok(Some(Self::from_bytes(*value.as_bytes()))),
            _ => Err(invalid("expected UUID")),
        }
    }
}

fn exact_decimal(value: &DecimalParts) -> Result<Decimal, MsSQLBridgeSourceError> {
    let magnitude = value.magnitude();
    // The bridge's string parser can round values with scale above 28.
    if magnitude > ((1u128 << 96) - 1) || value.scale > 28 {
        return Err(invalid(
            "decimal exceeds rust_decimal's 96-bit magnitude or scale 28",
        ));
    }
    Ok(Decimal::from_parts(
        magnitude as u32,
        (magnitude >> 32) as u32,
        (magnitude >> 64) as u32,
        !value.is_positive,
        u32::from(value.scale),
    ))
}

impl<'r> FromRow<'r> for Decimal {
    fn from_row(row: &'r Row, col: usize) -> Result<Option<Self>, MsSQLBridgeSourceError> {
        match raw(row, col)? {
            ColumnValues::Decimal(value) | ColumnValues::Numeric(value) => {
                exact_decimal(value).map(Some)
            }
            _ => checked(row, col),
        }
    }
}

impl<'r> FromRow<'r> for DateTime<Utc> {
    fn from_row(row: &'r Row, col: usize) -> Result<Option<Self>, MsSQLBridgeSourceError> {
        Ok(checked::<DateTime<FixedOffset>>(row, col)?.map(|value| value.with_timezone(&Utc)))
    }
}

fn legacy_datetime(days: i32, ticks: u32) -> Result<NaiveDateTime, MsSQLBridgeSourceError> {
    let date = NaiveDate::from_ymd_opt(1900, 1, 1)
        .and_then(|base| base.checked_add_signed(Duration::days(i64::from(days))))
        .ok_or_else(|| invalid("datetime date out of range"))?;
    // Keep the legacy 1/300-second precision rather than rounding to milliseconds.
    let nanos = u64::from(ticks) * 1_000_000_000 / 300;
    let time = NaiveTime::from_num_seconds_from_midnight_opt(
        (nanos / 1_000_000_000) as u32,
        (nanos % 1_000_000_000) as u32,
    )
    .ok_or_else(|| invalid("datetime time out of range"))?;
    Ok(date.and_time(time))
}

impl<'r> FromRow<'r> for NaiveDateTime {
    fn from_row(row: &'r Row, col: usize) -> Result<Option<Self>, MsSQLBridgeSourceError> {
        match raw(row, col)? {
            ColumnValues::DateTime(value) => legacy_datetime(value.days, value.time).map(Some),
            _ => checked(row, col),
        }
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

    fn row(value: ColumnValues) -> Row {
        // Ordinal access uses the values, not the metadata/name index.
        Row::from_tds(&[], vec![value])
    }

    #[test]
    fn datetime_preserves_tick_precision_and_checks_ranges() {
        assert_eq!(
            legacy_datetime(-1, 1).unwrap().to_string(),
            "1899-12-31 00:00:00.003333333"
        );
        assert_eq!(
            legacy_datetime(0, 25_919_999).unwrap().to_string(),
            "1900-01-01 23:59:59.996666666"
        );
        assert!(legacy_datetime(0, 25_920_000).is_err());
        assert!(legacy_datetime(0, u32::MAX).is_err());
        assert!(legacy_datetime(i32::MAX, 0).is_err());
        assert!(legacy_datetime(i32::MIN, 0).is_err());
    }

    #[test]
    fn nullable_numbers_widen_without_hiding_type_errors() {
        for (value, expected) in [
            (ColumnValues::TinyInt(255), 255),
            (ColumnValues::SmallInt(-42), -42),
            (ColumnValues::Int(i32::MIN), i64::from(i32::MIN)),
            (ColumnValues::BigInt(i64::MIN), i64::MIN),
        ] {
            let row = row(value);
            assert_eq!(
                IntN::from_row(&row, 0).unwrap().map(|v| v.0),
                Some(expected)
            );
        }
        let real = row(ColumnValues::Real(1.25));
        assert_eq!(FloatN::from_row(&real, 0).unwrap().map(|v| v.0), Some(1.25));
        let wrong_type = row(ColumnValues::Bit(true));
        assert!(matches!(
            i64::from_row(&wrong_type, 0),
            Err(MsSQLBridgeSourceError::Conversion(_))
        ));
        assert!(FloatN::from_row(&wrong_type, 0).is_err());
        assert!(u8::from_row(&row(ColumnValues::SmallInt(-1)), 0).is_err());
    }

    #[test]
    fn null_is_not_a_missing_column_or_type_mismatch() {
        let null = row(ColumnValues::Null);
        macro_rules! assert_null {
            ($($ty:ty),+ $(,)?) => {$(
                assert!(<$ty>::from_row(&null, 0).unwrap().is_none());
                assert!(<$ty>::from_row(&null, 1).is_err());
            )+};
        }
        assert_null!(
            u8,
            i16,
            i32,
            i64,
            IntN,
            f32,
            f64,
            FloatN,
            bool,
            &str,
            &[u8],
            uuid_old::Uuid,
            Decimal,
            NaiveDate,
            NaiveTime,
            NaiveDateTime,
            DateTime<Utc>
        );
        let binary = row(ColumnValues::Bytes(vec![0, 255]));
        assert_eq!(<&[u8]>::from_row(&binary, 0).unwrap(), Some(&[0, 255][..]));
        assert!(<&str>::from_row(&binary, 0).is_err());
        assert!(Decimal::from_row(&binary, 0).is_err());
        assert!(NaiveDateTime::from_row(&binary, 0).is_err());
    }

    #[test]
    fn decimal_limits_are_checked_not_rounded() {
        for numeric in [false, true] {
            for (magnitude, scale, expected) in [
                (12345, 4, "-1.2345"),
                (1, 28, "-0.0000000000000000000000000001"),
                ((1u128 << 96) - 1, 0, "-79228162514264337593543950335"),
            ] {
                let parts = DecimalParts::new(false, 38, scale, magnitude);
                let row = row(if numeric {
                    ColumnValues::Numeric(parts)
                } else {
                    ColumnValues::Decimal(parts)
                });
                assert_eq!(
                    Decimal::from_row(&row, 0).unwrap().unwrap().to_string(),
                    expected
                );
            }
            for (magnitude, scale) in [(1u128 << 96, 0), (1, 29)] {
                let parts = DecimalParts::new(true, 38, scale, magnitude);
                let row = row(if numeric {
                    ColumnValues::Numeric(parts)
                } else {
                    ColumnValues::Decimal(parts)
                });
                assert!(matches!(
                    Decimal::from_row(&row, 0),
                    Err(MsSQLBridgeSourceError::Conversion(_))
                ));
            }
        }
        assert_eq!(
            exact_decimal(&DecimalParts::new(true, 29, 0, (1u128 << 96) - 1)).unwrap(),
            Decimal::MAX
        );
    }

    #[test]
    fn uuid_bytes_are_converted_to_legacy_uuid() {
        let expected = "00112233-4455-6677-8899-aabbccddeeff";
        let uuid = row(ColumnValues::Uuid(expected.parse().unwrap()));
        assert_eq!(
            uuid_old::Uuid::from_row(&uuid, 0)
                .unwrap()
                .unwrap()
                .to_string(),
            expected
        );
        assert!(uuid_old::Uuid::from_row(&row(ColumnValues::Bytes(vec![0; 16])), 0).is_err());
    }

    #[test]
    fn money_keeps_its_sign_and_legacy_float_outputs() {
        for scaled in [
            -12345i64,
            -4_294_967_297,
            -4_294_967_296,
            i64::MIN,
            i64::MAX,
        ] {
            let money = row(ColumnValues::Money(
                (scaled as i32, (scaled >> 32) as i32).into(),
            ));
            assert_eq!(
                Decimal::from_row(&money, 0).unwrap(),
                Some(Decimal::new(scaled, 4))
            );
            assert_eq!(
                f64::from_row(&money, 0).unwrap(),
                Decimal::new(scaled, 4).to_f64()
            );
        }
        let smallmoney = row(ColumnValues::SmallMoney((-12345).into()));
        assert_eq!(f64::from_row(&smallmoney, 0).unwrap(), Some(-1.2345));
        assert_eq!(f32::from_row(&smallmoney, 0).unwrap(), Some(-1.2345f32));
        assert!(FloatN::from_row(&smallmoney, 0).is_err());
        assert_eq!(
            f32::from_row(&row(ColumnValues::Real(1.25)), 0).unwrap(),
            Some(1.25)
        );
        assert_eq!(
            f64::from_row(&row(ColumnValues::Float(-1.25)), 0).unwrap(),
            Some(-1.25)
        );
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
