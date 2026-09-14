use crate::errors::ConnectorXPythonError;
use crate::pandas::{
    destination::PandasDestination,
    typesystem::{DateTimeWrapperMicro, PandasTypeSystem},
};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use connectorx::{
    impl_transport,
    sources::mssql::{FloatN, IntN, MsSQLSource, MsSQLTypeSystem},
    typesystem::TypeConversion,
};
use rust_decimal::prelude::*;
use uuid_old::Uuid;

#[allow(dead_code)]
pub struct MsSQLPandasTransport<'py>(&'py ());
#[allow(dead_code)]
pub struct MsSQLBridgePandasTransport<'py>(&'py ());

macro_rules! bind_mssql_pandas {
    ($transport:ident, $source:ty) => {
impl_transport!(
    name = $transport<'tp>,
    error = ConnectorXPythonError,
    systems = MsSQLTypeSystem => PandasTypeSystem,
    route = $source => PandasDestination<'tp>,
    mappings = {
        { Tinyint[u8]                   => I64[i64]                | conversion auto }
        { Smallint[i16]                 => I64[i64]                | conversion auto }
        { Int[i32]                      => I64[i64]                | conversion auto }
        { Bigint[i64]                   => I64[i64]                | conversion auto }
        { Intn[IntN]                    => I64[i64]                | conversion option }
        { Float24[f32]                  => F64[f64]                | conversion auto }
        { Float53[f64]                  => F64[f64]                | conversion auto }
        { Floatn[FloatN]                => F64[f64]                | conversion option }
        { Bit[bool]                     => Bool[bool]              | conversion auto  }
        { Nvarchar[&'r str]             => Str[&'r str]            | conversion auto }
        { Varchar[&'r str]              => Str[&'r str]            | conversion none }
        { Nchar[&'r str]                => Str[&'r str]            | conversion none }
        { Char[&'r str]                 => Str[&'r str]            | conversion none }
        { Text[&'r str]                 => Str[&'r str]            | conversion none }
        { Ntext[&'r str]                => Str[&'r str]            | conversion none }
        { Binary[&'r [u8]]              => ByteSlice[&'r [u8]]     | conversion auto }
        { Varbinary[&'r [u8]]           => ByteSlice[&'r [u8]]     | conversion none }
        { Image[&'r [u8]]               => ByteSlice[&'r [u8]]     | conversion none }
        { Numeric[Decimal]              => F64[f64]                | conversion option }
        { Decimal[Decimal]              => F64[f64]                | conversion none }
        { Datetime[NaiveDateTime]       => DateTimeMicro[DateTimeWrapperMicro] | conversion option }
        { Datetime2[NaiveDateTime]      => DateTimeMicro[DateTimeWrapperMicro] | conversion none }
        { Smalldatetime[NaiveDateTime]  => DateTimeMicro[DateTimeWrapperMicro] | conversion none }
        { Date[NaiveDate]               => DateTimeMicro[DateTimeWrapperMicro] | conversion option }
        { Datetimeoffset[DateTime<Utc>] => DateTimeMicro[DateTimeWrapperMicro] | conversion option }
        { Uniqueidentifier[Uuid]        => String[String]          | conversion option }
        { Time[NaiveTime]               => String[String]          | conversion option }
        { SmallMoney[f32]               => F64[f64]                | conversion none }
        { Money[f64]                    => F64[f64]                | conversion none }
    }
);

impl<'py> TypeConversion<IntN, i64> for $transport<'py> {
    fn convert(val: IntN) -> i64 {
        val.0
    }
}

impl<'py> TypeConversion<FloatN, f64> for $transport<'py> {
    fn convert(val: FloatN) -> f64 {
        val.0
    }
}

impl<'py> TypeConversion<NaiveDateTime, DateTimeWrapperMicro> for $transport<'py> {
    fn convert(val: NaiveDateTime) -> DateTimeWrapperMicro {
        DateTimeWrapperMicro(DateTime::from_naive_utc_and_offset(val, Utc))
    }
}

impl<'py> TypeConversion<NaiveDate, DateTimeWrapperMicro> for $transport<'py> {
    fn convert(val: NaiveDate) -> DateTimeWrapperMicro {
        DateTimeWrapperMicro(DateTime::from_naive_utc_and_offset(
            val.and_hms_opt(0, 0, 0)
                .unwrap_or_else(|| panic!("and_hms_opt got None from {:?}", val)),
            Utc,
        ))
    }
}

impl<'py> TypeConversion<DateTime<Utc>, DateTimeWrapperMicro> for $transport<'py> {
    fn convert(val: DateTime<Utc>) -> DateTimeWrapperMicro {
        DateTimeWrapperMicro(val)
    }
}

impl<'py> TypeConversion<Uuid, String> for $transport<'py> {
    fn convert(val: Uuid) -> String {
        val.to_string()
    }
}

impl<'py> TypeConversion<Decimal, f64> for $transport<'py> {
    fn convert(val: Decimal) -> f64 {
        val.to_f64()
            .unwrap_or_else(|| panic!("cannot convert decimal {:?} to float64", val))
    }
}

impl<'py> TypeConversion<NaiveTime, String> for $transport<'py> {
    fn convert(val: NaiveTime) -> String {
        val.to_string()
    }
}
    };
}

bind_mssql_pandas!(MsSQLPandasTransport, MsSQLSource);
bind_mssql_pandas!(
    MsSQLBridgePandasTransport,
    connectorx::sources::mssql_bridge::MsSQLBridgeSource
);

#[cfg(test)]
mod tests {
    use super::*;
    use connectorx::typesystem::Transport;

    #[test]
    fn both_sources_keep_the_same_pandas_value_conversions() {
        fn legacy<T: Transport<S = MsSQLSource>>() {}
        fn bridge<T: Transport<S = connectorx::sources::mssql_bridge::MsSQLBridgeSource>>() {}
        legacy::<MsSQLPandasTransport<'static>>();
        bridge::<MsSQLBridgePandasTransport<'static>>();
        let id = Uuid::from_bytes([0xab; 16]);
        assert_eq!(
            <MsSQLPandasTransport as TypeConversion<Uuid, String>>::convert(id),
            <MsSQLBridgePandasTransport as TypeConversion<Uuid, String>>::convert(id),
        );
        for value in [
            Decimal::MIN,
            Decimal::ZERO,
            Decimal::MAX,
            Decimal::new(123, 2),
        ] {
            assert_eq!(
                <MsSQLPandasTransport as TypeConversion<Decimal, f64>>::convert(value),
                <MsSQLBridgePandasTransport as TypeConversion<Decimal, f64>>::convert(value),
            );
        }
    }
}
