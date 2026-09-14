use super::MsSQLBridgeSourceError;
use crate::sources::mssql::MsSQLTypeSystem;
use mssql_tiberius_bridge::{Column, ColumnType};
use std::convert::TryFrom;

impl TryFrom<&Column> for MsSQLTypeSystem {
    type Error = MsSQLBridgeSourceError;

    fn try_from(column: &Column) -> Result<Self, Self::Error> {
        use MsSQLTypeSystem::*;
        let nullable = column.nullable();
        Ok(match column.column_type() {
            ColumnType::Int1 if !nullable => Tinyint(false),
            ColumnType::Int2 if !nullable => Smallint(false),
            ColumnType::Int4 if !nullable => Int(false),
            ColumnType::Int8 if !nullable => Bigint(false),
            ColumnType::Int1 | ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8 => Intn(true),
            ColumnType::Float4 if !nullable => Float24(false),
            ColumnType::Float8 if !nullable => Float53(false),
            ColumnType::Float4 | ColumnType::Float8 => Floatn(true),
            ColumnType::Bit => Bit(nullable),
            ColumnType::NVarchar => Nvarchar(true),
            ColumnType::Varchar => Varchar(true),
            ColumnType::NChar => Nchar(true),
            ColumnType::Char => Char(true),
            ColumnType::NText => Ntext(true),
            ColumnType::Text => Text(true),
            ColumnType::Binary => Binary(true),
            ColumnType::VarBinary | ColumnType::BigVarBin => Varbinary(true),
            ColumnType::Image => Image(true),
            ColumnType::Guid => Uniqueidentifier(true),
            ColumnType::Decimaln => Decimal(true),
            ColumnType::Numericn => Numeric(true),
            ColumnType::Datetime | ColumnType::Datetime4 => Datetime(nullable),
            ColumnType::Datetime2 => Datetime2(true),
            ColumnType::Date => Date(true),
            ColumnType::Time => Time(true),
            ColumnType::DatetimeOffset => Datetimeoffset(true),
            ColumnType::Money => Money(true),
            ColumnType::Money4 => SmallMoney(true),
            ty => {
                return Err(MsSQLBridgeSourceError::Conversion(format!(
                    "unsupported column type {ty:?}"
                )))
            }
        })
    }
}
