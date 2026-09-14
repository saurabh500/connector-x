use super::MsSQLBridgeSourceError;
use crate::sources::mssql::MsSQLTypeSystem;
use mssql_tiberius_bridge::writer::{ColumnMetadata, TdsDataType};
use std::convert::TryFrom;

impl TryFrom<&ColumnMetadata> for MsSQLTypeSystem {
    type Error = MsSQLBridgeSourceError;

    fn try_from(column: &ColumnMetadata) -> Result<Self, Self::Error> {
        Self::try_from(&column.data_type)
    }
}

impl TryFrom<&TdsDataType> for MsSQLTypeSystem {
    type Error = MsSQLBridgeSourceError;

    fn try_from(ty: &TdsDataType) -> Result<Self, Self::Error> {
        use MsSQLTypeSystem::*;
        Ok(match ty {
            TdsDataType::Int1 => Tinyint(false),
            TdsDataType::Int2 => Smallint(false),
            TdsDataType::Int4 => Int(false),
            TdsDataType::Int8 => Bigint(false),
            TdsDataType::IntN => Intn(true),
            TdsDataType::Flt4 => Float24(false),
            TdsDataType::Flt8 => Float53(false),
            TdsDataType::FltN => Floatn(true),
            TdsDataType::Bit => Bit(false),
            TdsDataType::BitN => Bit(true),
            TdsDataType::NVarChar => Nvarchar(true),
            TdsDataType::BigVarChar | TdsDataType::VarChar => Varchar(true),
            TdsDataType::NChar => Nchar(true),
            TdsDataType::BigChar | TdsDataType::Char => Char(true),
            TdsDataType::NText => Ntext(true),
            TdsDataType::Text => Text(true),
            TdsDataType::BigBinary | TdsDataType::Binary => Binary(true),
            TdsDataType::BigVarBinary | TdsDataType::VarBinary => Varbinary(true),
            TdsDataType::Image => Image(true),
            TdsDataType::Guid => Uniqueidentifier(true),
            TdsDataType::DecimalN | TdsDataType::Decimal => Decimal(true),
            TdsDataType::NumericN | TdsDataType::Numeric => Numeric(true),
            TdsDataType::DateTime | TdsDataType::DateTim4 => Datetime(false),
            TdsDataType::DateTime2N => Datetime2(true),
            TdsDataType::DateTimeN => Datetime(true),
            TdsDataType::DateN => Date(true),
            TdsDataType::TimeN => Time(true),
            TdsDataType::DateTimeOffsetN => Datetimeoffset(true),
            TdsDataType::Money | TdsDataType::MoneyN => Money(true),
            TdsDataType::Money4 => SmallMoney(true),
            ty => {
                return Err(MsSQLBridgeSourceError::Conversion(format!(
                    "unsupported wire type {ty:?}"
                )))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_legacy_wire_width_and_nullability() {
        assert!(matches!(
            MsSQLTypeSystem::try_from(&TdsDataType::IntN).unwrap(),
            MsSQLTypeSystem::Intn(true)
        ));
        assert!(matches!(
            MsSQLTypeSystem::try_from(&TdsDataType::FltN).unwrap(),
            MsSQLTypeSystem::Floatn(true)
        ));
        assert!(matches!(
            MsSQLTypeSystem::try_from(&TdsDataType::Int4).unwrap(),
            MsSQLTypeSystem::Int(false)
        ));
        assert!(MsSQLTypeSystem::try_from(&TdsDataType::SsVariant).is_err());
    }
}
