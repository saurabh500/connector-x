use super::{
    conversion::{invalid, Value},
    MsSQLBridgeSourceError,
};
use mssql_tiberius_bridge::{
    writer::{
        DecimalParts, EncodingType, RowWriter, SqlDate, SqlDateTime, SqlDateTime2,
        SqlDateTimeOffset, SqlJson, SqlMoney, SqlSmallDateTime, SqlSmallMoney, SqlTime, SqlVector,
        SqlXml, Uuid,
    },
    ColumnValues,
};
use std::{borrow::Cow, convert::TryFrom};

/// One checked row decode into reusable owned cells.
///
/// Only publish the row after `finish` returns `Ok(true)`. A failed or cancelled
/// read can leave stale or partial cells; a fresh writer resets callback state,
/// not the bridge's connection state.
pub(crate) struct ValueWriter<'a> {
    row: &'a mut Vec<Value>,
    next_col: usize,
    ended: bool,
    error: Option<MsSQLBridgeSourceError>,
}

impl<'a> ValueWriter<'a> {
    pub(crate) fn new(row: &'a mut Vec<Value>, ncols: usize) -> Self {
        row.resize_with(ncols, || Value::Null);
        Self {
            row,
            next_col: 0,
            ended: false,
            error: None,
        }
    }

    fn slot(&mut self, col: usize) -> Option<&mut Value> {
        if self.error.is_some() {
            return None;
        }
        if col != self.next_col || col >= self.row.len() || self.ended {
            self.error = Some(invalid("result column count or callback order changed"));
            return None;
        }
        self.next_col += 1;
        Some(&mut self.row[col])
    }

    fn put(&mut self, col: usize, value: Value) {
        if let Some(slot) = self.slot(col) {
            *slot = value;
        }
    }

    fn checked(&mut self, col: usize, value: ColumnValues) {
        if let Some(slot) = self.slot(col) {
            match Value::try_from(value) {
                Ok(value) => *slot = value,
                Err(error) => self.error = Some(error),
            }
        }
    }

    fn unsupported(&mut self, col: usize) {
        if self.slot(col).is_some() {
            self.error = Some(invalid("unsupported column value"));
        }
    }

    pub(crate) fn finish(
        self,
        result: mssql_tiberius_bridge::Result<bool>,
    ) -> Result<bool, MsSQLBridgeSourceError> {
        // Callback errors must win even over a later transport failure.
        if let Some(error) = self.error {
            return Err(error);
        }
        let has_row = result?;
        if (has_row && (!self.ended || self.next_col != self.row.len()))
            || (!has_row && (self.ended || self.next_col != 0))
        {
            return Err(invalid("incomplete row callbacks"));
        }
        Ok(has_row)
    }
}

macro_rules! direct_writes {
    ($($method:ident($ty:ty) => $variant:ident),+ $(,)?) => {$(
        fn $method(&mut self, col: usize, value: $ty) {
            self.put(col, Value::$variant(value));
        }
    )+};
}

macro_rules! checked_writes {
    ($($method:ident($ty:ty) => $variant:ident),+ $(,)?) => {$(
        fn $method(&mut self, col: usize, value: $ty) {
            self.checked(col, ColumnValues::$variant(value));
        }
    )+};
}

impl RowWriter for ValueWriter<'_> {
    fn write_null(&mut self, col: usize) {
        self.put(col, Value::Null);
    }

    direct_writes!(
        write_bool(bool) => Bool, write_u8(u8) => U8,
        write_i16(i16) => I16, write_i32(i32) => I32, write_i64(i64) => I64,
        write_f32(f32) => F32, write_f64(f64) => F64, write_uuid(Uuid) => Uuid,
    );

    checked_writes!(
        write_decimal(DecimalParts) => Decimal, write_numeric(DecimalParts) => Numeric,
        write_date(SqlDate) => Date, write_time(SqlTime) => Time,
        write_datetime(SqlDateTime) => DateTime,
        write_smalldatetime(SqlSmallDateTime) => SmallDateTime,
        write_datetime2(SqlDateTime2) => DateTime2,
        write_datetimeoffset(SqlDateTimeOffset) => DateTimeOffset,
        write_money(SqlMoney) => Money, write_smallmoney(SqlSmallMoney) => SmallMoney,
    );

    fn write_string(&mut self, col: usize, bytes: Cow<'_, [u8]>, encoding: EncodingType) {
        if let Some(slot) = self.slot(col) {
            if let Err(error) = slot.set_string(bytes, encoding) {
                self.error = Some(error);
            }
        }
    }

    fn write_bytes(&mut self, col: usize, bytes: Cow<'_, [u8]>) {
        if let Some(slot) = self.slot(col) {
            slot.set_bytes(bytes);
        }
    }

    fn write_xml(&mut self, col: usize, _: SqlXml) {
        self.unsupported(col);
    }
    fn write_json(&mut self, col: usize, _: SqlJson) {
        self.unsupported(col);
    }
    fn write_vector(&mut self, col: usize, _: SqlVector) {
        self.unsupported(col);
    }

    fn end_row(&mut self) {
        if self.error.is_none() && (self.ended || self.next_col != self.row.len()) {
            self.error = Some(invalid("incomplete row callbacks"));
        }
        self.ended = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::mssql_bridge::conversion::FromValue;

    #[test]
    fn borrowed_bytes_are_owned_and_reuse_cell_capacity() {
        let mut row = vec![];
        let mut bytes = b"hello".to_vec();
        let mut writer = ValueWriter::new(&mut row, 2);
        writer.write_string(0, Cow::Borrowed(&bytes), EncodingType::Utf8);
        writer.write_bytes(1, Cow::Borrowed(&bytes));
        writer.end_row();
        assert!(writer.finish(Ok(true)).unwrap());
        let text_ptr = <&str>::from_value(&row[0]).unwrap().unwrap().as_ptr();
        let bytes_ptr = <&[u8]>::from_value(&row[1]).unwrap().unwrap().as_ptr();
        let row_ptr = row.as_ptr();
        for _ in 0..2 {
            let mut writer = ValueWriter::new(&mut row, 2);
            writer.write_string(0, Cow::Borrowed(&bytes), EncodingType::Utf8);
            writer.write_bytes(1, Cow::Borrowed(&bytes));
            writer.end_row();
            assert!(writer.finish(Ok(true)).unwrap());
            assert_eq!(row.as_ptr(), row_ptr);
            assert_eq!(
                <&str>::from_value(&row[0]).unwrap().unwrap().as_ptr(),
                text_ptr
            );
            assert_eq!(
                <&[u8]>::from_value(&row[1]).unwrap().unwrap().as_ptr(),
                bytes_ptr
            );
        }
        bytes.fill(b'x');
        assert_eq!(<&str>::from_value(&row[0]).unwrap(), Some("hello"));
        assert_eq!(<&[u8]>::from_value(&row[1]).unwrap(), Some(&b"hello"[..]));
    }

    #[test]
    fn callback_errors_are_not_null_and_do_not_leak_into_next_row() {
        let mut row = vec![];
        for bytes in [Cow::Borrowed(&[0xff][..]), Cow::Owned(vec![0xff])] {
            let mut writer = ValueWriter::new(&mut row, 2);
            writer.write_string(0, bytes, EncodingType::Utf8);
            writer.write_null(1);
            writer.end_row();
            let error = writer.finish(Ok(true)).unwrap_err();
            assert!(error.to_string().contains("invalid UTF-8 string"));
        }
        let mut writer = ValueWriter::new(&mut row, 2);
        writer.write_string(0, Cow::Borrowed(b"healthy"), EncodingType::Utf8);
        writer.write_null(1);
        writer.end_row();
        assert!(writer.finish(Ok(true)).unwrap());
        assert_eq!(<&str>::from_value(&row[0]).unwrap(), Some("healthy"));
        assert_eq!(<&str>::from_value(&row[1]).unwrap(), None);
    }

    #[test]
    fn first_callback_error_wins_over_later_callbacks_and_transport_errors() {
        let mut row = vec![Value::I32(123)];
        let mut writer = ValueWriter::new(&mut row, 1);
        writer.write_decimal(0, DecimalParts::new(true, 38, 0, 1u128 << 96));
        writer.write_i32(0, 42);
        writer.end_row();
        writer.end_row();
        let transport = mssql_tiberius_bridge::Error::Tds(
            mssql_tiberius_bridge::writer::TdsError::ProtocolError("later transport error".into()),
        );
        let error = writer.finish(Err(transport)).unwrap_err();
        assert!(error.to_string().contains("96-bit"));
        assert_eq!(i32::from_value(&row[0]).unwrap(), Some(123));
    }

    #[test]
    fn incomplete_duplicate_and_out_of_order_callbacks_fail() {
        let mut row = vec![];
        let writer = ValueWriter::new(&mut row, 1);
        assert!(writer.finish(Ok(true)).is_err());
        let mut writer = ValueWriter::new(&mut row, 1);
        writer.write_i32(0, 1);
        assert!(writer.finish(Ok(true)).is_err());
        let mut writer = ValueWriter::new(&mut row, 1);
        writer.end_row();
        assert!(writer.finish(Ok(true)).is_err());
        for col in [1, usize::MAX] {
            let mut writer = ValueWriter::new(&mut row, 1);
            writer.write_i32(col, 1);
            writer.end_row();
            assert!(writer.finish(Ok(true)).is_err());
        }
        let mut writer = ValueWriter::new(&mut row, 2);
        writer.write_i32(0, 1);
        writer.write_i32(0, 2);
        writer.write_i32(1, 3);
        writer.end_row();
        assert!(writer.finish(Ok(true)).is_err());
        let mut writer = ValueWriter::new(&mut row, 1);
        writer.write_null(0);
        writer.end_row();
        writer.end_row();
        assert!(writer.finish(Ok(true)).is_err());
        let mut writer = ValueWriter::new(&mut row, 1);
        writer.write_null(0);
        writer.end_row();
        writer.write_null(0);
        assert!(writer.finish(Ok(true)).is_err());
    }

    #[test]
    fn eof_and_zero_column_rows_require_consistent_callbacks() {
        let mut row = vec![Value::I32(1)];
        for _ in 0..2 {
            assert!(!ValueWriter::new(&mut row, 1).finish(Ok(false)).unwrap());
        }
        let mut writer = ValueWriter::new(&mut row, 1);
        writer.write_null(0);
        assert!(writer.finish(Ok(false)).is_err());
        let mut writer = ValueWriter::new(&mut row, 0);
        writer.end_row();
        assert!(writer.finish(Ok(true)).unwrap());
        assert!(row.is_empty());
        let mut writer = ValueWriter::new(&mut row, 0);
        writer.end_row();
        assert!(writer.finish(Ok(false)).is_err());
    }

    #[test]
    fn transport_errors_never_publish_rows() {
        let mut row = vec![];
        let mut writer = ValueWriter::new(&mut row, 1);
        writer.write_i32(0, 1);
        writer.end_row();
        let transport = mssql_tiberius_bridge::Error::Tds(
            mssql_tiberius_bridge::writer::TdsError::ProtocolError("transport failed".into()),
        );
        assert!(writer
            .finish(Err(transport))
            .unwrap_err()
            .to_string()
            .contains("transport failed"));
    }

    #[test]
    fn unsupported_callbacks_fail_but_nulls_are_valid() {
        let mut row = vec![];
        let mut writer = ValueWriter::new(&mut row, 1);
        writer.write_json(0, SqlJson::new(b"{}".to_vec()));
        writer.end_row();
        assert!(writer
            .finish(Ok(true))
            .unwrap_err()
            .to_string()
            .contains("unsupported"));
        let mut writer = ValueWriter::new(&mut row, 1);
        writer.write_vector(0, SqlVector::try_from_f32(vec![1.0]).unwrap());
        writer.end_row();
        assert!(writer
            .finish(Ok(true))
            .unwrap_err()
            .to_string()
            .contains("unsupported"));
        for _ in 0..2 {
            let mut writer = ValueWriter::new(&mut row, 1);
            writer.write_null(0);
            writer.end_row();
            assert!(writer.finish(Ok(true)).unwrap());
            assert!(<&str>::from_value(&row[0]).unwrap().is_none());
        }
    }

    #[test]
    fn unicode_reuses_storage_and_preserves_leading_bom() {
        let text = "\u{feff}héllo 水 🦀";
        let bytes: Vec<u8> = text.encode_utf16().flat_map(u16::to_le_bytes).collect();
        let mut row = vec![];
        let mut pointer = None;
        for bytes in [Cow::Borrowed(&bytes[..]), Cow::Owned(bytes.clone())] {
            let mut writer = ValueWriter::new(&mut row, 1);
            writer.write_string(0, bytes, EncodingType::Utf16);
            writer.end_row();
            assert!(writer.finish(Ok(true)).unwrap());
            let actual = <&str>::from_value(&row[0]).unwrap().unwrap();
            assert_eq!(actual, text);
            if let Some(pointer) = pointer {
                assert_eq!(actual.as_ptr(), pointer);
            } else {
                pointer = Some(actual.as_ptr());
            }
        }
    }

    #[test]
    fn invalid_encodings_fail_in_both_ownership_modes() {
        let mut row = vec![];
        for encoding in [EncodingType::Utf16, EncodingType::DelayedSet] {
            for bytes in [&[0x00, 0xd8][..], &[0x00][..]] {
                for bytes in [Cow::Borrowed(bytes), Cow::Owned(bytes.to_vec())] {
                    let mut writer = ValueWriter::new(&mut row, 1);
                    writer.write_string(0, bytes, encoding.clone());
                    writer.end_row();
                    assert!(writer.finish(Ok(true)).is_err());
                }
            }
        }
    }
}
