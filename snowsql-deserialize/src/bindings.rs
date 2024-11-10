use std::fmt;

#[derive(Clone, Debug)]
pub enum BindingValue {
    Bool(bool),

    Byte(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    ISize(isize),

    UByte(u8),
    SmallUInt(u16),
    UInt(u32),
    BigUInt(u64),
    USize(usize),

    Float(f32),
    Double(f64),
    Char(char),
    String(String),
    // Decimal(Decimal),
    // DateTime(NaiveDateTime),
    // Date(NaiveDate),
    // Time(NaiveTime),
}

impl BindingValue {
    pub fn kind(&self) -> BindingKind {
        match self {
            BindingValue::Bool(_) => BindingKind::Bool,
            BindingValue::Byte(_)
            | BindingValue::SmallInt(_)
            | BindingValue::Int(_)
            | BindingValue::BigInt(_)
            | BindingValue::ISize(_)
            | BindingValue::UByte(_)
            | BindingValue::SmallUInt(_)
            | BindingValue::UInt(_)
            | BindingValue::BigUInt(_)
            | BindingValue::USize(_) => BindingKind::Fixed,

            BindingValue::Float(_) | BindingValue::Double(_) => BindingKind::Real,
            BindingValue::Char(_) | BindingValue::String(_) => BindingKind::Text,
            // BindingValue::Decimal(_) => BindingKind::Real,
            // BindingValue::DateTime(_) => BindingKind::DateTime,
            // BindingValue::Date(_) => BindingKind::Date,
            // BindingValue::Time(_) => BindingKind::Time,
        }
    }
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BindingKind {
    Bool,
    Fixed,
    Real,
    Text,
    DateTime,
    Date,
    Time,
}

impl std::fmt::Display for BindingValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BindingValue::Bool(v) => <bool as fmt::Display>::fmt(v, f),
            BindingValue::Byte(v) => <i8 as fmt::Display>::fmt(v, f),
            BindingValue::SmallInt(v) => <i16 as fmt::Display>::fmt(v, f),
            BindingValue::Int(v) => <i32 as fmt::Display>::fmt(v, f),
            BindingValue::BigInt(v) => <i64 as fmt::Display>::fmt(v, f),
            BindingValue::ISize(v) => <isize as fmt::Display>::fmt(v, f),
            BindingValue::UByte(v) => <u8 as fmt::Display>::fmt(v, f),
            BindingValue::SmallUInt(v) => <u16 as fmt::Display>::fmt(v, f),
            BindingValue::UInt(v) => <u32 as fmt::Display>::fmt(v, f),
            BindingValue::BigUInt(v) => <u64 as fmt::Display>::fmt(v, f),
            BindingValue::USize(v) => <usize as fmt::Display>::fmt(v, f),
            BindingValue::Float(v) => <f32 as fmt::Display>::fmt(v, f),
            BindingValue::Double(v) => <f64 as fmt::Display>::fmt(v, f),
            BindingValue::Char(v) => <char as fmt::Display>::fmt(v, f),
            BindingValue::String(v) => <String as fmt::Display>::fmt(v, f),
        }
    }
}

impl From<&str> for BindingValue {
    fn from(value: &str) -> Self {
        BindingValue::String(value.to_owned())
    }
}

macro_rules! impl_from_binding_value {
    ($ty: ty, $ex: expr) => {
        impl From<$ty> for BindingValue {
            fn from(value: $ty) -> Self {
                $ex(value)
            }
        }
    };
}
impl_from_binding_value!(bool, BindingValue::Bool);
impl_from_binding_value!(i8, BindingValue::Byte);
impl_from_binding_value!(i16, BindingValue::SmallInt);
impl_from_binding_value!(i32, BindingValue::Int);
impl_from_binding_value!(i64, BindingValue::BigInt);
impl_from_binding_value!(isize, BindingValue::ISize);
impl_from_binding_value!(u8, BindingValue::UByte);
impl_from_binding_value!(u16, BindingValue::SmallUInt);
impl_from_binding_value!(u32, BindingValue::UInt);
impl_from_binding_value!(u64, BindingValue::BigUInt);
impl_from_binding_value!(usize, BindingValue::USize);
impl_from_binding_value!(f32, BindingValue::Float);
impl_from_binding_value!(f64, BindingValue::Double);
impl_from_binding_value!(char, BindingValue::Char);
impl_from_binding_value!(String, BindingValue::String);

//impl_from_binding_value!(Decimal, BindingValue::Decimal);
//impl_from_binding_value!(NaiveDateTime, BindingValue::DateTime);
//impl_from_binding_value!(NaiveDate, BindingValue::Date);
//impl_from_binding_value!(NaiveTime, BindingValue::Time);
