pub trait Selectable {
    const TABLE_NAME: &str;
    const SELECT: &str;
}
