use snowsql::Selectable;

#[test]
fn derive_selectable() {
    #[allow(dead_code)]
    #[derive(Selectable)]
    #[snowsql(table_name = "smt")]
    struct TestStruct {
        vafan: String,
    }

    assert_eq!(TestStruct::SELECT, "vafan");
    assert_eq!(TestStruct::TABLE_NAME, "smt");
}
