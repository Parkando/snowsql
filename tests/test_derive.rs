use snowsql::{FromRow, Selectable};

#[test]
fn derive_selectable() {
    #[allow(dead_code)]
    #[derive(FromRow, Selectable)]
    #[snowsql(table_name = "smt")]
    struct TestStruct {
        #[snowsql(order_by)]
        vafan: String,
    }

    assert_eq!(TestStruct::SELECT, "vafan");
    assert_eq!(TestStruct::TABLE_NAME, "smt");
    assert_eq!(TestStructOrderByInStructAttr::ORDER_BY, "vafan");

    #[allow(dead_code)]
    #[derive(FromRow, Selectable)]
    #[snowsql(table_name = "smtelse", order_by = "vafan")]
    struct TestStructOrderByInStructAttr {
        vafan: String,
    }

    assert_eq!(TestStructOrderByInStructAttr::SELECT, "vafan");
    assert_eq!(TestStructOrderByInStructAttr::TABLE_NAME, "smtelse");
    assert_eq!(TestStructOrderByInStructAttr::ORDER_BY, "vafan");
}
