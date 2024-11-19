use std::borrow::Cow;

#[test]
fn deserialize_weird_string() {
    let s = r#""Göran Perssons väg \"""#;

    // json strings containing '"' cannot be parsed into &str
    assert!(serde_json::from_str::<&str>(s).is_err());

    serde_json::from_str::<String>(s).expect("parsing into String is OK");
    serde_json::from_str::<Cow<'_, str>>(s).expect("so are Cows");
}
