use syn::{punctuated::Punctuated, token, Data, DeriveInput, Fields, LitStr};

use super::StructField;

pub struct StructData<'a> {
    pub ident: &'a syn::Ident,
    pub fields: Vec<StructField<'a>>,
    pub generics: &'a syn::Generics,
    pub table_name: Option<LitStr>,
    pub order_by: Option<LitStr>,
}

impl<'a> TryFrom<&'a DeriveInput> for StructData<'a> {
    type Error = syn::Error;

    fn try_from(ast: &'a DeriveInput) -> Result<Self, Self::Error> {
        let mut table_name = None::<LitStr>;
        let mut order_by = None::<LitStr>;

        for attr in &ast.attrs {
            if attr.path().is_ident("snowsql") {
                for snowflake_attr in attr.parse_args_with(
                    Punctuated::<super::StructAttr, token::Comma>::parse_terminated,
                )? {
                    match snowflake_attr {
                        super::StructAttr::TableName(name) => table_name = Some(name),
                        super::StructAttr::OrderBy(col) => order_by = Some(col),
                    }
                }
            }
        }

        let fields = match &ast.data {
            Data::Struct(data) => match &data.fields {
                Fields::Named(data) => {
                    let mut fields = Vec::with_capacity(data.named.len());
                    for (i, field) in data.named.iter().enumerate() {
                        fields.push(StructField::from_index_and_field(i, field)?);
                    }

                    fields
                }
                _ => panic!("Named fields only!"),
            },
            Data::Enum(_) => panic!("This macro can only be derived in a struct, not enum."),
            Data::Union(_) => panic!("This macro can only be derived in a struct, not union."),
        };

        Ok(Self {
            ident: &ast.ident,
            fields,
            generics: &ast.generics,
            table_name,
            order_by,
        })
    }
}

impl<'a> StructData<'a> {
    pub fn order_by(&self) -> String {
        let fields_with_order_by = self.fields.iter().filter(|f| f.is_order_by).count();

        if 1 < fields_with_order_by {
            panic!("Selectable: cannot mark multiple columns with `order_by`");
        } else if 0 < fields_with_order_by && self.order_by.is_some() {
            panic!(
		"Selectable: combining #[snowsql(order_by = \"{}\")] and columns marked with #[snowsql(order_by)] is not allowed",
		self.order_by.as_ref().unwrap().value());
        }

        if let Some(order_by) = self.order_by.as_ref() {
            let name = order_by.value();

            if self.fields.iter().all(|f| f.ident != name.as_str()) {
                panic!("Selectable: order_by column `{name}` not found");
            }

            name
        } else if let Some(field) = self.fields.iter().find(|f| f.is_order_by) {
            field.ident.to_string()
        } else {
            panic!("Selectable: add #[snowsql(order_by = \"col\")] to struct or mark a field with #[snowsql(order_by)]")
        }
    }
}
