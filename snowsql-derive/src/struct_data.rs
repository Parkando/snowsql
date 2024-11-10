use syn::{punctuated::Punctuated, token, Data, DeriveInput, Fields, LitStr};

pub struct StructData<'a> {
    pub ident: &'a syn::Ident,
    pub fields: Vec<StructField<'a>>,
    pub generics: &'a syn::Generics,
    pub table_name: Option<LitStr>,
}

pub struct StructField<'a> {
    pub ident: &'a syn::Ident,
    pub index: usize,
    pub typ: &'a syn::Type,
}

impl<'a> TryFrom<&'a DeriveInput> for StructData<'a> {
    type Error = syn::Error;

    fn try_from(ast: &'a DeriveInput) -> Result<Self, Self::Error> {
        let mut table_name = None::<LitStr>;

        for attr in &ast.attrs {
            if attr.path().is_ident("snowsql") {
                for snowflake_attr in attr.parse_args_with(
                    Punctuated::<super::StructAttr, token::Comma>::parse_terminated,
                )? {
                    match snowflake_attr {
                        super::StructAttr::TableName(name) => table_name = Some(name),
                    }
                }
            }
        }

        let fields = match &ast.data {
            Data::Struct(data) => match &data.fields {
                Fields::Named(data) => {
                    let mut fields = Vec::with_capacity(data.named.len());

                    for (i, field) in data.named.iter().enumerate() {
                        fields.push(StructField {
                            ident: field.ident.as_ref().unwrap(),
                            index: i,
                            typ: &field.ty,
                        });
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
        })
    }
}
