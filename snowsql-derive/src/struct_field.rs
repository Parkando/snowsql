use quote::{quote, ToTokens};
use syn::{punctuated::Punctuated, token};

pub struct StructField<'a> {
    pub ident: &'a syn::Ident,
    #[allow(dead_code)]
    pub index: usize,
    pub typ: &'a syn::Type,
    pub is_order_by: bool,
}

impl<'a> StructField<'a> {
    pub fn from_index_and_field(index: usize, field: &'a syn::Field) -> Result<Self, syn::Error> {
        let mut is_order_by = false;

        for attr in &field.attrs {
            if attr.path().is_ident("snowsql") {
                for snowflake_attr in attr.parse_args_with(
                    Punctuated::<super::FieldAttr, token::Comma>::parse_terminated,
                )? {
                    match snowflake_attr {
                        super::FieldAttr::OrderBy => is_order_by = true,
                    }
                }
            }
        }

        Ok(Self {
            ident: field.ident.as_ref().unwrap(),
            index,
            typ: &field.ty,
            is_order_by,
        })
    }

    pub fn seq_access_field_init(&self) -> impl ToTokens {
        let ident = self.ident;
        let typ = self.typ;

        quote! { #ident: seq.next::<#typ>(stringify!(#ident))? }
    }
}
