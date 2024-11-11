use syn::{parse::Parse, Ident};

pub enum FieldAttr {
    OrderBy,
}

impl Parse for FieldAttr {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;

        if name == "order_by" {
            return Ok(Self::OrderBy);
        }

        Err(syn::Error::new(input.span(), "invalid attribute `{name}`"))
    }
}
