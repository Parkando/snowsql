use syn::{parse::Parse, token, Ident, LitStr};

pub enum StructAttr {
    TableName(LitStr),
    OrderBy(LitStr),
}

impl Parse for StructAttr {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;

        if name == "table_name" {
            input.parse::<token::Eq>()?;
            let table_name = input.parse::<LitStr>()?;
            return Ok(Self::TableName(table_name));
        } else if name == "order_by" {
            input.parse::<token::Eq>()?;
            let table_name = input.parse::<LitStr>()?;
            return Ok(Self::OrderBy(table_name));
        }

        Err(syn::Error::new(input.span(), "invalid attribute `{name}`"))
    }
}
