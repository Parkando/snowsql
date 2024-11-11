extern crate proc_macro;
use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{self, parse_macro_input, DeriveInput};

mod field_attr;
mod struct_attr;
mod struct_data;
mod struct_field;

use {field_attr::*, struct_attr::*, struct_data::*, struct_field::*};

#[proc_macro_derive(FromRow)]
pub fn derive_deserialize(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = parse_macro_input!(input);

    let struct_data = match StructData::try_from(&ast) {
        Err(err) => return err.into_compile_error().into(),
        Ok(sd) => sd,
    };

    let from_row = impl_from_row(&struct_data);

    #[rustfmt::skip]
    let expanded = quote! {
	#from_row
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(Selectable, attributes(snowsql))]
pub fn derive_selectable(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = parse_macro_input!(input);
    let struct_data = match StructData::try_from(&ast) {
        Err(err) => return err.into_compile_error().into(),
        Ok(sd) => sd,
    };

    if struct_data.table_name.is_none() {
        panic!("Selectable needs #[snowsql(table_name = \"table_name\")]");
    }

    let selectable = impl_selectable(&struct_data);

    #[rustfmt::skip]
    let expanded = quote! {
	#selectable
    };

    TokenStream::from(expanded)
}

fn impl_from_row(sd: &StructData) -> impl ToTokens {
    let (impl_generics, ty_generics, where_clause) = sd.generics.split_for_impl();

    let name = sd.ident;

    let fields_from_str = sd.fields.iter().map(
        |StructField {
             ident, index, typ, ..
         }| {
            #[rustfmt::skip]
        quote! {
            #ident: <#typ>::deserialize_from_str(row[#index].as_deref())
            .map_err(|err| snowsql::DeserializeError::Field {
                field: stringify!(#ident),
                err: Box::new(err)
            })?
        }
        },
    );

    #[rustfmt::skip]
    quote! {
        impl #impl_generics snowsql::FromRow for #name #ty_generics #where_clause {
            fn from_row(
                row: Vec<Option<String>>
            ) -> snowsql::DeserializeResult<Self> {
		use snowsql::DeserializeFromStr;

		Ok(#name #ty_generics {
		    #(#fields_from_str),*
                })
            }
        }
    }
}

fn impl_selectable(sd: &StructData<'_>) -> impl ToTokens {
    let (impl_generics, ty_generics, where_clause) = sd.generics.split_for_impl();

    let name = sd.ident;

    let Some(table_name) = sd.table_name.as_ref() else {
        panic!("Selectable needs #[snowsql(table_name = \"table_name\")]");
    };

    if sd.fields.iter().any(|f| f.is_order_by) && sd.order_by.is_some() {
        panic!(
            "Selectable cannot have #[snowsql(order_by = \"{}\")] and col `{}` marked as order_by",
            sd.order_by.as_ref().unwrap().value(),
            sd.fields
                .iter()
                .find(|f| f.is_order_by)
                .map(|f| f.ident.to_string())
                .unwrap(),
        );
    }

    let order_by = sd.order_by();
    let field_names = sd.fields.iter().map(|f| f.ident);

    #[rustfmt::skip]
    quote! {
	impl #impl_generics snowsql::Selectable for #name #ty_generics #where_clause {
            const SELECT: &'static str = stringify!(#(#field_names),*);
            const TABLE_NAME: &'static str = #table_name;
            const ORDER_BY: &'static str = #order_by;
	}
    }
}
