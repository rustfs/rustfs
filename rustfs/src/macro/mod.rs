use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{parse_macro_input, Expr, LitStr};

#[proc_macro]
pub fn timed_println(input: TokenStream) -> TokenStream {
    // 解析输入
    let expr = parse_macro_input!(input as Expr);
    
    // 生成带有时间戳的打印代码
    let expanded = quote! {
        {
            let now = chrono::Utc::now();
            let timestamp = now.format("%Y-%m-%dT%H:%M:%S.%6fZ");
            println!("{}       rustfs: {}", timestamp, #expr);
        }
    };
    
    TokenStream::from(expanded)
}


#[proc_macro]
pub fn timed_println_str(input: TokenStream) -> TokenStream {
    // 尝试将输入解析为字符串字面量
    let input_str = parse_macro_input!(input as LitStr);
    let str_value = input_str.value();
    
    // 生成带有时间戳的打印代码
    let expanded = quote! {
        {
            let now = chrono::Utc::now();
            let timestamp = now.format("%Y-%m-%dT%H:%M:%S.%6fZ");
            println!("{}       rustfs: {}", timestamp, #str_value);
        }
    };
    
    TokenStream::from(expanded)
}