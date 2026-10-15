//! Source fixture has no instrumentation annotations or manual event calls.
mod child;
#[path="explicit.rs"]
mod explicit;
use std::sync::atomic::AtomicUsize;

pub fn ordinary(value:u32)->Result<u32,String> { child::double(value) }
pub struct Counter { value:u32 }
impl Counter {
    pub fn new()->Self {Self {value:0}}
    pub fn added_without_annotation(&mut self)->u32 {self.value+=1;self.value}
}
pub fn return_error()->Result<(),String> {Err("secret payload /private/path".into())}
pub fn generic_error<T>(_:T)->Result<(),String> {Err("must not read as success".into())}
pub fn panic_probe() {panic!("secret panic payload")}
pub async fn suspended(ready:&AtomicUsize)->Result<u32,String> {
    std::future::poll_fn(|cx| {
        if ready.load(std::sync::atomic::Ordering::SeqCst)>0 {std::task::Poll::Ready(())}else{cx.waker().wake_by_ref();std::task::Poll::Pending}
    }).await;
    ordinary(5)
}
pub const fn static_value()->u32 {7}

pub fn explicit_path_probe()->u32 {explicit::value()}

pub fn conditional_return()->Result<(),Vec<String>> {
    #[cfg(unix)]
    {Err(vec!["private error".into()])}
    #[cfg(windows)]
    {Err(vec!["private error".into()])}
}
pub fn source_location()->(&'static str,u32) {(file!(),line!())}
impl Counter {pub fn borrowed(&mut self)->Result<&mut u32,()> {Ok(&mut self.value)}}

pub trait Value {fn get(&self)->u32;}
pub struct Concrete;
impl Value for Concrete {fn get(&self)->u32 {12}}
pub fn boxed()->Result<Box<dyn Value>,String> {Ok(Box::new(Concrete))}
pub fn opaque()->Result<impl Iterator<Item=u32>,String> {Ok([1,2].into_iter())}

pub fn inferred_callback()->Result<(),Vec<String>> {let call=||Err(vec!["private".into()]);call()}

pub fn typed_callback()->Result<(),Vec<String>> {let call=||->Result<(),Vec<String>>{Err(vec!["private".into()])};call()}

pub struct NoFormat;
pub fn no_format_error()->Result<(),NoFormat> {Err(NoFormat)}

pub fn exit_probe() -> ! { std::process::exit(23) }
