#![feature(decl_macro)]
#[macro_use]
extern crate rocket;

mod cors;
use test_mango::*;

#[get("/")]
fn index() -> &'static str {
    "Servus das ist die API von team Gelb!"
}

#[get("/get_mvv_data?<typ>&<from>&<to>")]
fn get_mvv_data(typ: String, from: Option<String>, to: Option<String>) -> String {
    let x = typ.split(",").collect();
    let v = get_data(x, from, to);
    match v {
        Some(t) => serde_json::to_string(&t).unwrap(),
        None => format!("No such air property named: \"{}\" in mvv Data!", typ),
    }
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .attach(cors::CORS)
        .mount("/", routes![index, get_mvv_data])
}
