#![feature(decl_macro)]
#[macro_use]
extern crate rocket;

use rocket::State;

mod cors;
use test_mango::*;

#[get("/")]
fn index() -> &'static str {
    "Servus das ist die API von team Gelb!"
}

#[get("/get_cached_data")]
fn get_cached_data(db: &State<Db>, cache: &State<Wraper>) -> String {
    let r1 = cache.cache.read().unwrap();
    println!("{}", r1.data.len());
    if chrono::Local::now().timestamp() - r1.last_update > 86400 {
        drop(r1);
        println!("updating cache!");
        let mut w = cache.cache.write().unwrap();
        w.update(db);
        w.last_update = chrono::Local::now().timestamp();
        return serde_json::to_string(&w.data).unwrap();
    }
    serde_json::to_string(&r1.data).unwrap()
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .manage(Db {
            mongo: test_mango::create_db_conn(
                "mongodb://Nagel:xL8NyJYnnKkuBM4WaVz8NVsGTg@149.172.147.39:27017",
            )
            .database("gdv"),
        })
        .manage(Wraper::new())
        .attach(cors::CORS)
        .mount("/", routes![index, get_cached_data])
}
