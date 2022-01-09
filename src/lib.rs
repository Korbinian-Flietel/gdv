#![allow(non_snake_case)]
use mongodb::{bson::doc, sync::Client, sync::Database};
use rocket::State;
use serde::{Deserialize, Serialize};
use std::sync::RwLock;

#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    pub city: String,
    pub timeSeriesId: String,
    pub timeStamp: i64,
    pub value: f64,
}

pub struct Db {
    pub mongo: Database,
}

pub struct Wraper {
    pub cache: RwLock<Cache>,
}

impl Wraper {
    pub fn new() -> Self {
        Wraper {
            cache: RwLock::new(Cache::new()),
        }
    }
}

pub struct Cache {
    pub data: Vec<Payload>,
    pub last_update: i64,
}

impl Cache {
    pub fn new() -> Self {
        Cache {
            data: init_cache().unwrap(),
            last_update: chrono::Local::now().timestamp(),
        }
    }

    pub fn update(&mut self, db: &State<Db>) {
        let collection = db.mongo.collection::<Payload>("device_data");
        let pipeline = vec![
            doc! {"$match": {"timeStamp": {
                    "$gte": self.last_update + 1
                }
            }},
            doc! {"$sort": {"timeStamp": mongodb::bson::Bson::Int32(-1) }},
        ];
        let pay = collection.aggregate(pipeline, None);

        match pay {
            Ok(v) => {
                let payload: Vec<_> = v.collect();
                let mut res: Vec<Payload> = payload
                    .into_iter()
                    .map(|e| mongodb::bson::from_document(e.unwrap()).unwrap())
                    .collect();
                self.data.append(&mut res);
            }
            Err(_) => {
                panic!("Whoopsie")
            }
        }
    }
}

pub fn init_cache() -> Option<Vec<Payload>> {
    let client = create_db_conn("mongodb://Nagel:xL8NyJYnnKkuBM4WaVz8NVsGTg@149.172.147.39:27017")
        .database("gdv");
    let collection = client.collection::<Payload>("device_data");

    let date = chrono::Local::now().timestamp() - (31556952 * 3);

    let pipeline = vec![
        doc! {"$match": {"timeStamp": {
                "$gte": date
            }
        }},
        doc! {"$sort": {"timeStamp": mongodb::bson::Bson::Int32(-1) }},
    ];

    let pay = collection.aggregate(pipeline, None);

    match pay {
        Ok(v) => {
            let payload: Vec<_> = v.collect();
            let res = payload
                .into_iter()
                .map(|e| mongodb::bson::from_document(e.unwrap()).unwrap())
                .collect();
            Some(res)
        }
        Err(_) => {
            return None;
        }
    }
}

pub fn get_data(
    t: Vec<&str>,
    _fr: Option<String>,
    _to: Option<String>,
    db: &State<Db>,
) -> Option<Vec<Payload>> {
    let collection = db.mongo.collection::<Payload>("device_data");

    let date = chrono::Local::now().timestamp() - (31556952 * 5);

    let pipeline = vec![
        doc! {"$match": {"timeSeriesId": {
                "$in": t
            },
            "timeStamp": {
                "$gte": date
            }
        }},
        doc! {"$sort": {"timeStamp": -1 }},
    ];

    let pay = collection.aggregate(pipeline, None);

    match pay {
        Ok(v) => {
            let payload: Vec<_> = v.collect();
            let res = payload
                .into_iter()
                .map(|e| mongodb::bson::from_document(e.unwrap()).unwrap())
                .collect();
            Some(res)
        }
        Err(_) => {
            return None;
        }
    }
}

pub fn create_db_conn(conn_str: &str) -> Client {
    Client::with_uri_str(conn_str).unwrap()
}
