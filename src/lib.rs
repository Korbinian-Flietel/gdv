#![allow(non_snake_case)]
use mongodb::{bson::doc, sync::Client, sync::Database};
use rocket::State;
use serde::{Deserialize, Serialize};

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
