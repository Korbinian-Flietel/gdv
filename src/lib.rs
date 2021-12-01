#![allow(non_snake_case)]
use std::io::Read;

use std::collections::HashMap;
use chrono::TimeZone;
use mongodb::{bson::doc, sync::Client};
use serde::{Deserialize, Serialize};


#[derive(Debug, Serialize, Deserialize)]
pub struct Device {
    deviceId: String,
    pub timeSeries: Vec<TimeSeries>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimeSeries {
    pub timeSeriesId: String,
    pub timestamps: Vec<String>,
    pub values: Vec<f64>,
    error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    pub timeSeriesId: String,
    pub timeStamp: i64,
    pub value: f64,
}

pub fn transfer_mvv_data() {

    let conn = create_db_conn("mongodb://Nagel:xL8NyJYnnKkuBM4WaVz8NVsGTg@149.172.144.70:27017");

    let base_url = "https://api.mvvsmartcities.com/v2/";
    let api_key = "8e29fc8d4a784c57a61de3f1b399ca78";
    
    let ts_id_no2_a = "3558a2dc-35a7-40af-a8c6-2d31772f58ca".to_string();
    let ts_id_no2_b = "caa4b523-5dec-469c-8817-81cfeb29c5d7".to_string();
    let ts_id_pm10  = "e7fc2f27-726f-4f0d-88fa-92e513e3a7da".to_string();
    let ts_id_so2   = "ebcdbeb2-98d0-456a-94fd-cd131770ab99".to_string();
    let ts_id_o3    = "300e86c7-1e18-4c73-9824-8f2053118692".to_string();
    let ts_id_co    = "77926f27-f648-4da7-8922-0be18b1b9d9f".to_string();

    let mut key_to_val: HashMap<String, String> = HashMap::new();
    key_to_val.insert(ts_id_no2_a.clone(), String::from("no2_a"));
    key_to_val.insert(ts_id_no2_b.clone(), String::from("no2_b"));
    key_to_val.insert(ts_id_pm10.clone(), String::from("pm10"));
    key_to_val.insert(ts_id_so2.clone(), String::from("so2"));
    key_to_val.insert(ts_id_o3.clone(), String::from("o3"));
    key_to_val.insert(ts_id_co.clone(), String::from("co"));

    let db = conn.database("gdv");

    let collection = db.collection::<Payload>("device_data");
    
    let pipeline = vec![doc! {"$sort": {"timeStamp": -1 }}, doc! { "$limit": 1}];

    let mut max = collection.aggregate(pipeline, None).unwrap();

    let mut earliest_date: String;
    let mut today         = today_as_str();


    if let Some(x) = max.next() {
        let d: Payload = mongodb::bson::from_document(x.unwrap()).unwrap();
        let date = chrono::Local.timestamp(d.timeStamp, 100);
        earliest_date = date.to_string().replace(" ", "T")[..23].to_owned();
        println!("{}",earliest_date);
        earliest_date.push_str("Z");
    } else {
        let date = chrono::Utc.timestamp(0, 0);
        earliest_date = date.to_string().replace(" ", "T")[..23].to_owned();
        earliest_date.push_str("Z");
    }
   
    loop {
        println!("Checking for new Data!");
        let json_response = get_api_data(
            base_url,
            api_key,
            "timeseries",
            &format!("?timeSeriesId={}&timeSeriesId={}&timeSeriesId={}&timeSeriesId={}&timeSeriesId={}&timeSeriesId={}&from={}&to={}&func=avg&interval=H&sort=desc",
            ts_id_co, ts_id_no2_a, ts_id_no2_b, ts_id_o3, ts_id_pm10, ts_id_so2, earliest_date, today)
        );

        let ts = json_response[0].timeSeries.clone();

        let payload: Vec<Vec<Payload>> = ts.into_iter().map(|el| {
            let mut v = Vec::new();
            for r in el.timestamps.iter().zip(&el.values) {
                v.push(Payload {
                    timeSeriesId: key_to_val[&el.timeSeriesId].clone(),
                    timeStamp: chrono::NaiveDateTime::parse_from_str(r.0, "%Y-%m-%dT%H:%M:%S%.fZ").unwrap().timestamp(),
                    value: *r.1,
                });
            }
            v
        }).collect();

        let pl: Vec<Payload> = payload.into_iter().flatten().collect();
        let payload_size = pl.len();

        if payload_size > 0 {
            println!("Inserting {} Datapoints!", payload_size);

            collection.insert_many(pl, None).unwrap();
            earliest_date = today;
            std::thread::sleep(std::time::Duration::from_secs(600));
            today = today_as_str();
        } else {
            println!("No Datapoints to Insert! Sleeping for 10 min");
            
            std::thread::sleep(std::time::Duration::from_secs(600));
            today = today_as_str();
        }


        
    }
}

pub fn get_data(t: &String, _fr: Option<String>, _to: Option<String>) -> Option<Vec<Payload>> {
    let conn = create_db_conn("mongodb://Nagel:xL8NyJYnnKkuBM4WaVz8NVsGTg@149.172.144.70:27017");

    let db = conn.database("gdv");

    let collection = db.collection::<Payload>("device_data");

    let pipeline  = vec![doc! {"$match": {"timeSeriesId": t }}, doc! {"$sort": {"timeStamp": -1 }}];

    let pay = collection.aggregate(pipeline, None);

    match pay {
        Ok(v) => {
            let payload: Vec<_> = v.collect();
            let res = payload.into_iter().map(|e| mongodb::bson::from_document(e.unwrap()).unwrap()).collect();
            Some(res)
        },
        Err(_) => { return None; }
    }
}

fn today_as_str() -> String {
    let now = chrono::Local::now();
    let mut now_s = now.to_string().replace(" ", "T")[..23].to_owned();
    now_s.push_str("Z");
    now_s
}

fn create_db_conn(conn_str: &str) -> Client {
    Client::with_uri_str(conn_str).unwrap()
}

fn get_api_data(url: &str, key: &str, service: &str, query: &str) -> Vec<Device> {
    let client = reqwest::blocking::Client::new();
    println!("{}",query);
    let mut res = client
        .get(&format!("{}{}{}",url,service,query))
        .header("Ocp-Apim-Subscription-Key", key)
        .header("User-Agent","HSMA Gruppe Gelb")
        .send()
        .unwrap();
    let mut body = String::new();
    res.read_to_string(&mut body).unwrap();
    body = format!("[{{\"deviceId\": \"Test\", \"timeSeries\": {}}}]", body);
    serde_json::from_str(&body).unwrap()
}