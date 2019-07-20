#[warn(unused_mut)]
use std::time::Duration;

use chrono::prelude::*;
#[warn(unused_imports)]
use clap::{App, Arg};
use csv;
use csv::{ReaderBuilder, Trim};
use ratelimit;
use reqwest;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Record {
    idfa: String,
    ip: String,
}


fn main() {
    let matches = App::new("click upload ")
        .version("1.0")
        .arg(Arg::with_name("file")
            .short("f")
            .long("file")
            .value_name("FILE")
            .help("Path to id file")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("campaign")
            .long("campaign")
            .short("p")
            .help("Sets campaign id")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("cid")
            .long("cid")
            .default_value("test")
            .help("Sets creative id"))
        .arg(Arg::with_name("skip")
            .short("s")
            .long("skip")
            .default_value("0")
            .help("Skip line from the id file"))
        .arg(Arg::with_name("qps")
            .long("qps")
            .short("q")
            .default_value("1")
            .help("Request per second"))
        .get_matches();

    let id_file = matches.value_of("file").expect("file must set");
    let cid = matches.value_of("cid").expect("file must set");
    let campaign = matches.value_of("campaign").map(|a| a.parse::<i32>()).unwrap().unwrap();
    let skip = matches.value_of("skip").map(|a| a.parse::<i32>()).unwrap().unwrap();
    let qps = matches.value_of("qps").map(|a| a.parse::<u32>()).unwrap().unwrap();
    println!("{},{},{},{},{}", id_file, campaign, cid, skip, qps);


    let mut ratelimit = ratelimit::Builder::new()
        .capacity(qps) //number of tokens the bucket will hold
        .interval(Duration::new(1, 0)) //add quantum tokens every 1 second
        .build();

    let mut rdr = ReaderBuilder::new()
        .trim(Trim::All)
        .has_headers(false)
        .from_path(id_file)
        .unwrap();

    let mut current_line = 0;
    for result in rdr.deserialize() {
        current_line += 1;
        if current_line < skip {
            continue;
        }
        let record: Record = result.unwrap();

        ratelimit.wait();
        let local: DateTime<Local> = Local::now();
        println!("{} - {}", local, current_line);
        let url = format!("http://clk.cpa.mobcastlead.com/clk?campaign_id={}&cid={}&idfa={}&ip={}", campaign, cid, record.idfa, record.ip);
        let response = reqwest::get(&url[..]);
        match response {
            Ok(mut res) => {
                let data = res.text().unwrap();
                println!("status = {}, {}", res.status(), data);
            }
            _ => {
                println!("bad request");
            }
        }
    }
    println!("END");
}
