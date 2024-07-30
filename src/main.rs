use bytes::Bytes;
use chrono;
use chrono::Timelike;
use serde::{Deserialize, Serialize};
use rumqttc::{Client, MqttOptions, QoS, Event::*, Packet::Publish};
use std::collections::BTreeMap;
use std::time::Duration;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::str::FromStr;
extern crate pretty_env_logger;
#[macro_use] extern crate log;

const RFID: &str = "evcharger1/rfid_auth";
const OVERRIDE: &str = "evcharger1/override";
const ENERGY: &str = "evcharger1/session_energy";
const VEHICLE: &str = "evcharger1/vehicle";

#[derive(Serialize, Deserialize, Debug, Clone)]
struct User {
    name: String,
    rfid: String,
    kwh_remaining: f64,
    total_lifetime_usage: f64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum State {
    Active,
    Disabled,
    Null,
}

#[derive(Serialize, Deserialize, Debug)]
struct Override {
    state: State,
    auto_release: bool
}

#[derive(Debug, Clone)]
struct Session {
    user : User,
    kw_used : f64,
    is_connected : bool,
}

fn rfid_auth<'a>(rfid: &'a str, client: &'a Client, users : &'a BTreeMap<String, User>) -> Option<User> {
    if rfid.is_empty() {
        return None;
    }
    info!("rfid = {rfid:?}");
    let (activate, user) : (State, Option<User>) = match users.get(rfid) {
        Some(user) => { (State::Active, Some(user.clone())) }
        None => { (State::Disabled, None) }
    };
    send_override(activate, client);

    return user;
}

fn handle_rfid(payload : &Bytes, current_session: Option<Session>, client : &Client, users : &BTreeMap<String, User>) -> Option<Session>{
    let new_user = rfid_auth(std::str::from_utf8(&payload).unwrap(), &client, &users);
    match current_session {
        Some(_session) => { Some(_session) },
        None => {
            match new_user {
                Some(new_user) => { Some(Session{ user : new_user, kw_used : 0.0, is_connected : false}) },
                None => { None}
            }
        }
    }
}

fn send_override(state: State, client : &Client) {
    let over_ride = Override{ state : state, auto_release : true};

    client
        .publish(OVERRIDE.to_owned()+"/set", QoS::AtLeastOnce, false, serde_json::to_vec(&over_ride).unwrap())
        .unwrap();

    info!("sent override = {over_ride:?}");
}

fn handle_override(payload : &Bytes) {
    match serde_json::from_slice::<Override>(&payload) {
        Ok(o) => { info!("override recived {o:?}") },
        Err(e) => { error!("{}", e) }
    }
}

fn handle_energy(payload : &Bytes, current_session: Option<Session>, client: &Client) -> Option<Session> {
    match std::str::from_utf8(&payload) {
        Ok(watt_hours) => {
            return add_energy_to_session(&watt_hours, current_session, &client);
        },
        Err(e) => {
            error!("{}", e);
            return current_session;
        }
    }
}

fn add_energy_to_session(watt_hours: &str, current_session: Option<Session>, client: &Client)  -> Option<Session> {
    match f64::from_str(watt_hours) {
        Ok(watt_hours) => {
            let kw_hours = watt_hours/1000.0;
            info!("Current charging session has used {:.2}kWh", kw_hours);
            match current_session {
                Some(mut session) => {
                    session.kw_used = kw_hours;
                    let current_hour = chrono::offset::Local::now().time().hour();
                    info!("current time is {}", current_hour);
                    if session.user.kwh_remaining <= kw_hours  || (current_hour >= 16 && current_hour <= 20) {
                        send_override(State::Disabled, client);
                    }
                    return Some(session);
                },
                None => { None }
            }
        },
        Err(e) => {
            error!("{}", e);
            return current_session;
        }
    }
}

fn handle_vehicle(payload : &Bytes, current_session: Option<Session>, users: &mut BTreeMap<String, User>, client: &Client) -> Option<Session> {
    match std::str::from_utf8(&payload) {
        Ok(is_connected) => {
            let is_connected = is_connected == "1";
            info!("vehcile message = {} current session {:?}", is_connected, current_session);
            match current_session {
                Some(mut session) => {
                    if is_connected {
                        session.is_connected = true;
                        return Some(session);
                    }
                    if !session.is_connected {
                        return Some(session);
                    }

                    send_override(State::Disabled, client);
                    let mut user = session.user;
                    info!("removing {:.2}kWh from user {} balance of {} ", session.kw_used, user.name, user.kwh_remaining);
                    user.kwh_remaining -= session.kw_used;
                    user.total_lifetime_usage += session.kw_used;
                    info!("new user = {:?}", user.clone());
                    users.insert(user.rfid.clone(), user);

                    let user_list : Vec<&mut User> = users.into_iter().map(|(_name, user)| user).collect();

                    match OpenOptions::new().write(true).truncate(true).open("users.json".to_string()) {
                        Ok(f) => {
                            let mut writer = BufWriter::new(f);
                            match serde_json::to_writer_pretty(&mut writer, &user_list) {
                                Ok(_) => {
                                    if let Err(e) = writer.flush() {
                                        error!("flush failed {}", e);
                                    }
                                },
                                Err(e) => { error!("write failed: {}", e); }
                            }
                        },
                        Err(e) => { error!("unable to open file: {}", e) }
                    }
                    return None;
                },
                None => { return None; }
            }
        },
        Err(e) => {
            error!("vehicle packet decode error {}", e);
            return current_session;
        }
    }
}

fn main() -> std::io::Result<()> {
    // Initialize the logger
    pretty_env_logger::init();

    let file = File::open("users.json".to_string())?;
    let reader = BufReader::new(file);
    let users : Vec<User> = serde_json::from_reader(reader)?;
    let mut users : BTreeMap<String, User> = users.into_iter().map(|user| (user.rfid.clone(), user)).collect();

    // Set MQTT connection options and last will message
    let mut mqttoptions = MqttOptions::new("rumqtt", "10.18.15.192", 1883);
    mqttoptions
        .set_credentials("mqtt", "password")
        .set_keep_alive(Duration::from_secs(5));
    // Create MQTT client and connection, and call the publish function in a new thread
    let (client, mut connection) = Client::new(mqttoptions, 10);

    client.subscribe(RFID, QoS::AtMostOnce).unwrap();
    client.subscribe(OVERRIDE, QoS::AtMostOnce).unwrap();
    client.subscribe(ENERGY, QoS::AtMostOnce).unwrap();
    client.subscribe(VEHICLE, QoS::AtMostOnce).unwrap();

    let mut current_session : Option<Session>  = None;

    // Iterate through the notifications in the connection and handle each notification
    for (i, notification) in connection.iter().enumerate() {
        match notification {
            Ok(n) => {
                match n {
                    Incoming(incoming) => {
                        match incoming {
                            Publish(ref publish) => {
                                match publish.topic.as_str() {
                                    RFID  => { current_session = handle_rfid(&publish.payload, current_session, &client, &users); },
                                    ENERGY  => { current_session = handle_energy(&publish.payload, current_session, &client); },
                                    OVERRIDE  => { handle_override(&publish.payload); },
                                    VEHICLE  => { current_session = handle_vehicle(&publish.payload, current_session, &mut users, &client); },
                                    _ => { }
                                }
                            },
                            _ => {} // non publish types
                        }
                    },
                    _ => {} // Outgoing and other packet types
                }
            },
            Err(error) => {
                println!("Error {i}. Notification = {error:?}");
            }
        }
    }

    println!("Done with the stream!!");
    Ok(())
}
