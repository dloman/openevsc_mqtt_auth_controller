use bytes::Bytes;
use chrono;
use chrono::Timelike;
use serde::{Deserialize, Serialize};
use rumqttc::{Client, MqttOptions, QoS, Event, Event::*, Packet::Publish, ConnectionError};
use std::collections::BTreeMap;
use std::time::Duration;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::ops::Mul;
use std::str::FromStr;
extern crate pretty_env_logger;
#[macro_use] extern crate log;
extern crate derive_more;
use derive_more::{Sub, AddAssign, SubAssign};

const RFID: &str = "evcharger1/rfid_auth";
const OVERRIDE: &str = "evcharger1/override";
const ENERGY: &str = "evcharger1/session_energy";
const VEHICLE: &str = "evcharger1/vehicle";

fn handle_packet(notification : Result<Event, ConnectionError>) -> (String, Bytes) {
    if let Ok(event) = notification {
        if let Incoming(incoming) = event {
            if let Publish(publish) = incoming {
                return (publish.topic, publish.payload);
            }
        }
    }
    return ("".to_string(), Bytes::new());
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, AddAssign, Sub)]
struct KWh(f64);

#[derive(Serialize, Deserialize, Debug)]
struct DollarsPerkWh(f64);

#[derive(Serialize, Deserialize, Debug, Clone, Default, AddAssign, SubAssign)]
struct Dollars(f64);

impl Mul<DollarsPerkWh> for KWh {
    type Output = Dollars;
    fn mul(self, rhs: DollarsPerkWh) -> Dollars {
        return Dollars(rhs.0 *self.0);
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, AddAssign)]
struct Usage {
    on_peak: KWh,
    off_peak: KWh,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct User {
    name: String,
    rfid: String,
    dollars_remaining: Dollars,
    total_lifetime_usage: Usage,
}


#[derive(Serialize, Deserialize, Debug)]
struct Rate {
    start : u32,
    end : u32,
    price_per_kwh : DollarsPerkWh,
}

#[derive(Serialize, Deserialize, Debug)]
struct Rates {
    on_peak : Rate,
    off_peak : Rate,
}

const RATES: Rates = Rates{
    on_peak: Rate{start:16, end:21, price_per_kwh: DollarsPerkWh(0.85)},
    off_peak: Rate{start:0, end:0, price_per_kwh: DollarsPerkWh(0.35)},
};

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
    kwh_used : KWh,
    usage : Usage,
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
                Some(new_user) => { Some(Session{ user : new_user, usage : Default::default(), is_connected : false, kwh_used : KWh(0.0)}) },
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

fn handle_energy(payload : &Bytes, current_session: Option<Session>) -> Option<Session> {
    match std::str::from_utf8(&payload) {
        Ok(watt_hours) => {
            return add_energy_to_session(&watt_hours, current_session);
        },
        Err(e) => {
            error!("{}", e);
            return current_session;
        }
    }
}

fn get_usage(update : KWh, mut usage : Usage, current_hour : u32) -> Usage {
    if current_hour >= RATES.on_peak.start && current_hour < RATES.on_peak.end {
        usage.on_peak += update;
    } else {
        usage.off_peak += update;
    }
    return usage;
}

fn add_energy_to_session(watt_hours: &str, current_session: Option<Session>)  -> Option<Session> {
    match f64::from_str(watt_hours) {
        Ok(watt_hours) => {
            let kw_hours = KWh(watt_hours/1000.0);
            info!("Current charging session has used {:?}", kw_hours);
            match current_session {
                Some(mut session) => {
                    let current_hour = chrono::offset::Local::now().time().hour();
                    info!("current time is {}", current_hour);

                    session.usage = get_usage(kw_hours.clone() - session.kwh_used, session.usage, current_hour);
                    session.kwh_used = kw_hours;
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

fn update_user(mut user: User, usage : Usage) -> User {
    let mut dollars_used = usage.on_peak.clone() * RATES.on_peak.price_per_kwh;
    dollars_used += usage.off_peak.clone() * RATES.off_peak.price_per_kwh;
    info!("removing current session {:?} from user {} balance of {:?} ", dollars_used, user.name, user.dollars_remaining);
    user.dollars_remaining -= dollars_used;
    user.total_lifetime_usage += usage;
    info!("new user = {:?}", user.clone());
    return user;
}

fn write_user_json(users:&mut BTreeMap<String, User>) {
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
}

fn handle_vehicle(payload : &Bytes, current_session: Option<Session>, users: &mut BTreeMap<String, User>, client: &Client) -> Option<Session> {
    match std::str::from_utf8(&payload) {
        Ok(is_connected) => {
            let is_connected = is_connected == "1";
            info!("vehicle message = {} current session {:?}", is_connected, current_session);
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
                    let user = update_user(session.user, session.usage);
                    users.insert(user.rfid.clone(), user);
                    write_user_json(users);
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

    let mut current_session : Option<Session> = None;

    // Iterate through the notifications in the connection and handle each notification
    for notification in connection.iter() {

        let (topic , payload) = handle_packet(notification);

        match topic.as_str() {
            RFID  => { current_session = handle_rfid(&payload, current_session, &client, &users); },
            ENERGY  => { current_session = handle_energy(&payload, current_session); },
            OVERRIDE  => { handle_override(&payload); },
            VEHICLE  => { current_session = handle_vehicle(&payload, current_session, &mut users, &client); },
            _ => { }
        }
    }

    println!("Done with the stream!!");
    Ok(())
}
