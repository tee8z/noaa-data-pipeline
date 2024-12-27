use std::{cmp, sync::Arc};

use crate::helpers::{spawn_app, MockWeatherAccess};
use axum::{
    body::{to_bytes, Body},
    http::Request,
};
use hyper::{header, Method};
use log::info;
use oracle::{
    oracle::get_winning_bytes, AddEventEntry, CreateEvent, Event, EventStatus, Forecast,
    Observation, WeatherChoices,
};
use serde_json::from_slice;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::time::sleep;
use tower::ServiceExt;
use uuid::{ClockSequence, Timestamp, Uuid};

fn get_uuid_from_timestamp(timestamp_str: &str) -> Uuid {
    struct Context;
    impl ClockSequence for Context {
        type Output = u16;
        fn generate_sequence(&self, _ts_secs: u64, _ts_nanos: u32) -> u16 {
            0
        }
    }

    let dt = OffsetDateTime::parse(
        timestamp_str,
        &time::format_description::well_known::Rfc3339,
    )
    .expect("Valid RFC3339 timestamp");
    let ts = Timestamp::from_unix(Context, dt.unix_timestamp() as u64, dt.nanosecond());
    Uuid::new_v7(ts)
}

#[tokio::test]
async fn can_handle_no_events() {
    let weather_data = MockWeatherAccess::new();
    let test_app = spawn_app(Arc::new(weather_data)).await;
    let request = Request::builder()
        .method(Method::POST)
        .uri(String::from("/oracle/update"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::empty())
        .unwrap();
    let response = test_app
        .app
        .clone()
        .oneshot(request)
        .await
        .expect("Failed to execute request.");
    sleep(std::time::Duration::from_secs(1)).await;
    assert!(response.status().is_success());
}

#[tokio::test]
async fn can_get_event_run_etl_and_see_it_signed() {
    let mut weather_data = MockWeatherAccess::new();
    //called twice per ETL process
    weather_data
        .expect_forecasts_data()
        .times(2)
        .returning(|_, _| Ok(mock_forecast_data()));
    weather_data
        .expect_observation_data()
        .times(2)
        .returning(|_, _| Ok(mock_observation_data()));

    let test_app = spawn_app(Arc::new(weather_data)).await;

    // This makes the event window 1 day (what is used by the oracle)
    let observation_date = OffsetDateTime::parse("2024-08-12T00:00:00+00:00", &Rfc3339).unwrap();
    let signing_date = OffsetDateTime::parse("2024-08-13T00:00:00+00:00", &Rfc3339).unwrap();

    let new_event_1 = CreateEvent {
        id: Uuid::now_v7(),
        observation_date,
        signing_date,
        locations: vec![
            String::from("PFNO"),
            String::from("KSAW"),
            String::from("PAPG"),
            String::from("KWMC"),
        ],
        total_allowed_entries: 4,
        number_of_values_per_entry: 6,
        coordinator: None,
    };

    info!("above create event");
    let event = test_app.oracle.create_event(new_event_1).await.unwrap();

    let entry_1 = AddEventEntry {
        id: get_uuid_from_timestamp("2024-08-11T00:00:00.10Z"),
        event_id: event.id,
        expected_observations: vec![
            WeatherChoices {
                stations: String::from("PFNO"),
                temp_low: Some(oracle::ValueOptions::Under),
                temp_high: None,
                wind_speed: Some(oracle::ValueOptions::Over),
            },
            WeatherChoices {
                stations: String::from("KSAW"),
                temp_low: None,
                temp_high: None,
                wind_speed: Some(oracle::ValueOptions::Over),
            },
            WeatherChoices {
                stations: String::from("KWMC"),
                temp_low: Some(oracle::ValueOptions::Par),
                temp_high: Some(oracle::ValueOptions::Under),
                wind_speed: Some(oracle::ValueOptions::Par),
            },
        ],
        coordinator: None,
    };
    let entry_2 = AddEventEntry {
        id: get_uuid_from_timestamp("2024-08-11T00:00:00.20Z"),
        event_id: event.id,
        expected_observations: vec![
            WeatherChoices {
                stations: String::from("PFNO"),
                temp_low: Some(oracle::ValueOptions::Par),
                temp_high: None,
                wind_speed: Some(oracle::ValueOptions::Par),
            },
            WeatherChoices {
                stations: String::from("KSAW"),
                temp_low: Some(oracle::ValueOptions::Par),
                temp_high: None,
                wind_speed: Some(oracle::ValueOptions::Over),
            },
            WeatherChoices {
                stations: String::from("KWMC"),
                temp_low: Some(oracle::ValueOptions::Par),
                temp_high: Some(oracle::ValueOptions::Under),
                wind_speed: None,
            },
        ],
        coordinator: None,
    };
    let entry_3 = AddEventEntry {
        id: get_uuid_from_timestamp("2024-08-11T00:00:00.30Z"),
        event_id: event.id,
        expected_observations: vec![
            WeatherChoices {
                stations: String::from("PFNO"),
                temp_low: Some(oracle::ValueOptions::Par),
                temp_high: None,
                wind_speed: Some(oracle::ValueOptions::Under),
            },
            WeatherChoices {
                stations: String::from("KSAW"),
                temp_low: Some(oracle::ValueOptions::Over),
                temp_high: None,
                wind_speed: Some(oracle::ValueOptions::Over),
            },
            WeatherChoices {
                stations: String::from("KWMC"),
                temp_low: Some(oracle::ValueOptions::Par),
                temp_high: None,
                wind_speed: Some(oracle::ValueOptions::Under),
            },
        ],
        coordinator: None,
    };
    let entry_4 = AddEventEntry {
        id: get_uuid_from_timestamp("2024-08-11T00:00:00.40Z"),
        event_id: event.id,
        expected_observations: vec![
            WeatherChoices {
                stations: String::from("PFNO"),
                temp_low: Some(oracle::ValueOptions::Over),
                temp_high: None,
                wind_speed: Some(oracle::ValueOptions::Par),
            },
            WeatherChoices {
                stations: String::from("KSAW"),
                temp_low: None,
                temp_high: Some(oracle::ValueOptions::Under),
                wind_speed: Some(oracle::ValueOptions::Over),
            },
            WeatherChoices {
                stations: String::from("KWMC"),
                temp_low: Some(oracle::ValueOptions::Par),
                temp_high: None,
                wind_speed: Some(oracle::ValueOptions::Under),
            },
        ],
        coordinator: None,
    };
    test_app
        .oracle
        .add_event_entry(entry_1.clone())
        .await
        .unwrap();
    test_app
        .oracle
        .add_event_entry(entry_2.clone())
        .await
        .unwrap();
    test_app
        .oracle
        .add_event_entry(entry_3.clone())
        .await
        .unwrap();
    test_app
        .oracle
        .add_event_entry(entry_4.clone())
        .await
        .unwrap();

    // 1) get event before etl
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("/oracle/events/{}", event.id))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::empty())
        .unwrap();

    let response = test_app
        .app
        .clone()
        .oneshot(request)
        .await
        .expect("Failed to execute request.");
    assert!(response.status().is_success());
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let res: Event = from_slice(&body).unwrap();
    assert_eq!(res.status, EventStatus::Completed);
    assert!(res.attestation.is_none());

    // 2) request etl to run
    let request = Request::builder()
        .method(Method::POST)
        .uri(String::from("/oracle/update"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::empty())
        .unwrap();

    let response = test_app
        .app
        .clone()
        .oneshot(request)
        .await
        .expect("Failed to execute request.");
    assert!(response.status().is_success());

    // wait for etl to run in background
    sleep(std::time::Duration::from_secs(1)).await;

    // 3) get event after etl
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("/oracle/events/{}", event.id))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::empty())
        .unwrap();

    let response = test_app
        .app
        .oneshot(request)
        .await
        .expect("Failed to execute request.");
    assert!(response.status().is_success());
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let res: Event = from_slice(&body).unwrap();

    // Verify the event was signed and status changed
    assert_eq!(res.status, EventStatus::Signed);
    assert!(res.attestation.is_some());

    let mut entries_scores_order = res.entries.clone();
    entries_scores_order.sort_by_key(|entry| cmp::Reverse(entry.score));
    info!("entries: {:?}", entries_scores_order);

    // Make sure the expected entries won and calculated the correct score for each
    let entry_1_res = entries_scores_order
        .iter()
        .find(|entry| entry.id == entry_1.id)
        .unwrap();
    assert_eq!(entry_1_res.score.unwrap(), 409899);
    let entry_2_res = entries_scores_order
        .iter()
        .find(|entry| entry.id == entry_2.id)
        .unwrap();
    assert_eq!(entry_2_res.score.unwrap(), 309799);
    let entry_3_res = entries_scores_order
        .iter()
        .find(|entry| entry.id == entry_3.id)
        .unwrap();
    assert_eq!(entry_3_res.score.unwrap(), 409699);
    let entry_4_res = entries_scores_order
        .iter()
        .find(|entry| entry.id == entry_4.id)
        .unwrap();
    assert_eq!(entry_4_res.score.unwrap(), 109599);

    let mut entry_outcome_order = res.entries.clone();
    entry_outcome_order.sort_by_key(|entry| entry.id);

    let first_place_index = entry_outcome_order
        .iter()
        .position(|entry| entry.id == entry_1.id)
        .unwrap();

    let second_place_index = entry_outcome_order
        .iter()
        .position(|entry| entry.id == entry_3.id)
        .unwrap();

    let third_place_index = entry_outcome_order
        .iter()
        .position(|entry| entry.id == entry_2.id)
        .unwrap();

    let winners = vec![first_place_index, second_place_index, third_place_index];

    let winning_bytes = get_winning_bytes(winners);
    println!("winning_bytes in test: {:?}", winning_bytes);

    let outcome_index = event
        .event_announcement
        .outcome_messages
        .iter()
        .position(|outcome| *outcome == winning_bytes)
        .unwrap();

    let attested_outcome = res.event_announcement.attestation_secret(
        outcome_index,
        test_app.oracle.raw_private_key(),
        res.nonce,
    );

    // Verify the attestation matches what we calculate in the test
    assert_eq!(attested_outcome, res.attestation);
}

fn mock_forecast_data() -> Vec<Forecast> {
    vec![
        Forecast {
            station_id: String::from("PFNO"),
            date: String::from("2024-08-12"),
            start_time: String::from("2024-08-11T00:00:00+00:00"),
            end_time: String::from("2024-08-12T00:00:00+00:00"),
            temp_low: 9,
            temp_high: 35,
            wind_speed: 8,
        },
        Forecast {
            station_id: String::from("KSAW"),
            date: String::from("2024-08-12"),
            start_time: String::from("2024-08-11T00:00:00+00:00"),
            end_time: String::from("2024-08-12T00:00:00+00:00"),
            temp_low: 17,
            temp_high: 25,
            wind_speed: 3,
        },
        Forecast {
            station_id: String::from("PAPG"),
            date: String::from("2024-08-12"),
            start_time: String::from("2024-08-11T00:00:00+00:00"),
            end_time: String::from("2024-08-12T00:00:00+00:00"),
            temp_low: 14,
            temp_high: 17,
            wind_speed: 6,
        },
        Forecast {
            station_id: String::from("KWMC"),
            date: String::from("2024-08-12"),
            start_time: String::from("2024-08-11T00:00:00+00:00"),
            end_time: String::from("2024-08-12T00:00:00+00:00"),
            temp_low: 31,
            temp_high: 33,
            wind_speed: 11,
        },
    ]
}

fn mock_observation_data() -> Vec<Observation> {
    vec![
        Observation {
            station_id: String::from("PFNO"),
            start_time: String::from("2024-08-12T00:00:00+00:00"),
            end_time: String::from("2024-08-13T00:00:00+00:00"),
            temp_low: 9.4,
            temp_high: 35 as f64,
            wind_speed: 11,
        },
        Observation {
            station_id: String::from("KSAW"),
            start_time: String::from("2024-08-12T00:00:00+00:00"),
            end_time: String::from("2024-08-13T00:00:00+00:00"),
            temp_low: 22 as f64,
            temp_high: 25 as f64,
            wind_speed: 10,
        },
        Observation {
            station_id: String::from("PAPG"),
            start_time: String::from("2024-08-12T00:00:00+00:00"),
            end_time: String::from("2024-08-13T00:00:00+00:00"),
            temp_low: 15 as f64,
            temp_high: 16 as f64,
            wind_speed: 6,
        },
        Observation {
            station_id: String::from("KWMC"),
            start_time: String::from("2024-08-12T00:00:00+00:00"),
            end_time: String::from("2024-08-13T00:00:00+00:00"),
            temp_low: 32.8,
            temp_high: 34.4,
            wind_speed: 11,
        },
    ]
}
