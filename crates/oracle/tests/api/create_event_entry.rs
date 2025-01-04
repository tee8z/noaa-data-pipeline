use crate::helpers::{spawn_app, MockWeatherAccess};
use axum::{
    body::{to_bytes, Body},
    http::Request,
};
use hyper::{header, Method};
use log::info;
use oracle::{AddEventEntry, CreateEvent, WeatherChoices, WeatherEntry};
use serde_json::{from_slice, to_string};
use std::sync::Arc;
use time::OffsetDateTime;
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn can_create_entry_into_event() {
    let test_app = spawn_app(Arc::new(MockWeatherAccess::new())).await;

    let new_event = CreateEvent {
        id: Uuid::now_v7(),
        observation_date: OffsetDateTime::now_utc(),
        signing_date: OffsetDateTime::now_utc(),
        locations: vec![
            String::from("PFNO"),
            String::from("KSAW"),
            String::from("PAPG"),
            String::from("KWMC"),
        ],
        total_allowed_entries: 5,
        number_of_values_per_entry: 6,
        coordinator: None,
    };
    let oracle_event = test_app.oracle.create_event(new_event).await.unwrap();
    let new_entry = AddEventEntry {
        id: Uuid::now_v7(),
        event_id: oracle_event.id,
        expected_observations: vec![
            WeatherChoices {
                stations: String::from("PFNO"),
                temp_low: Some(oracle::ValueOptions::Par),
                temp_high: None,
                wind_speed: None,
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
    let body_json = to_string(&new_entry).unwrap();
    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("/oracle/events/{}/entry", oracle_event.id))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body_json))
        .unwrap();

    let response = test_app
        .app
        .oneshot(request)
        .await
        .expect("Failed to execute request.");
    info!("response status: {}", response.status());
    assert!(response.status().is_success());
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let res: WeatherEntry = from_slice(&body).unwrap();
    assert_eq!(res.event_id, new_entry.event_id);
    assert_eq!(res.id, new_entry.id);
    assert_eq!(res.expected_observations, new_entry.expected_observations);
}

#[tokio::test]
async fn can_create_and_get_event_entry() {
    let test_app = spawn_app(Arc::new(MockWeatherAccess::new())).await;

    let new_event = CreateEvent {
        id: Uuid::now_v7(),
        observation_date: OffsetDateTime::now_utc(),
        signing_date: OffsetDateTime::now_utc(),
        locations: vec![
            String::from("PFNO"),
            String::from("KSAW"),
            String::from("PAPG"),
            String::from("KWMC"),
        ],
        total_allowed_entries: 10,
        number_of_values_per_entry: 6,
        coordinator: None,
    };
    let oracle_event = test_app.oracle.create_event(new_event).await.unwrap();
    let new_entry = AddEventEntry {
        id: Uuid::now_v7(),
        event_id: oracle_event.id,
        expected_observations: vec![
            WeatherChoices {
                stations: String::from("PFNO"),
                temp_low: Some(oracle::ValueOptions::Par),
                temp_high: None,
                wind_speed: None,
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
    let body_json = to_string(&new_entry).unwrap();
    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("/oracle/events/{}/entry", oracle_event.id))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body_json))
        .unwrap();

    let response = test_app
        .app
        .clone()
        .oneshot(request)
        .await
        .expect("Failed to execute request.");
    info!("response status: {:?}", response);
    assert!(response.status().is_success());
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let res_post: WeatherEntry = from_slice(&body).unwrap();

    let request_get = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/oracle/events/{}/entry/{}",
            oracle_event.id, res_post.id
        ))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::empty())
        .unwrap();

    let response_get = test_app
        .app
        .oneshot(request_get)
        .await
        .expect("Failed to execute request.");

    assert!(response_get.status().is_success());
    let body = to_bytes(response_get.into_body(), usize::MAX)
        .await
        .unwrap();
    let res: WeatherEntry = from_slice(&body).unwrap();
    assert_eq!(res_post.id, res.id);
    assert_eq!(res_post.event_id, res.event_id);
    assert_eq!(res_post.score, res.score);
    assert_eq!(res_post.expected_observations, res.expected_observations);
}
