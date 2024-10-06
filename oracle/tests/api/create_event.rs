use std::sync::Arc;

use crate::helpers::{spawn_app, MockWeatherAccess};
use axum::{
    body::{to_bytes, Body},
    http::Request,
};
use dlctix::Outcome;
use hyper::{header, Method};
use oracle::{CreateEvent, Event};
use serde_json::{from_slice, to_string};
use time::OffsetDateTime;
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn can_create_oracle_event() {
    let uri = String::from("/oracle/events");
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
    let body_json = to_string(&new_event).unwrap();
    let request = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body_json))
        .unwrap();

    let response = test_app
        .app
        .oneshot(request)
        .await
        .expect("Failed to execute request.");
    assert!(response.status().is_success());
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let res: Event = from_slice(&body).unwrap();
    assert_eq!(res.signing_date, new_event.signing_date);
    assert_eq!(res.locations, new_event.locations);
    assert_eq!(
        res.total_allowed_entries,
        new_event.total_allowed_entries as i64
    );
    assert_eq!(res.entry_ids.len(), 0);
    assert_eq!(
        res.number_of_values_per_entry,
        new_event.number_of_values_per_entry as i64
    );
    assert!(res.weather.is_empty());
    assert!(res.nonce.serialize().len() > 0);
    assert!(res.attestation.is_none());
    assert!(res
        .event_annoucement
        .is_valid_outcome(&Outcome::Attestation(1)));
}

#[tokio::test]
async fn can_create_and_get_oracle_event() {
    let uri = String::from("/oracle/events");
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
    let body_json = to_string(&new_event).unwrap();
    let request_post = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body_json))
        .unwrap();

    let response_post = test_app
        .app
        .clone()
        .oneshot(request_post)
        .await
        .expect("Failed to execute request.");
    assert!(response_post.status().is_success());
    let body = to_bytes(response_post.into_body(), usize::MAX)
        .await
        .unwrap();
    let res_post: Event = from_slice(&body).unwrap();

    let request_get = Request::builder()
        .method(Method::GET)
        .uri(format!("/oracle/events/{}", res_post.id))
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
    let res: Event = from_slice(&body).unwrap();
    assert_eq!(
        res.signing_date,
        new_event
            .signing_date
            .replace_nanosecond(new_event.signing_date.nanosecond() / 1_000 * 1_000)
            .unwrap()
    );
    assert_eq!(
        res.observation_date,
        new_event
            .observation_date
            .replace_nanosecond(new_event.observation_date.nanosecond() / 1_000 * 1_000)
            .unwrap()
    );
    assert_eq!(res.locations, new_event.locations);
    assert_eq!(
        res.total_allowed_entries,
        new_event.total_allowed_entries as i64
    );
    assert_eq!(res.entry_ids.len(), 0);
    assert_eq!(
        res.number_of_values_per_entry,
        new_event.number_of_values_per_entry as i64
    );
    assert!(res.weather.is_empty());
    assert!(res.nonce.serialize().len() > 0);
    assert!(res.attestation.is_none());
    assert!(res
        .event_annoucement
        .is_valid_outcome(&Outcome::Attestation(1)));
}
