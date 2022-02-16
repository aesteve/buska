use std::collections::BTreeMap;
use axum::{
    Router,
    routing::{get, service_method_routing as service},
    response::sse::{Event, KeepAlive, Sse},
};
use serde::Serialize;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use axum::error_handling::HandleErrorExt;
use axum::http::StatusCode;
use chrono::{DateTime, Duration, Utc};
use futures::stream::Stream;
use rdkafka::Message;
use tokio::sync::mpsc::Receiver;
use tower_http::{services::ServeDir, trace::TraceLayer};
use buska_core::config::KafkaClusterConfig;
use buska_core::search::bounds::{SearchBounds, SearchEnd, SearchStart};
use buska_core::search::extractors::json::json_single_extract;
use buska_core::search::matchers::RegexMatch;
use buska_core::search::notifications::{PreparationStep, Progress, ProgressNotification, SearchNotification};
use buska_core::search::SearchDefinition;
use buska_core::search_topic;

#[tokio::main]
async fn main() {

    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "example_sse=debug,tower_http=debug")
    }

    let static_files_service =
        service::get(ServeDir::new("web/assets").append_index_html_on_directories(true))
            .handle_error(|error: std::io::Error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Unhandled internal error: {}", error),
                )
            });

    let app = Router::new()
        .fallback(static_files_service)
        .route("/sse", get(sse_handler))
        .layer(TraceLayer::new_for_http());

    // run it
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn sse_handler() -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let conf = KafkaClusterConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        security: None
    };
    let bounds = SearchBounds {
        start: SearchStart::Earliest,
        end: SearchEnd::CurrentLast
    };
    let (sender, receiver) = tokio::sync::mpsc::channel::<SearchNotification>(1024);
    let matcher = RegexMatch::new("some-[0-9]{1}$").unwrap();
    let extractor = json_single_extract("$.nested.string").unwrap();
    tokio::task::spawn(async move {
        search_topic(
            conf,
            "test".to_string(),
            sender,
            bounds,
            &mut SearchDefinition::new(extractor, Box::new(matcher)),
            Duration::milliseconds(100)
        ).await;
    });
    Sse::new(SseSearchNotifications { recv: receiver })
        .keep_alive(KeepAlive::default())
}

struct SseSearchNotifications {
    recv: Receiver<SearchNotification>,
}

impl tokio_stream::Stream for SseSearchNotifications {
    type Item = Result<Event, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, co: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.recv
            .poll_recv(co)
            .map(|maybe_notif| maybe_notif.map(notif_as_event))
    }
}

#[derive(Serialize)]
struct SearchSseEvent {
    step: usize,
    description: String
}

#[derive(Serialize)]
struct ProgressSseEvent {
    pub overall_progress: Progress,
    pub per_partition_progress: BTreeMap<String, Progress>,
    pub matches: i64,
    pub elapsed: i64,
    pub eta: DateTime<Utc>
}

impl ProgressSseEvent {
    fn new(progress: ProgressNotification) -> Self {
        ProgressSseEvent {
            overall_progress: progress.overall_progress,
            per_partition_progress: string_keys(progress.per_partition_progress),
            matches: progress.matches,
            elapsed: progress.elapsed.num_milliseconds(),
            eta: progress.eta
        }
    }
}

fn string_keys(map: BTreeMap<i32, Progress>) -> BTreeMap<String, Progress> {
    map.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn notif_as_event(notif: SearchNotification) -> Result<Event, Infallible> {
    let e = match notif {
        SearchNotification::Prepare(PreparationStep::CreateClient) =>
            Event::default()
                .event("step")
                .data(serde_json::to_string(&SearchSseEvent { step: 1, description: "Creating Kafka client".to_string() }).unwrap()),
        SearchNotification::Prepare(PreparationStep::FetchMetadata) =>
            Event::default()
                .event("step")
                .data(serde_json::to_string(&SearchSseEvent { step: 2, description: "Fetching topic metadata".to_string() }).unwrap()),
        SearchNotification::Prepare(PreparationStep::FetchWatermarks) =>
            Event::default()
                .event("step")
                .data(serde_json::to_string(&SearchSseEvent { step: 3, description: "Fetching topic watermarks (min/max)".to_string() }).unwrap()),
        SearchNotification::Prepare(PreparationStep::SeekPartitions) =>
            Event::default()
                .event("step")
                .data(serde_json::to_string(&SearchSseEvent { step: 4, description: "Seeking partitions to desired offsets".to_string() }).unwrap()),
        SearchNotification::Start =>
            Event::default()
                .event("step")
                .data(serde_json::to_string(&SearchSseEvent { step: 5, description: "Search started".to_string() }).unwrap()),
        SearchNotification::Finish(_) =>
            Event::default()
                .event("step")
                .data(serde_json::to_string(&SearchSseEvent { step: 6, description: "Search finished".to_string() }).unwrap()),
        SearchNotification::Progress(p) =>
            Event::default()
                .event("search-progress")
                .data(serde_json::to_string(&ProgressSseEvent::new(p)).unwrap()),
        SearchNotification::Match(m) => {
            Event::default()
                .event("match")
                .data(m.payload_view::<str>().unwrap().unwrap())
        }
    };
    Ok(e)
}