use buska_core::search::notifications::{ProgressNotification, SearchNotification};
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use rdkafka::message::{Message, OwnedMessage};
use std::collections::HashMap;
use std::fs::File;
use std::io::{LineWriter, Write};
use tokio::sync::mpsc::Receiver;

#[derive(PartialEq)]
pub(crate) enum Out {
    Stdout,
    File(String),
}

/// Listens to search notifications and prints those to stdout
pub(crate) async fn cli_notifications_loop(
    mut receiver: Receiver<SearchNotification>,
    unbounded: bool,
    out: Out,
) {
    let mut stop = false;
    let mut bars: Option<HashMap<i32, ProgressBar>> = None;
    let mut step = 0;
    let mut writer: Option<LineWriter<File>> = None;
    let mut matches: Vec<OwnedMessage> = vec![];
    while !stop {
        if let Some(received) = receiver.recv().await {
            match received {
                SearchNotification::Prepare(preparation) => {
                    step += 1;
                    println!("[{step}/5] {preparation}");
                }
                SearchNotification::Start => println!("[5/5] Searching"),
                SearchNotification::Finish(summary) => {
                    stop = true;
                    if let Some(b) = bars.as_mut() {
                        for bar in b.values_mut() {
                            bar.finish();
                        }
                    }
                    println!("Finished searching. Summary:");
                    println!("{summary}");
                    if summary.matches > 0 && out == Out::Stdout {
                        println!("Matches:");
                        for matched in &matches {
                            println!(
                                "{}",
                                matched
                                    .payload_view::<str>()
                                    .expect("Could not display matched record as string")
                                    .expect("Could not display matched record as string")
                            );
                        }
                    }
                }
                SearchNotification::Progress(progress) => {
                    if !unbounded {
                        // can't display a progress bar if running infinitely
                        if let Some(b) = bars.as_mut() {
                            for (part, part_progress) in progress.per_partition_progress {
                                let bar = b.get_mut(&part).unwrap();
                                bar.set_position(part_progress.done as u64);
                                if part_progress.done >= part_progress.total {
                                    bar.finish();
                                }
                            }
                        } else {
                            let (_, progress_bars) = create_partition_bars(progress);
                            bars = Some(progress_bars);
                        }
                    }
                }
                SearchNotification::Match(matched) => match out {
                    Out::Stdout => {
                        println!("Match found:");
                        println!(
                            "{}",
                            matched
                                .payload_view::<str>()
                                .expect("Could not display matched record as string")
                                .expect("Could not display matched record as string")
                        );
                        matches.push(matched.clone())
                    }
                    Out::File(ref path) => {
                        if writer.as_ref().is_none() {
                            let file = File::create(path).expect("Could not create out file");
                            writer = Some(LineWriter::new(file));
                        }
                        writer
                            .as_mut()
                            .unwrap()
                            .write_all(
                                matched
                                    .payload_view::<str>()
                                    .expect("Could not display matched record as string")
                                    .expect("Could not display matched record as string")
                                    .as_bytes(),
                            )
                            .expect("Could not write match to out file");
                    }
                },
            }
        }
    }
}

fn create_partition_bars(
    progress: ProgressNotification,
) -> (MultiProgress, HashMap<i32, ProgressBar>) {
    let style = ProgressStyle::with_template(
        "{msg} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>7}/{len:7} ({eta})",
    )
    .expect("Internal error: wrong template")
    .with_key(
        "eta",
        |state: &ProgressState, w: &mut dyn std::fmt::Write| {
            write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
        },
    )
    .progress_chars("#>-");
    let mut map = HashMap::new();
    let mb = MultiProgress::new();
    for (part, part_progress) in progress.per_partition_progress {
        let part_bar = mb
            .add(
                ProgressBar::new(part_progress.total as u64)
                    .with_message(format!("Partition {part}: ")),
            )
            .with_style(style.clone());
        map.insert(part, part_bar);
    }
    (mb, map)
}
