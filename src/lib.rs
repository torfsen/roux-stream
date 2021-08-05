#![warn(missing_docs)]

/*!
Streaming API for [`roux`]

Reddit's API does not provide "firehose"-style streaming of new posts and
comments. Instead, the endpoints for retrieving the latest posts and comments
have to be polled regularly. This crate automates that task and provides streams
for a subreddit's posts (submissions) and comments.

See [`stream_submissions`] and [`stream_comments`] for
details.

# Logging

This module uses the logging infrastructure provided by the [`log`] crate.
*/

use futures::channel::mpsc;
use futures::{Sink, SinkExt, Stream};
use log::{debug, warn};
use roux::subreddit::responses::{comments::SubredditCommentsData, SubmissionsData};
use roux::{util::RouxError, Subreddit};
use std::collections::HashSet;
use std::marker::Unpin;
use tokio::time::{sleep, Duration};
use tokio_retry::RetryIf;

// TODO: Tests

async fn pull_submissions_into_sink<S, R, I>(
    subreddit: Subreddit,
    sleep_time: Duration,
    retry_strategy: R,
    mut sink: S,
) -> Result<(), S::Error>
where
    S: Sink<Result<SubmissionsData, RouxError>> + Unpin,
    R: IntoIterator<IntoIter = I, Item = Duration> + Clone,
    I: Iterator<Item = Duration>,
{
    // How many submissions to fetch per request
    const LIMIT: u32 = 100;
    let mut seen_ids: HashSet<String> = HashSet::new();

    loop {
        debug!("Fetching latest submissions from r/{}", subreddit.name);
        let latest = RetryIf::spawn(
            retry_strategy.clone(),
            || subreddit.latest(LIMIT, None),
            |error: &RouxError| {
                debug!(
                    "Error while fetching the latest submissions from r/{}: {}",
                    subreddit.name, error,
                );
                true
            },
        )
        .await;
        match latest {
            Ok(latest_submissions) => {
                let latest_submissions = latest_submissions
                    .data
                    .children
                    .into_iter()
                    .map(|thing| thing.data);

                let mut latest_ids: HashSet<String> = HashSet::new();

                let mut num_new = 0;
                for submission in latest_submissions {
                    latest_ids.insert(submission.id.clone());
                    if !seen_ids.contains(&submission.id) {
                        num_new += 1;
                        sink.send(Ok(submission)).await?;
                    }
                }

                debug!(
                    "Got {} new submissions for r/{} (out of {})",
                    num_new, subreddit.name, LIMIT
                );
                if num_new == LIMIT && !seen_ids.is_empty() {
                    warn!(
                        "All received submissions for r/{} were new, try a shorter sleep_time",
                        subreddit.name
                    );
                }

                seen_ids = latest_ids;
            }
            Err(error) => {
                // Forward the error through the stream
                warn!(
                    "Error while fetching the latest submissions from r/{}: {}",
                    subreddit.name, error,
                );
                sink.send(Err(error)).await?;
            }
        }

        sleep(sleep_time).await;
    }
}

/**
Stream new submissions in a subreddit

The subreddit is polled regularly for new submissions, and each previously
unseen submission is sent into the returned stream.

`sleep_time` controls the interval between calls to the Reddit API, and depends
on how much traffic the subreddit has. Each call fetches the 100 latest items
(the maximum number allowed by Reddit). A warning is logged if none of those
items has been seen in the previous call: this indicates a potential miss of new
content and suggests that a smaller `sleep_time` should be chosen.

For details on `retry_strategy` see [`tokio_retry`].
*/
pub fn stream_submissions<R, I>(
    subreddit: &Subreddit,
    sleep_time: Duration,
    retry_strategy: R,
) -> impl Stream<Item = Result<SubmissionsData, RouxError>>
where
    R: IntoIterator<IntoIter = I, Item = Duration> + Clone + Send + Sync + 'static,
    I: Iterator<Item = Duration> + Send + Sync + 'static,
{
    let (sink, stream) = mpsc::unbounded();
    tokio::spawn(pull_submissions_into_sink(
        // We need an owned instance (or at least statically bound
        // reference) for tokio::spawn. Since Subreddit isn't Copy
        // or Clone, we simply create a new instance.
        Subreddit::new(subreddit.name.as_str()),
        sleep_time,
        retry_strategy,
        sink,
    ));
    stream
}

async fn pull_comments_into_sink<S, R, I>(
    subreddit: Subreddit,
    sleep_time: Duration,
    retry_strategy: R,
    mut sink: S,
) -> Result<(), S::Error>
where
    S: Sink<Result<SubredditCommentsData, RouxError>> + Unpin,
    R: IntoIterator<IntoIter = I, Item = Duration> + Clone,
    I: Iterator<Item = Duration>,
{
    // How many submissions to fetch per request
    const LIMIT: u32 = 100;
    let mut seen_ids: HashSet<String> = HashSet::new();

    loop {
        debug!("Fetching latest comments from r/{}", subreddit.name);
        let latest = RetryIf::spawn(
            retry_strategy.clone(),
            || subreddit.latest_comments(None, Some(LIMIT)),
            |error: &RouxError| {
                debug!(
                    "Error while fetching the latest comments from r/{}: {}",
                    subreddit.name, error,
                );
                true
            },
        )
        .await;
        match latest {
            Ok(latest_comments) => {
                let latest_comments = latest_comments
                    .data
                    .children
                    .into_iter()
                    .map(|thing| thing.data);

                let mut latest_ids: HashSet<String> = HashSet::new();

                let mut num_new = 0;
                for comment in latest_comments {
                    let id = comment.id.as_ref().cloned().unwrap();
                    latest_ids.insert(id.clone());
                    if !seen_ids.contains(&id) {
                        num_new += 1;
                        sink.send(Ok(comment)).await?;
                    }
                }

                debug!(
                    "Got {} new comments for r/{} (out of {})",
                    num_new, subreddit.name, LIMIT
                );
                if num_new == LIMIT && !seen_ids.is_empty() {
                    warn!(
                        "All received comments for r/{} were new, try a shorter sleep_time",
                        subreddit.name
                    );
                }

                seen_ids = latest_ids;
            }
            Err(error) => {
                // Forward the error through the stream
                warn!(
                    "Error while fetching the latest comments from r/{}: {}",
                    subreddit.name, error,
                );
                sink.send(Err(error)).await?;
            }
        }

        sleep(sleep_time).await;
    }
}

/**
Stream new comments in a subreddit

The subreddit is polled regularly for new comments, and each previously
unseen comment is sent into the returned stream.

`sleep_time` controls the interval between calls to the Reddit API, and depends
on how much traffic the subreddit has. Each call fetches the 100 latest items
(the maximum number allowed by Reddit). A warning is logged if none of those
items has been seen in the previous call: this indicates a potential miss of new
content and suggests that a smaller `sleep_time` should be chosen.

For details on `retry_strategy` see [`tokio_retry`].
*/
pub fn stream_comments<R, I>(
    subreddit: &Subreddit,
    sleep_time: Duration,
    retry_strategy: R,
) -> impl Stream<Item = Result<SubredditCommentsData, RouxError>>
where
    R: IntoIterator<IntoIter = I, Item = Duration> + Clone + Send + Sync + 'static,
    I: Iterator<Item = Duration> + Send + Sync + 'static,
{
    let (sink, stream) = mpsc::unbounded();
    tokio::spawn(pull_comments_into_sink(
        // We need an owned instance (or at least statically bound
        // reference) for tokio::spawn. Since Subreddit isn't Copy
        // or Clone, we simply create a new instance.
        Subreddit::new(subreddit.name.as_str()),
        sleep_time,
        retry_strategy,
        sink,
    ));
    stream
}
