#![warn(missing_docs)]

/*!
Streaming API for `roux`

Reddit's API does not provide "firehose"-style streaming of new posts and
comments. Instead, the endpoints for retrieving the latest posts and comments
have to be polled regularly. This crate automates that task and provides streams
for a subreddit's posts (submissions) and comments.

See [`stream_subreddit_submissions`] and [`stream_subreddit_comments`] for
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
use tokio_retry::Retry;

// TODO: Tests

/// Error that may happen when streaming submissions
#[derive(Debug)]
pub enum SubmissionStreamError<S>
where
    S: Sink<SubmissionsData> + Unpin,
{
    /// An issue with getting the data from Reddit
    Roux(RouxError),

    /// An issue with sending the data through the sink
    Sink(S::Error),
}

async fn _pull_submissions_into_sink<S>(
    subreddit: Subreddit,
    sleep_time: Duration,
    mut sink: S,
) -> Result<(), S::Error>
where
    S: Sink<Result<SubmissionsData, RouxError>> + Unpin,
{
    // How many submissions to fetch per request
    const LIMIT: u32 = 100;
    let mut seen_ids: HashSet<String> = HashSet::new();

    loop {
        debug!("Fetching latest submissions from r/{}", subreddit.name);
        let latest = subreddit.latest(LIMIT, None).await;
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
*/
pub fn stream_submissions(
    subreddit: &Subreddit,
    sleep_time: Duration,
) -> impl Stream<Item = Result<SubmissionsData, RouxError>> {
    let (sink, stream) = mpsc::unbounded();
    tokio::spawn(_pull_submissions_into_sink(
        // We need an owned instance (or at least statically bound
        // reference) for tokio::spawn. Since Subreddit isn't Copy
        // or Clone, we simply create a new instance.
        Subreddit::new(subreddit.name.as_str()),
        sleep_time,
        sink,
    ));
    stream
}

/// Error that may happen when streaming comments
#[derive(Debug)]
pub enum CommentStreamError<S>
where
    S: Sink<SubredditCommentsData> + Unpin,
{
    /// An issue with getting the data from Reddit
    Roux(RouxError),

    /// An issue with sending the data through the sink
    Sink(S::Error),
}

/**
Stream new comments in a subreddit

The subreddit is polled regularly for new comments, and each previously
unseen comment is sent into the sink.

`sleep_time` controls the interval between calls to the Reddit API, and depends
on how much traffic the subreddit has. Each call fetches the 100 latest items
(the maximum number allowed by Reddit). A warning is logged if none of those
items has been seen in the previous call: this indicates a potential miss of new
content and suggests that a smaller `sleep_time` should be chosen.

`retry_strategy` controls how to deal with errors that occur while fetching
content from Reddit. See [`tokio_retry::strategy`].
*/
pub async fn stream_subreddit_comments<S, R, I>(
    subreddit: &Subreddit,
    mut sink: S,
    sleep_time: Duration,
    retry_strategy: &R,
) -> Result<(), CommentStreamError<S>>
where
    S: Sink<SubredditCommentsData> + Unpin,
    R: IntoIterator<IntoIter = I, Item = Duration> + Clone,
    I: Iterator<Item = Duration>,
{
    // How many comments to fetch per request
    const LIMIT: u32 = 100;
    let mut seen_ids: HashSet<String> = HashSet::new();
    loop {
        let latest_comments = Retry::spawn(retry_strategy.clone(), || {
            subreddit.latest_comments(None, Some(LIMIT))
        })
        .await
        .map_err(CommentStreamError::Roux)?
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
                sink.send(comment).await.map_err(CommentStreamError::Sink)?;
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
        sleep(sleep_time).await;
    }
}
