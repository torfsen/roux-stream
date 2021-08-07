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
use futures::{Future, Sink, SinkExt, Stream};
use log::{debug, warn};
use roux::responses::{BasicThing, Listing};
use roux::subreddit::responses::{comments::SubredditCommentsData, SubmissionsData};
use roux::{util::RouxError, Subreddit};
use std::collections::HashSet;
use std::marker::Unpin;
use tokio::time::{sleep, Duration};
use tokio_retry::RetryIf;

// TODO: Tests

/**
Pull new items from Reddit and push them into a sink.

This function contains the core of the streaming logic. It

    1. pulls latest items (submissions or comments) from Reddit, retrying that
       operation if necessary according to `retry_strategy`,
    2. filters out already seen items using their ID,
    3. pushes the new items (or an error if pulling failed) into `sink`,
    4. sleeps for `sleep_time`,

and then repeats that process for ever.

The function can be used to pull either submissions or comments by
providing suitable `pull` and `get_id` callbacks that account for slight
differences in the `roux` API for submissions and comments.
*/
async fn pull_into_sink<S, R, Pull, PullFut, GetId, Data>(
    subreddit: &Subreddit,
    sleep_time: Duration,
    retry_strategy: R,
    mut sink: S,
    item_name: &str,
    limit: u32,
    pull: Pull,
    get_id: GetId,
) -> Result<(), S::Error>
where
    S: Sink<Result<Data, RouxError>> + Unpin,
    R: IntoIterator<Item = Duration> + Clone,
    Pull: Fn(u32) -> PullFut,
    PullFut: Future<Output = Result<BasicThing<Listing<BasicThing<Data>>>, RouxError>>,
    GetId: Fn(&Data) -> String,
{
    let mut seen_ids: HashSet<String> = HashSet::new();

    loop {
        debug!("Fetching latest {}s from r/{}", item_name, subreddit.name);
        let latest = RetryIf::spawn(
            retry_strategy.clone(),
            || pull(limit),
            |error: &RouxError| {
                debug!(
                    "Error while fetching the latest {}s from r/{}: {}",
                    item_name, subreddit.name, error,
                );
                true
            },
        )
        .await;
        match latest {
            Ok(latest_items) => {
                let latest_items = latest_items.data.children.into_iter().map(|item| item.data);
                let mut latest_ids: HashSet<String> = HashSet::new();

                let mut num_new = 0;
                for item in latest_items {
                    let id = get_id(&item);
                    latest_ids.insert(id.clone());
                    if !seen_ids.contains(&id) {
                        num_new += 1;
                        sink.send(Ok(item)).await?;
                    }
                }

                debug!(
                    "Got {} new {}s for r/{} (out of {})",
                    num_new, item_name, subreddit.name, limit
                );
                if num_new == limit && !seen_ids.is_empty() {
                    warn!(
                        "All received {}s for r/{} were new, try a shorter sleep_time",
                        item_name, subreddit.name
                    );
                }

                seen_ids = latest_ids;
            }
            Err(error) => {
                // Forward the error through the stream
                warn!(
                    "Error while fetching the latest {}s from r/{}: {}",
                    item_name, subreddit.name, error,
                );
                sink.send(Err(error)).await?;
            }
        }

        sleep(sleep_time).await;
    }
}

async fn pull_submissions_into_sink<S, R>(
    subreddit: Subreddit,
    sleep_time: Duration,
    retry_strategy: R,
    sink: S,
) -> Result<(), S::Error>
where
    S: Sink<Result<SubmissionsData, RouxError>> + Unpin,
    R: IntoIterator<Item = Duration> + Clone,
{
    pull_into_sink(
        &subreddit,
        sleep_time,
        retry_strategy,
        sink,
        "submission",
        100,
        |limit| subreddit.latest(limit, None),
        |submission| submission.id.clone(),
    )
    .await
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

async fn pull_comments_into_sink<S, R>(
    subreddit: Subreddit,
    sleep_time: Duration,
    retry_strategy: R,
    sink: S,
) -> Result<(), S::Error>
where
    S: Sink<Result<SubredditCommentsData, RouxError>> + Unpin,
    R: IntoIterator<Item = Duration> + Clone,
{
    pull_into_sink(
        &subreddit,
        sleep_time,
        retry_strategy,
        sink,
        "comment",
        100,
        |limit| subreddit.latest_comments(None, Some(limit)),
        |comment| comment.id.as_ref().cloned().unwrap(),
    )
    .await
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
