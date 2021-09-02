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

use async_trait::async_trait;
use futures::channel::mpsc;
use futures::{Sink, SinkExt, Stream};
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
The [`roux`] APIs for submissions and comments are slightly different. We use
the [`Puller`] trait as the common interface to which we then adapt those APIs.
This allows us to implement our core logic (e.g. retries and duplicate
filtering) once without caring about the differences between submissions and
comments. In addition, this makes it easier to test the core logic because
we can provide a mock implementation.
*/
#[async_trait]
trait Puller<Data> {
    async fn pull(&self) -> Result<BasicThing<Listing<BasicThing<Data>>>, RouxError>;
    fn get_id(&self, data: &Data) -> String;
    fn get_items_name(&self) -> String;
    fn get_source_name(&self) -> String;
}

struct SubredditPuller {
    subreddit: Subreddit,
}

// How many items to fetch per request
const LIMIT: u32 = 100;

#[async_trait]
impl Puller<SubmissionsData> for SubredditPuller {
    async fn pull(&self) -> Result<BasicThing<Listing<BasicThing<SubmissionsData>>>, RouxError> {
        self.subreddit.latest(LIMIT, None).await
    }

    fn get_id(&self, data: &SubmissionsData) -> String {
        data.id.clone()
    }

    fn get_items_name(&self) -> String {
        "submissions".to_owned()
    }

    fn get_source_name(&self) -> String {
        format!("r/{}", self.subreddit.name)
    }
}

#[async_trait]
impl Puller<SubredditCommentsData> for SubredditPuller {
    async fn pull(
        &self,
    ) -> Result<BasicThing<Listing<BasicThing<SubredditCommentsData>>>, RouxError> {
        self.subreddit.latest_comments(None, Some(LIMIT)).await
    }

    fn get_id(&self, data: &SubredditCommentsData) -> String {
        data.id.as_ref().cloned().unwrap()
    }

    fn get_items_name(&self) -> String {
        "comments".to_owned()
    }

    fn get_source_name(&self) -> String {
        format!("r/{}", self.subreddit.name)
    }
}

/**
Pull new items from Reddit and push them into a sink.

This function contains the core of the streaming logic. It

1. pulls latest items (submissions or comments) from Reddit, retrying that
   operation if necessary according to `retry_strategy`,
2. filters out already seen items using their ID,
3. pushes the new items (or an error if pulling failed) into `sink`,
4. sleeps for `sleep_time`,

and then repeats that process for ever.
*/
async fn pull_into_sink<S, R, Data>(
    puller: &(dyn Puller<Data> + Send + Sync),
    sleep_time: Duration,
    retry_strategy: R,
    mut sink: S,
) -> Result<(), S::Error>
where
    S: Sink<Result<Data, RouxError>> + Unpin,
    R: IntoIterator<Item = Duration> + Clone,
{
    let items_name = puller.get_items_name();
    let source_name = puller.get_source_name();
    let mut seen_ids: HashSet<String> = HashSet::new();

    loop {
        debug!("Fetching latest {} from {}", items_name, source_name);
        let latest = RetryIf::spawn(
            retry_strategy.clone(),
            || puller.pull(),
            |error: &RouxError| {
                debug!(
                    "Error while fetching the latest {} from {}: {}",
                    items_name, source_name, error,
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
                    let id = puller.get_id(&item);
                    latest_ids.insert(id.clone());
                    if !seen_ids.contains(&id) {
                        num_new += 1;
                        sink.send(Ok(item)).await?;
                    }
                }

                debug!(
                    "Got {} new {} for {} (out of {})",
                    num_new, items_name, source_name, LIMIT
                );
                if num_new == LIMIT && !seen_ids.is_empty() {
                    warn!(
                        "All received {} for {} were new, try a shorter sleep_time",
                        items_name, source_name
                    );
                }

                seen_ids = latest_ids;
            }
            Err(error) => {
                // Forward the error through the stream
                warn!(
                    "Error while fetching the latest {} from {}: {}",
                    items_name, source_name, error,
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
    // We need an owned instance (or at least statically bound
    // reference) for tokio::spawn. Since Subreddit isn't Clone,
    // we simply create a new instance.
    let subreddit = Subreddit::new(subreddit.name.as_str());
    tokio::spawn(async move {
        pull_into_sink(
            &SubredditPuller { subreddit },
            sleep_time,
            retry_strategy,
            sink,
        )
        .await
    });
    stream
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
    // We need an owned instance (or at least statically bound
    // reference) for tokio::spawn. Since Subreddit isn't Clone,
    // we simply create a new instance.
    let subreddit = Subreddit::new(subreddit.name.as_str());
    tokio::spawn(async move {
        pull_into_sink(
            &SubredditPuller { subreddit },
            sleep_time,
            retry_strategy,
            sink,
        )
        .await
    });
    stream
}
