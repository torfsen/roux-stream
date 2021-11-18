/*
Copyright (c) 2021 Florian Brucker (www.florianbrucker.de)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

/*!
A streaming API for the [`roux`] Reddit client.

Reddit's API does not provide "firehose"-style streaming of new posts and
comments. Instead, the endpoints for retrieving the latest posts and comments
have to be polled regularly. This crate automates that task and provides streams
for a subreddit's posts (submissions) and comments.

See [`stream_submissions`] and [`stream_comments`] for
details.

# Logging

This module uses the logging infrastructure provided by the [`log`] crate.
*/

#![warn(missing_docs)]

use async_trait::async_trait;
use futures::channel::mpsc;
use futures::{Sink, SinkExt, Stream};
use log::{debug, warn};
use roux::responses::{BasicThing, Listing};
use roux::subreddit::responses::{comments::SubredditCommentsData, SubmissionsData};
use roux::{util::RouxError, Subreddit};
use std::collections::HashSet;
use std::error::Error;
use std::marker::Unpin;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_retry::RetryIf;

/**
The [`roux`] APIs for submissions and comments are slightly different. We use
the [`Puller`] trait as the common interface to which we then adapt those APIs.
This allows us to implement our core logic (e.g. retries and duplicate
filtering) once without caring about the differences between submissions and
comments. In addition, this makes it easier to test the core logic because
we can provide a mock implementation.
*/
#[async_trait]
trait Puller<Data, E: Error> {
    // The "real" implementations of this function (for pulling
    // submissions and comments from Reddit) would not need `self` to
    // be `mut` here (because there the state change happens externally,
    // i.e. within Reddit). However, writing good tests is much easier
    // if `self` is mutable here.
    async fn pull(&mut self) -> Result<BasicThing<Listing<BasicThing<Data>>>, E>;
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
impl Puller<SubmissionsData, RouxError> for SubredditPuller {
    async fn pull(
        &mut self,
    ) -> Result<BasicThing<Listing<BasicThing<SubmissionsData>>>, RouxError> {
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
impl Puller<SubredditCommentsData, RouxError> for SubredditPuller {
    async fn pull(
        &mut self,
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
async fn pull_into_sink<S, R, Data, E>(
    puller: &mut (dyn Puller<Data, E> + Send + Sync),
    sleep_time: Duration,
    retry_strategy: R,
    mut sink: S,
) -> Result<(), S::Error>
where
    S: Sink<Result<Data, E>> + Unpin,
    R: IntoIterator<Item = Duration> + Clone,
    E: Error,
{
    let items_name = puller.get_items_name();
    let source_name = puller.get_source_name();
    let mut seen_ids: HashSet<String> = HashSet::new();

    /*
    Because `puller.pull` takes a mutable reference we need wrap it in
    a mutex to be able to pass it as a callback to `RetryIf::spawn`.
     */
    let puller_mutex = Mutex::new(puller);

    loop {
        debug!("Fetching latest {} from {}", items_name, source_name);
        let latest = RetryIf::spawn(
            retry_strategy.clone(),
            || async { puller_mutex.lock().await.pull().await },
            |error: &E| {
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
                let puller = puller_mutex.lock().await;
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
                if num_new == latest_ids.len() && !seen_ids.is_empty() {
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
Spawn a task that pulls items and puts them into a stream.

Depending on `T`, this function will either stream submissions or comments.
*/
fn stream_items<R, I, T>(
    subreddit: &Subreddit,
    sleep_time: Duration,
    retry_strategy: R,
) -> (
    impl Stream<Item = Result<T, RouxError>>,
    JoinHandle<Result<(), mpsc::SendError>>,
)
where
    R: IntoIterator<IntoIter = I, Item = Duration> + Clone + Send + Sync + 'static,
    I: Iterator<Item = Duration> + Send + Sync + 'static,
    SubredditPuller: Puller<T, RouxError>,
    T: Send + 'static,
{
    let (sink, stream) = mpsc::unbounded();
    // We need an owned instance (or at least statically bound
    // reference) for tokio::spawn. Since Subreddit isn't Clone,
    // we simply create a new instance.
    let subreddit = Subreddit::new(subreddit.name.as_str());
    let join_handle = tokio::spawn(async move {
        pull_into_sink(
            &mut SubredditPuller { subreddit },
            sleep_time,
            retry_strategy,
            sink,
        )
        .await
    });
    (stream, join_handle)
}

/**
Stream new submissions in a subreddit.

Creates a separate tokio task that regularly polls the subreddit for new
submissions. Previously unseen submissions are sent into the returned
stream.

Returns a tuple `(stream, join_handle)` where `stream` is the
[`Stream`](futures::Stream) from which the submissions can be read, and
`join_handle` is the [`JoinHandle`](tokio::task::JoinHandle) for the
polling task.

`sleep_time` controls the interval between calls to the Reddit API, and
depends on how much traffic the subreddit has. Each call fetches the 100
latest items (the maximum number allowed by Reddit). A warning is logged
if none of those items has been seen in the previous call: this indicates
a potential miss of new content and suggests that a smaller `sleep_time`
should be chosen. Enable debug logging for more statistics.

If an error occurs while fetching the latest submissions from Reddit then
fetching is retried according to `retry_strategy` (see [`tokio_retry`] for
details). If one of the retries succeeds then normal operation is resumed.
If `retry_strategy` is finite and the last retry fails then its error is
sent into the stream, afterwards normal operation is resumed.

The spawned task runs indefinitely unless an error is encountered when
sending data into the stream (for example because the receiver is dropped).
In that case the task stops and the error is returned via `join_handle`.

See also [`stream_comments`].


# Example

The following example prints new submissions to
[r/AskReddit](https://reddit.com/r/AskReddit) in an endless loop.

```
use std::time::Duration;

use futures::StreamExt;
use roux::Subreddit;
use roux_stream::stream_submissions;
use tokio_retry::strategy::ExponentialBackoff;


#[tokio::main]
async fn main() {
    let subreddit = Subreddit::new("AskReddit");

    // How often to retry when pulling the data from Reddit fails and
    // how long to wait between retries. See the docs of `tokio_retry`
    // for details.
    let retry_strategy = ExponentialBackoff::from_millis(5).factor(100).take(3);

    let (mut stream, join_handle) = stream_submissions(
        &subreddit,
        Duration::from_secs(60),
        retry_strategy,
    );

    while let Some(submission) = stream.next().await {
        // `submission` is an `Err` if getting the latest submissions
        // from Reddit failed even after retrying.
        let submission = submission.unwrap();
        println!("\"{}\" by {}", submission.title, submission.author);
        # // An endless loop doesn't work well with doctests, so in that
        # // case we abort the task and exit the loop directly.
        # join_handle.abort();
        # break;
    }
    # // Aborting the task will make the join handle return an error. Let's
    # // make sure it's the right one.
    # let join_result = join_handle.await;
    # assert!(join_result.is_err());
    # assert!(join_result.err().unwrap().is_cancelled());
    # // Now we need to make sure that the remaining code in the example
    # // still works, so we create a fake `join_handle` for it to work
    # // with.
    # let join_handle = async { Some(Some(())) };

    // In case there was an error sending the submissions through the
    // stream, `join_handle` will report it.
    join_handle.await.unwrap().unwrap();
}
```
*/
pub fn stream_submissions<R, I>(
    subreddit: &Subreddit,
    sleep_time: Duration,
    retry_strategy: R,
) -> (
    impl Stream<Item = Result<SubmissionsData, RouxError>>,
    JoinHandle<Result<(), mpsc::SendError>>,
)
where
    R: IntoIterator<IntoIter = I, Item = Duration> + Clone + Send + Sync + 'static,
    I: Iterator<Item = Duration> + Send + Sync + 'static,
{
    stream_items(subreddit, sleep_time, retry_strategy)
}

/**
Stream new comments in a subreddit.

Creates a separate tokio task that regularly polls the subreddit for new
comments. Previously unseen comments are sent into the returned
stream.

Returns a tuple `(stream, join_handle)` where `stream` is the
[`Stream`](futures::Stream) from which the comments can be read, and
`join_handle` is the [`JoinHandle`](tokio::task::JoinHandle) for the
polling task.

`sleep_time` controls the interval between calls to the Reddit API, and
depends on how much traffic the subreddit has. Each call fetches the 100
latest items (the maximum number allowed by Reddit). A warning is logged
if none of those items has been seen in the previous call: this indicates
a potential miss of new content and suggests that a smaller `sleep_time`
should be chosen. Enable debug logging for more statistics.

If an error occurs while fetching the latest comments from Reddit then
fetching is retried according to `retry_strategy` (see [`tokio_retry`] for
details). If one of the retries succeeds then normal operation is resumed.
If `retry_strategy` is finite and the last retry fails then its error is
sent into the stream, afterwards normal operation is resumed.

The spawned task runs indefinitely unless an error is encountered when
sending data into the stream (for example because the receiver is dropped).
In that case the task stops and the error is returned via `join_handle`.

See also [`stream_submissions`].


# Example

The following example prints new comments to
[r/AskReddit](https://reddit.com/r/AskReddit) in an endless loop.

```
use std::time::Duration;

use futures::StreamExt;
use roux::Subreddit;
use roux_stream::stream_comments;
use tokio_retry::strategy::ExponentialBackoff;


#[tokio::main]
async fn main() {
    let subreddit = Subreddit::new("AskReddit");

    // How often to retry when pulling the data from Reddit fails and
    // how long to wait between retries. See the docs of `tokio_retry`
    // for details.
    let retry_strategy = ExponentialBackoff::from_millis(5).factor(100).take(3);

    let (mut stream, join_handle) = stream_comments(
        &subreddit,
        Duration::from_secs(10),
        retry_strategy,
    );

    while let Some(comment) = stream.next().await {
        // `comment` is an `Err` if getting the latest comments
        // from Reddit failed even after retrying.
        let comment = comment.unwrap();
        println!(
            "{}{} (by u/{})",
            comment.link_url.unwrap(),
            comment.id.unwrap(),
            comment.author.unwrap()
        );
        # // An endless loop doesn't work well with doctests, so in that
        # // case we abort the task and exit the loop directly.
        # join_handle.abort();
        # break;
    }
    # // Aborting the task will make the join handle return an error. Let's
    # // make sure it's the right one.
    # let join_result = join_handle.await;
    # assert!(join_result.is_err());
    # assert!(join_result.err().unwrap().is_cancelled());
    # // Now we need to make sure that the remaining code in the example
    # // still works, so we create a fake `join_handle` for it to work
    # // with.
    # let join_handle = async { Some(Some(())) };

    // In case there was an error sending the submissions through the
    // stream, `join_handle` will report it.
    join_handle.await.unwrap().unwrap();
}
```
*/
pub fn stream_comments<R, I>(
    subreddit: &Subreddit,
    sleep_time: Duration,
    retry_strategy: R,
) -> (
    impl Stream<Item = Result<SubredditCommentsData, RouxError>>,
    JoinHandle<Result<(), mpsc::SendError>>,
)
where
    R: IntoIterator<IntoIter = I, Item = Duration> + Clone + Send + Sync + 'static,
    I: Iterator<Item = Duration> + Send + Sync + 'static,
{
    stream_items(subreddit, sleep_time, retry_strategy)
}

#[cfg(test)]
mod tests {
    use crate::{pull_into_sink, Puller};
    use async_trait::async_trait;
    use futures::{channel::mpsc, StreamExt};
    use log::{Level, LevelFilter};
    use logtest::Logger;
    use roux::responses::{BasicThing, Listing};
    use std::{error::Error, fmt::Display, time::Duration};
    use tokio::sync::RwLock;

    /*
    Any test case that checks the logging output must run in isolation,
    so that the log output of other test cases does not disturb it. We
    use an `RwLock` to achieve that: tests that do log checking take a
    write lock, while the other test cases take a read lock.
    */
    static LOCK: RwLock<()> = RwLock::const_new(());

    #[derive(Debug, PartialEq)]
    struct PullerError(String);

    impl Display for PullerError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl Error for PullerError {}

    struct MockPuller {
        iter: Box<dyn Iterator<Item = Vec<String>> + Sync + Send>,
    }

    impl MockPuller {
        fn new(batches: Vec<Vec<&str>>) -> Self {
            MockPuller {
                iter: Box::new(
                    batches
                        .iter()
                        .map(|batch| batch.iter().map(|item| item.to_string()).collect())
                        .collect::<Vec<Vec<String>>>()
                        .into_iter(),
                ),
            }
        }
    }

    #[async_trait]
    impl Puller<String, PullerError> for MockPuller {
        /*
        Each call to `pull` returns the next batch of items. If a batch
        consists of a single String that begins with "error" then instead
        of an Ok an Err is returned.
        */
        async fn pull(&mut self) -> Result<BasicThing<Listing<BasicThing<String>>>, PullerError> {
            let children;
            if let Some(items) = self.iter.next() {
                match items.as_slice() {
                    [item] if item.starts_with("error") => {
                        return Err(PullerError(item.clone()));
                    }
                    _ => {
                        children = items
                            .iter()
                            .map(|item| BasicThing {
                                kind: "mock".to_owned(),
                                data: item.clone(),
                            })
                            .collect();
                    }
                }
            } else {
                children = vec![];
            }

            let listing = Listing {
                modhash: None,
                dist: None,
                after: None,
                before: None,
                children: children,
            };
            let result = BasicThing {
                kind: "listing".to_owned(),
                data: listing,
            };
            Ok(result)
        }

        fn get_id(&self, data: &String) -> String {
            data.clone()
        }

        fn get_items_name(&self) -> String {
            "MockItems".to_owned()
        }

        fn get_source_name(&self) -> String {
            "MockSource".to_owned()
        }
    }

    async fn check<R, I>(
        responses: Vec<Vec<&str>>,
        retry_strategy: R,
        expected: Vec<Result<&str, PullerError>>,
    ) where
        R: IntoIterator<IntoIter = I, Item = Duration> + Clone + Send + Sync + 'static,
        I: Iterator<Item = Duration> + Send + Sync + 'static,
    {
        let mut mock_puller = MockPuller::new(responses);
        let (sink, stream) = mpsc::unbounded();
        tokio::spawn(async move {
            pull_into_sink(
                &mut mock_puller,
                Duration::from_millis(1),
                retry_strategy,
                sink,
            )
            .await
        });
        let items = stream.take(expected.len()).collect::<Vec<_>>().await;
        assert_eq!(
            items,
            expected
                .into_iter()
                .map(|result| result.map(|ok_value| ok_value.to_string()))
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn test_simple_pull() {
        let _lock = LOCK.read().await;
        check(vec![vec!["hello"]], vec![], vec![Ok("hello")]).await;
    }

    #[tokio::test]
    async fn test_duplicate_filtering() {
        let _lock = LOCK.read().await;
        check(
            vec![vec!["a", "b", "c"], vec!["b", "c", "d"], vec!["d", "e"]],
            vec![],
            vec![Ok("a"), Ok("b"), Ok("c"), Ok("d"), Ok("e")],
        )
        .await;
    }

    #[tokio::test]
    async fn test_success_after_retry() {
        let _lock = LOCK.read().await;
        check(
            vec![
                vec!["a", "b", "c"],
                vec!["error1"],
                vec!["error2"],
                vec!["b", "c", "d"],
            ],
            vec![Duration::from_millis(1), Duration::from_millis(1)],
            vec![Ok("a"), Ok("b"), Ok("c"), Ok("d")],
        )
        .await;
    }

    #[tokio::test]
    async fn test_failure_after_retry() {
        let _lock = LOCK.read().await;
        check(
            vec![
                vec!["a", "b", "c"],
                vec!["error1"],
                vec!["error2"],
                vec!["b", "c", "d"],
            ],
            vec![Duration::from_millis(1)],
            vec![
                Ok("a"),
                Ok("b"),
                Ok("c"),
                Err(PullerError("error2".to_owned())),
                Ok("d"),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_warning_if_all_items_are_unseen() {
        let _lock = LOCK.write().await; // exclusive lock
        let mut logger = Logger::start();
        log::set_max_level(LevelFilter::Warn);
        check(
            vec![vec!["a", "b"], vec!["c", "d"]],
            vec![],
            vec![Ok("a"), Ok("b"), Ok("c"), Ok("d")],
        )
        .await;

        let num_records = logger.len();
        if num_records != 1 {
            println!();
            println!("{} LOG MESSAGES:", logger.len());
            while let Some(record) = logger.pop() {
                println!("[{}] {}", record.level(), record.args());
            }
            println!();
            assert!(false, "Expected 1 log message, got {}", num_records);
        }

        let record = logger.pop().unwrap();
        assert_eq!(record.level(), Level::Warn);
        assert_eq!(
            record.args(),
            "All received MockItems for MockSource were new, try a shorter sleep_time",
        );
    }

    #[tokio::test]
    async fn test_sink_error_when_sending_new_item() {
        let _lock = LOCK.read().await;
        let mut mock_puller = MockPuller::new(vec![vec!["a"]]);
        let (sink, stream) = mpsc::unbounded();
        drop(stream); // drop receiver so that sending fails
        let join_handle = tokio::spawn(async move {
            pull_into_sink(&mut mock_puller, Duration::from_millis(1), vec![], sink).await
        });
        let result = join_handle.await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sink_error_when_sending_error() {
        let _lock = LOCK.read().await;
        let mut mock_puller = MockPuller::new(vec![vec!["error"]]);
        let (sink, stream) = mpsc::unbounded();
        drop(stream); // drop receiver so that sending fails
        let join_handle = tokio::spawn(async move {
            pull_into_sink(&mut mock_puller, Duration::from_millis(1), vec![], sink).await
        });
        let result = join_handle.await.unwrap();
        assert!(result.is_err());
    }
}
