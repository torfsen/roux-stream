use futures::{Sink, SinkExt};
use log::{debug, warn};
use roux::subreddit::responses::{comments::SubredditCommentsData, SubmissionsData};
use roux::{util::RouxError, Subreddit};
use std::collections::HashSet;
use std::marker::Unpin;
use tokio::time::{sleep, Duration};
use tokio_retry::Retry;

#[derive(Debug)]
pub enum SubmissionStreamError<S>
where
    S: Sink<SubmissionsData> + Unpin,
{
    Roux(RouxError),
    Sink(S::Error),
}

pub async fn stream_subreddit_submissions<S, R, I>(
    subreddit: &Subreddit,
    mut sink: S,
    sleep_time: Duration,
    retry_strategy: &R,
) -> Result<(), SubmissionStreamError<S>>
where
    S: Sink<SubmissionsData> + Unpin,
    R: IntoIterator<IntoIter = I, Item = Duration> + Clone,
    I: Iterator<Item = Duration>,
{
    // How many submissions to fetch per request
    const LIMIT: u32 = 100;
    let mut seen_ids: HashSet<String> = HashSet::new();

    loop {
        let latest_submissions =
            Retry::spawn(retry_strategy.clone(), || subreddit.latest(LIMIT, None))
                .await
                .map_err(SubmissionStreamError::Roux)?
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
                sink.send(submission)
                    .await
                    .map_err(SubmissionStreamError::Sink)?
            }
        }

        debug!("Got {} new items (out of {})", num_new, LIMIT);
        if num_new == LIMIT && !seen_ids.is_empty() {
            warn!("All received items were new, try a shorter sleep_time");
        }

        seen_ids = latest_ids;
        sleep(sleep_time).await;
    }
}

#[derive(Debug)]
pub enum CommentStreamError<S>
where
    S: Sink<SubredditCommentsData> + Unpin,
{
    Roux(RouxError),
    Sink(S::Error),
}

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

        debug!("Got {} new items (out of {})", num_new, LIMIT);
        if num_new == LIMIT && !seen_ids.is_empty() {
            warn!("All received items were new, try a shorter sleep_time");
        }

        seen_ids = latest_ids;
        sleep(sleep_time).await;
    }
}
