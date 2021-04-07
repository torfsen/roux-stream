use std::collections::HashSet;
use std::marker::Unpin;
use futures::{Sink, SinkExt};
use log::{debug, warn};
use roux::{Subreddit, util::RouxError};
use roux::subreddit::responses::{
    SubmissionsData,
    comments::SubredditCommentsData,
};
use tokio::time::{sleep, Duration};


#[derive(Debug)]
pub enum SubmissionStreamError<S>
where S: Sink<SubmissionsData> + Unpin
{
    Roux(RouxError),
    Sink(S::Error),
}

pub async fn stream_subreddit_submissions<S>(
    subreddit: &Subreddit,
    mut sink: S,
    sleep_time: Duration,
) -> Result<(), SubmissionStreamError<S>>
where S: Sink<SubmissionsData> + Unpin
{
    // How many submissions to fetch per request
    const LIMIT: u32 = 100;
    let mut seen_ids: HashSet<String> = HashSet::new();
    loop {
        // TODO: Retry on connection issues
        let latest_submissions = subreddit
            .latest(LIMIT, None)
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
                sink.send(submission).await.map_err(SubmissionStreamError::Sink)?
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
where S: Sink<SubredditCommentsData> + Unpin
{
    Roux(RouxError),
    Sink(S::Error),
}

pub async fn stream_subreddit_comments<S>(
    subreddit: &Subreddit,
    mut sink: S,
    sleep_time: Duration,
) -> Result<(), CommentStreamError<S>>
where S: Sink<SubredditCommentsData> + Unpin
{
    // How many comments to fetch per request
    const LIMIT: u32 = 100;
    let mut seen_ids: HashSet<String> = HashSet::new();
    loop {
        let latest_comments = subreddit
            .latest_comments(None, Some(LIMIT))
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