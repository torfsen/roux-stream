use std::collections::HashSet;
use std::marker::Unpin;
use futures::{Sink, SinkExt};
use log::{debug, warn};
use roux::Subreddit;
use roux::subreddit::responses::{
    SubmissionsData,
    comments::SubredditCommentsData,
};
use tokio::time::{sleep, Duration};


/*
 * Reddit's API does not offer a "firehose" style stream of new items,
 * so we need to build that ourselves. The idea is to repeatedly get the
 * latest items and remove those that we've already seen.
 */
pub async fn stream_subreddit_submissions(
    subreddit: &Subreddit,
    mut sink: impl Sink<SubmissionsData> + Unpin,
    sleep_time: Duration,
) {
    // How many submissions to fetch per request
    const LIMIT: u32 = 100;
    let mut seen_ids: HashSet<String> = HashSet::new();
    loop {
        // TODO: Retry on connection issues
        let latest_submissions = subreddit
            .latest(LIMIT, None)
            .await
            .unwrap()
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
                // TODO: Let this error bubble up
                sink.send(submission).await.unwrap_or_else(|_| panic!("Send failed"));
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


pub async fn stream_subreddit_comments(
    subreddit: &Subreddit,
    mut sink: impl Sink<SubredditCommentsData> + Unpin,
    sleep_time: Duration,
) {
    // How many comments to fetch per request
    const LIMIT: u32 = 100;
    let mut seen_ids: HashSet<String> = HashSet::new();
    loop {
        let latest_comments = subreddit
            .latest_comments(None, Some(LIMIT))
            .await
            .unwrap()
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
                sink.send(comment).await.unwrap_or_else(|_| panic!("Send failed"));
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