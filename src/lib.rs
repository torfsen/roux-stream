use std::collections::HashSet;
use std::marker::Unpin;
use futures::{Sink, SinkExt};

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
) {
    let mut seen_ids: HashSet<String> = HashSet::new();

    loop {
        let latest_posts = subreddit
            .latest(25, None)
            .await
            .unwrap()
            .data
            .children;

        let mut latest_ids: HashSet<String> = HashSet::new();

        for post in latest_posts {
            let data = post.data;
            latest_ids.insert(data.id.clone());
            if !seen_ids.contains(&data.id) {
                sink.send(data).await.unwrap_or_else(|_| panic!("Send failed"));
            }
        }

        seen_ids = latest_ids;

        // TODO: Adjust sleep duration based on number of new posts
        sleep(Duration::from_secs(5)).await;
    }
}


pub async fn stream_subreddit_comments(
    subreddit: &Subreddit,
    mut sink: impl Sink<SubredditCommentsData> + Unpin,
) {
    let mut seen_ids: HashSet<String> = HashSet::new();

    loop {
        let latest_comments = subreddit
            .latest_comments(None, None)
            .await
            .unwrap()
            .data
            .children;

        let mut latest_ids: HashSet<String> = HashSet::new();

        for comment in latest_comments {
            let data = comment.data;
            let id = data.id.as_ref().cloned().unwrap();
            latest_ids.insert(id.clone());
            if !seen_ids.contains(&id) {
                sink.send(data).await.unwrap_or_else(|_| panic!("Send failed"));
            }
        }

        seen_ids = latest_ids;

        // TODO: Adjust sleep duration based on number of new comments
        sleep(Duration::from_secs(5)).await;
    }
}
