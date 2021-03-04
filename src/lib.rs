use std::collections::HashSet;

use roux::Subreddit;
use roux::subreddit::responses::{
    SubmissionsData,
    comments::SubredditCommentsData,
};
use tokio::time::{sleep, Duration};


pub trait Listener {
    #[allow(unused_variables)]
    fn new_post(&self, subreddit: &Subreddit, data: &SubmissionsData) {
        // Default implementation does nothing
    }

    #[allow(unused_variables)]
    fn new_comment(&self, subreddit: &Subreddit, data: &SubredditCommentsData) {
        // Default implementation does nothing
    }
}


/*
 * Reddit's API does not offer a "firehose" style stream of new items,
 * so we need to build that ourselves. The idea is to repeatedly get the
 * latest items and remove those that we've already seen.
 */
pub async fn stream_subreddit_posts(
    subreddit: &Subreddit,
    listener: &dyn Listener,
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
                listener.new_post(subreddit, &data);
            }
        }

        seen_ids = latest_ids;

        // TODO: Adjust sleep duration based on number of new posts
        sleep(Duration::from_secs(5)).await;
    }
}


pub async fn stream_subreddit_comments(
    subreddit: &Subreddit,
    listener: &dyn Listener,
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
                listener.new_comment(subreddit, &data);
            }
        }

        seen_ids = latest_ids;

        // TODO: Adjust sleep duration based on number of new comments
        sleep(Duration::from_secs(5)).await;
    }
}
