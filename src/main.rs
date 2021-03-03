use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use roux::Subreddit;
use roux::subreddit::responses::{
    SubmissionsData,
    comments::SubredditCommentsData,
};
use tokio;
use tokio::time::{sleep, Duration};



/*
 * SubmissionsData does not implement Hash, so we wrap it into a custom
 * type that automatially derefs into SubmissionsData.
 */

struct Post(SubmissionsData);

impl Deref for Post {
    type Target = SubmissionsData;

    fn deref(&self) -> &SubmissionsData {
        &self.0
    }
}

impl PartialEq for Post {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Post {}

impl Hash for Post {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.id.hash(hasher);
    }
}

/*
 * SubredditCommentsData does not implement Hash, so we wrap it into a
 * custom type that automatially derefs into SubredditCommentsData.
 */

struct Comment(SubredditCommentsData);

impl Deref for Comment {
    type Target = SubredditCommentsData;

    fn deref(&self) -> &SubredditCommentsData {
        &self.0
    }
}

impl PartialEq for Comment {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Comment {}

impl Hash for Comment {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.id.hash(hasher);
    }
}

trait Listener {
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
async fn stream_subreddit_posts(
    subreddit: &Subreddit,
    listener: &dyn Listener,
) {
    let mut seen_posts: HashSet<Post> = HashSet::new();

    loop {
        let latest_posts: HashSet<Post> = subreddit
            .latest(25, None)
            .await
            .unwrap()
            .data
            .children
            .into_iter()
            .map(|thing| Post(thing.data))
            .collect();

        let new_posts: HashSet<_> = latest_posts
            .difference(&seen_posts)
            .collect();
        for post in new_posts {
            listener.new_post(subreddit, post);
        }

        seen_posts = latest_posts;

        // TODO: Adjust sleep duration based on number of new posts
        sleep(Duration::from_secs(5)).await;
    }
}


async fn stream_subreddit_comments(
    subreddit: &Subreddit,
    listener: &dyn Listener,
) {
    let mut seen_comments: HashSet<Comment> = HashSet::new();

    loop {
        let latest_comments: HashSet<Comment> = subreddit
            .latest_comments(None, None)
            .await
            .unwrap()
            .data
            .children
            .into_iter()
            .map(|thing| Comment(thing.data))
            .collect();

        let new_comments: HashSet<_> = latest_comments
            .difference(&seen_comments)
            .collect();

        for comment in new_comments {
            listener.new_comment(subreddit, comment);
        }

        seen_comments = latest_comments;

        // TODO: Adjust sleep duration based on number of new comments
        sleep(Duration::from_secs(5)).await;
    }
}


struct MyListener {
}

impl Listener for MyListener {
    fn new_post(&self, _subreddit: &Subreddit, data: &SubmissionsData) {
        println!("New post by {}", data.author);
    }

    fn new_comment(&self, _subreddit: &Subreddit, data: &SubredditCommentsData) {
        println!("New comment by {}", data.author.as_ref().unwrap());
    }
}


#[tokio::main]
async fn main() {
    let subreddit = Subreddit::new("AskReddit");
    let listener = MyListener {};

    tokio::join!(
        stream_subreddit_posts(&subreddit, &listener),
        stream_subreddit_comments(&subreddit, &listener),
    );
}
