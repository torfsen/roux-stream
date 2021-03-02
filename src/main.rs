use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use roux::Subreddit;
use roux::subreddit::responses::SubmissionsData;
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
        self.url == other.url
    }
}

impl Eq for Post {}

impl Hash for Post {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.url.hash(hasher);
    }
}


#[tokio::main]
async fn main() {
    let subreddit = Subreddit::new("AskReddit");

    /*
     * Reddit's API does not offer a "firehose" style stream of new
     * items, so we need to build that ourselves.
     *
     * The idea is to repeatedly get the latest items and remove those
     * that we've already seen.
     */

    let mut seen_posts: HashSet<Post> = HashSet::new();

    loop {
        println!("\nGetting latest posts...\n");
        let latest_posts: HashSet<Post> = subreddit
            .latest(25, None)
            .await
            .unwrap()
            .data
            .children.into_iter()
            .map(|thing| Post(thing.data))
            .collect();

        let new_posts: HashSet<_> = latest_posts
            .difference(&seen_posts)
            .collect();
        println!("{} new posts:", new_posts.len());
        for post in new_posts {
            println!("  {}", post.title);
        }

        seen_posts = latest_posts;

        // TODO: Adjust sleep duration based on number of new posts
        sleep(Duration::from_secs(5)).await;
    }
}
