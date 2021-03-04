use roux::Subreddit;
use roux::subreddit::responses::{
    SubmissionsData,
    comments::SubredditCommentsData,
};
use tokio;

use subreddit_dumper;
use subreddit_dumper::Listener;

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
        subreddit_dumper::stream_subreddit_posts(&subreddit, &listener),
        subreddit_dumper::stream_subreddit_comments(&subreddit, &listener),
    );
}
