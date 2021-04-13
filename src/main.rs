use futures::{Stream, StreamExt, channel::mpsc};
use roux::{Subreddit, subreddit::responses::{SubmissionsData, SubredditCommentsData}};
use tokio;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tokio::time::Duration;

use subreddit_dumper;


async fn submission_reader<S>(stream: &mut S)
where S: Stream<Item=SubmissionsData> + Unpin
{
    while let Some(submission) = stream.next().await {
        println!("New submission by {}", submission.author);
    }
}

async fn comment_reader<S>(stream: &mut S)
where S: Stream<Item=SubredditCommentsData> + Unpin
{
    while let Some(comment) = stream.next().await {
        println!("New comment by {}", comment.author.as_ref().unwrap());
    }
}

#[tokio::main]
async fn main() {
    // Initialize logging
    stderrlog::new()
        .module(module_path!())
        .verbosity(3)
        .init()
        .unwrap();

    let subreddit = Subreddit::new("AskReddit");

    let (mut submission_sender, mut submission_receiver) = mpsc::unbounded();
    let (mut comment_sender, mut comment_receiver) = mpsc::unbounded();

    let retry_strategy = ExponentialBackoff::from_millis(100)
        .map(jitter) // add jitter to delays
        .take(3);    // limit to 3 retries

    let (submission_res, _, comment_res, _) = tokio::join!(
        subreddit_dumper::stream_subreddit_submissions(
            &subreddit,
            &mut submission_sender,
            Duration::from_secs(60),
            &retry_strategy,
        ),
        submission_reader(&mut submission_receiver),
        subreddit_dumper::stream_subreddit_comments(
            &subreddit,
            &mut comment_sender,
            Duration::from_secs(15),
            &retry_strategy,
        ),
        comment_reader(&mut comment_receiver),
    );
    submission_res.unwrap();
    comment_res.unwrap();
}