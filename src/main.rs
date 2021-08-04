use std::io::Error;

use futures::{channel::mpsc, Stream, StreamExt};
use roux::{
    subreddit::responses::{SubmissionsData, SubredditCommentsData},
    util::RouxError,
    Subreddit,
};
use tokio;
use tokio::time::Duration;
use tokio_retry::strategy::{jitter, ExponentialBackoff};

use subreddit_dumper;

async fn submission_reader<S>(stream: &mut S) -> Result<(), RouxError>
where
    S: Stream<Item = Result<SubmissionsData, RouxError>> + Unpin,
{
    while let Some(submission) = stream.next().await {
        match submission {
            Ok(submission) => println!(
                "New submission in r/{} by {}",
                submission.subreddit, submission.author
            ),
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

async fn comment_reader<S>(stream: &mut S)
where
    S: Stream<Item = SubredditCommentsData> + Unpin,
{
    while let Some(comment) = stream.next().await {
        println!(
            "New comment in r/{} by {}",
            comment.subreddit.unwrap(),
            comment.author.unwrap()
        );
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

    //let (mut comment_sender, mut comment_receiver) = mpsc::unbounded();

    let retry_strategy = ExponentialBackoff::from_millis(100)
        .map(jitter) // add jitter to delays
        .take(3); // limit to 3 retries

    let mut submissions_stream = subreddit_dumper::stream_submissions(
        &subreddit,
        Duration::from_secs(10),
        retry_strategy,
    );

    let (submission_result /*comment_res , _*/,) = tokio::join!(
        submission_reader(&mut submissions_stream),
        /*
        subreddit_dumper::stream_subreddit_comments(
            &subreddit,
            &mut comment_sender,
            Duration::from_secs(15),
            &retry_strategy,
        ),
        comment_reader(&mut comment_receiver),
        */
    );
    submission_result.unwrap();
    //comment_res.unwrap();
}
