use futures::{Stream, StreamExt};
use roux::{
    subreddit::responses::{SubmissionsData, SubredditCommentsData},
    util::RouxError,
    Subreddit,
};
use tokio;
use tokio::time::Duration;
use tokio_retry::strategy::{jitter, ExponentialBackoff};

use roux_stream;

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

async fn comment_reader<S>(stream: &mut S) -> Result<(), RouxError>
where
    S: Stream<Item = Result<SubredditCommentsData, RouxError>> + Unpin,
{
    while let Some(comment) = stream.next().await {
        match comment {
            Ok(comment) => println!(
                "New comment in r/{} by {}",
                comment.subreddit.unwrap(),
                comment.author.unwrap()
            ),
            Err(error) => return Err(error),
        }
    }
    Ok(())
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

    let retry_strategy = ExponentialBackoff::from_millis(5)
        .factor(100)
        .map(jitter) // add jitter to delays
        .take(3); // limit to 3 retries

    let mut submissions_stream = roux_stream::stream_submissions(
        &subreddit,
        Duration::from_secs(60),
        retry_strategy.clone(),
    );
    let mut comments_stream = roux_stream::stream_comments(
        &subreddit,
        Duration::from_secs(10),
        retry_strategy.clone(),
    );

    let (submission_result, comment_result) = tokio::join!(
        submission_reader(&mut submissions_stream),
        comment_reader(&mut comments_stream),
    );
    submission_result.unwrap();
    comment_result.unwrap();
}
