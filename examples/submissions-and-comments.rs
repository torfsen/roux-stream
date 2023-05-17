/*
Copyright (c) 2021 Florian Brucker (www.florianbrucker.de)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
 */

/*
This example illustrates how to listen for both new submissions and new
comments at the same time. You can run it using

    cargo run --example submissions-and-comments
 */

use futures::{Stream, StreamExt};
use roux::{
    submission::SubmissionData,
    comment::CommentData,
    util::RouxError,
    Subreddit,
};
use tokio::time::Duration;
use tokio_retry::strategy::{jitter, ExponentialBackoff};

use roux_stream::StreamError;

async fn submission_reader<S>(stream: &mut S) -> Result<(), StreamError<RouxError>>
where
    S: Stream<Item = Result<SubmissionData, StreamError<RouxError>>> + Unpin,
{
    while let Some(submission) = stream.next().await {
        let submission = submission?;
        println!(
            "New submission in r/{} by {}",
            submission.subreddit, submission.author
        )
    }
    Ok(())
}

async fn comment_reader<S>(stream: &mut S) -> Result<(), StreamError<RouxError>>
where
    S: Stream<Item = Result<CommentData, StreamError<RouxError>>> + Unpin,
{
    while let Some(comment) = stream.next().await {
        let comment = comment?;
        println!(
            "New comment in r/{} by {}",
            comment.subreddit.unwrap(),
            comment.author.unwrap()
        )
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

    let retry_strategy = ExponentialBackoff::from_millis(5)
        .factor(100)
        .map(jitter) // add jitter to delays
        .take(3); // limit to 3 retries

    // Abort fetching new items after 10s
    let timeout = Duration::from_secs(10);

    let (mut submissions_stream, submissions_handle) = roux_stream::stream_submissions(
        &subreddit,
        Duration::from_secs(60),
        retry_strategy.clone(),
        Some(timeout.clone()),
    );
    let (mut comments_stream, comments_handle) = roux_stream::stream_comments(
        &subreddit,
        Duration::from_secs(10),
        retry_strategy.clone(),
        Some(timeout.clone()),
    );

    let (submission_result, comment_result) = tokio::join!(
        submission_reader(&mut submissions_stream),
        comment_reader(&mut comments_stream),
    );
    submission_result.unwrap();
    comment_result.unwrap();
    submissions_handle.await.unwrap().unwrap();
    comments_handle.await.unwrap().unwrap();
}
