# `roux-stream`

A streaming API for the Rust Reddit client
[`roux`](https://github.com/halcyonnouveau/roux).

Reddit's API does not provide "firehose"-style streaming of new posts and
comments. Instead, the endpoints for retrieving the latest posts and comments
have to be polled regularly. This crate automates that task and provides streams
for a subreddit's posts (submissions) and comments.


## License

Distributed under the MIT license. See the file `LICENSE` for details.


## Changelog

See the file `CHANGELOG.md`.


# Development

Clone the repository:

```bash
git clone https://github.com/torfsen/roux-stream.git
cd roux-stream
```

Run the tests:

```bash
cargo test
```
