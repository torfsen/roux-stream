[![Build](https://github.com/torfsen/roux-stream/actions/workflows/build.yml/badge.svg)](https://github.com/torfsen/roux-stream/actions/workflows/build.yml) [![Crates.io](https://img.shields.io/crates/v/roux-stream)](https://crates.io/crates/roux-stream) [![docs.rs](https://img.shields.io/docsrs/roux-stream)](https://docs.rs/roux-stream) [![Crates.io](https://img.shields.io/crates/l/roux-stream)](https://github.com/torfsen/roux-stream/blob/main/LICENSE)

# `roux-stream`

A streaming API for the Rust Reddit client
[`roux`](https://github.com/halcyonnouveau/roux).

Reddit's API does not provide "firehose"-style streaming of new posts and
comments. Instead, the endpoints for retrieving the latest posts and comments
have to be polled regularly. This crate automates that task and provides streams
for a subreddit's posts (submissions) and comments.


## Documentation

The documentation is available [on `docs.rs`](https://docs.rs/roux-stream) and
contains examples for [streaming submissions](https://docs.rs/roux-stream/latest/roux_stream/fn.stream_submissions.html#example)
and [streaming comments](https://docs.rs/roux-stream/latest/roux_stream/fn.stream_comments.html#example).

An example for streaming both submissions and comments at the same time can be
found in the file [`examples/submissions-and-comments.rs`](https://github.com/torfsen/roux-stream/blob/main/examples/submissions-and-comments.rs).


## License

Distributed under the MIT license. See the file [`LICENSE`](LICENSE) for
details.


## Changelog

See the file [`CHANGELOG.md`](CHANGELOG.md).


## Contributors

* [@torfsen](https://github.com/torfsen)
* [@fourbytes](https://github.com/fourbytes)
* [@dark0dave](https://github.com/dark0dave)
* [@asad-awadia](https://github.com/asad-awadia)


## Development

Clone the repository:

```bash
git clone https://github.com/torfsen/roux-stream.git
cd roux-stream
```

Run the tests:

```bash
cargo test
```

Run the example in `examples/submissions-and-comments.rs`:

```bash
cargo run --example submissions-and-comments
```
