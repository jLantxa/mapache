all:
	@make debug
	@make release
	@make fmt
	@make doc
	@make test
	@make lint

debug:
	@cargo build --all --all-targets

release:
	@cargo build --release --all --all-targets

doc:
	@cargo doc --no-deps --document-private-items

test:
	@cargo test

fmt:
	@cargo fmt

lint:
	@cargo clippy

clean:
	@cargo clean
