.PHONY: build
build:
	docker compose build

.PHONY: dev
dev: build
	docker compose up --watch

.PHONY: clean
clean:
	docker compose down --remove-orphans --volumes --rmi=all
	cargo clean

# RUSTFLAGS as CI sets them in .github/workflows/verify-impl.yml. `-Dwarnings` is
# the part that matters: plain `cargo test` leaves warnings as warnings, so dead
# code, unused imports, and the like pass locally and fail the coverage job. Keep
# this in sync with the workflow.
CI_RUSTFLAGS := -Dwarnings -A mismatched_lifetime_syntaxes

# Same commands CI's coverage job runs. Writes lcov.info and an HTML report under
# coverage/. Needs cargo-llvm-cov: cargo install cargo-llvm-cov
.PHONY: coverage
coverage:
	RUSTFLAGS="$(CI_RUSTFLAGS)" cargo llvm-cov --workspace --lcov --output-path lcov.info --ignore-filename-regex 'proto/'
	RUSTFLAGS="$(CI_RUSTFLAGS)" cargo llvm-cov report --html --output-dir coverage --ignore-filename-regex 'proto/'
	@echo "HTML report: coverage/html/index.html"

# The fast subset of the above: builds and tests under CI's lint settings without
# the coverage instrumentation, for catching a -Dwarnings failure before pushing.
.PHONY: check-ci
check-ci:
	RUSTFLAGS="$(CI_RUSTFLAGS)" cargo check --workspace --all-targets
	RUSTFLAGS="$(CI_RUSTFLAGS)" cargo fmt --all --check

.PHONY: changelog
changelog:
	#SNAPCHAIN_VERSION=$(awk -F '"' '/^version =/ {print $2}' ./Cargo.toml)
	echo "Generating changelog for version: $(SNAPCHAIN_VERSION)"
	git cliff --unreleased --tag $(SNAPCHAIN_VERSION) --prepend CHANGELOG.md