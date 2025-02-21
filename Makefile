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

.PHONY: changelog
changelog:
	#SNAPCHAIN_VERSION=$(awk -F '"' '/^version =/ {print $2}' ./Cargo.toml)
	echo "Generating changelog for version: $(SNAPCHAIN_VERSION)"
	git cliff --unreleased --tag $(SNAPCHAIN_VERSION) --prepend CHANGELOG.md