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
