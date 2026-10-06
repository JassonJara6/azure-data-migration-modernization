.PHONY: source-up source-setup source-explore source-down source-reset validate

source-up:
	docker compose up -d sqlserver

source-setup:
	bash scripts/setup-source.sh

source-explore:
	bash scripts/explore-source.sh

source-down:
	docker compose down

source-reset:
	docker compose down --volumes
	rm -rf data artifacts

validate:
	docker compose config --quiet
	bash -n scripts/*.sh
