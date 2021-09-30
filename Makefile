.DEFAULT_GOAL := all

kafka:
	docker-compose -f docker-compose.yml up --build --abort-on-container-exit --force-recreate

vendor:
	go mod vendor
	go mod tidy

.PHONY: all kafka vendor