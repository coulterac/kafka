.DEFAULT_GOAL := all

kafka:
	docker-compose -f docker-compose.yml up -d --build --abort-on-container-exit --force-recreate

.PHONY: all kafka