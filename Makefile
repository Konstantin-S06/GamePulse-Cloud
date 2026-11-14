RUN = docker compose exec -T runner
PY = /opt/gamepulse-venv/bin/python

.PHONY: init build up down shell test lint sample s3-smoke

init:
	@if [ ! -f .env ]; then cp .env.example .env; fi
	@uid=$$(id -u); \
	  if grep -q '^AIRFLOW_UID=' .env; then \
	    sed -i.bak "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$$uid/" .env && rm -f .env.bak; \
	  else \
	    echo "AIRFLOW_UID=$$uid" >> .env; \
	  fi
	@key=$$(openssl rand -base64 32 | tr '+/' '-_'); \
	  if grep -q '^AIRFLOW_FERNET_KEY=' .env; then \
	    sed -i.bak "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=$$key|" .env && rm -f .env.bak; \
	  else \
	    echo "AIRFLOW_FERNET_KEY=$$key" >> .env; \
	  fi

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

shell:
	docker compose exec runner bash

test:
	$(RUN) $(PY) -m pytest -m 'not integration' -q

lint:
	$(RUN) $(PY) -m ruff check src tests
	$(RUN) $(PY) -m ruff format --check src tests

sample:
	$(RUN) $(PY) -m gamepulse.simulator.cli \
	  --run-date 2026-01-05 --num-matches 20 \
	  --out data/sample/events.jsonl

s3-smoke:
	$(RUN) $(PY) -m gamepulse.ops.s3_smoke_test
