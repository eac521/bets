test:
	pytest tests/ -v -m "not integration"

test-all:
	pytest tests/ -v

test-integration:
	pytest tests/ -v -m integration