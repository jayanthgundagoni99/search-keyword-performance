.PHONY: run test deploy synth destroy docker-build clean help

INPUT ?= data/data.sql

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

run: ## Run the attribution locally (INPUT=data/data.sql)
	cd code && python -m search_keyword_performance ../$(INPUT)

test: ## Run the full pytest suite
	python -m pytest tests/ -v

deploy: ## Deploy the CDK stack to AWS (Lambda + Batch + Fargate)
	bash scripts/deploy-search-keyword.sh

synth: ## Synthesize the CloudFormation template (dry run)
	bash scripts/deploy-search-keyword.sh --dry-run

destroy: ## Destroy the CDK stack
	cd infra && npx cdk destroy --force

docker-build: ## Build the Docker image for Batch/Fargate
	docker build -t search-keyword-performance .

clean: ## Remove generated output files
	rm -f *_SearchKeywordPerformance.tab
	rm -f code/*_SearchKeywordPerformance.tab
