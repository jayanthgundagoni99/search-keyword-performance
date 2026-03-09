#!/usr/bin/env bash
# Deploy the Search Keyword Performance CDK stack.
# Run from the repository root.
#
# Usage:
#   ./scripts/deploy-search-keyword.sh                          # deploy (dev, no termination protection)
#   ./scripts/deploy-search-keyword.sh --prod                   # deploy with termination protection
#   ./scripts/deploy-search-keyword.sh --image-tag abc123       # deploy with specific image tag
#   ./scripts/deploy-search-keyword.sh --dry-run                # synth only (no deploy)
set -euo pipefail

cd "$(dirname "$0")/../infra"

if [ ! -d "node_modules" ]; then
    echo "Installing CDK dependencies..."
    npm install
fi

IMAGE_TAG="latest"
DRY_RUN=false
PROD=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)   DRY_RUN=true; shift ;;
        --prod)      PROD=true; shift ;;
        --image-tag) IMAGE_TAG="$2"; shift 2 ;;
        *)           echo "Unknown option: $1"; exit 1 ;;
    esac
done

CDK_ARGS="--parameters ImageTag=$IMAGE_TAG"
if $PROD; then
    CDK_ARGS="$CDK_ARGS --context prod=true"
fi

if $DRY_RUN; then
    echo "Synthesizing CloudFormation template (dry run)..."
    npx cdk synth $CDK_ARGS
else
    MODE="dev"
    if $PROD; then MODE="PRODUCTION (termination protection ON)"; fi
    echo "Deploying SearchKeywordPerformanceStack [$MODE] (image tag: $IMAGE_TAG)..."
    npx cdk deploy --require-approval never $CDK_ARGS
fi
