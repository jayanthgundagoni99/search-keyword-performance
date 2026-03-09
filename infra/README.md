# Search Keyword Performance -- Infrastructure

## Overview

This AWS CDK project (TypeScript) provisions the cloud infrastructure for the **Search Keyword Performance Attribution Engine**. It deploys a two-tier processing architecture:

### Tier 1: Lambda (Small Files)

- **S3 Bucket** -- stores input data (`input/` prefix) and processed output (`output/` prefix)
- **Lambda Function** -- Python 3.12, triggered by `s3:ObjectCreated:*` on `input/`, 15-min timeout, 512 MB

### Tier 2: Batch + Fargate (Large Files)

- **ECR Repository** -- container image for the batch processor
- **Fargate Compute Environment** -- up to 16 vCPUs
- **Job Queue** -- `search-keyword-performance`
- **Job Definition** -- 2 vCPU, 8 GB memory, runs `batch_handler.py`

All resources are tagged with `Project: Search-Keyword-Performance`.

## Stack Outputs

| Output | Description |
|---|---|
| `BucketName` | S3 data bucket name |
| `LambdaFunctionArn` | Lambda function ARN |
| `ECRRepositoryUri` | ECR repository URI for Docker image |
| `BatchJobQueueArn` | Batch job queue ARN |
| `BatchJobDefinitionArn` | Batch job definition ARN |

## Prerequisites

- [AWS CLI](https://aws.amazon.com/cli/) configured with credentials
- [Node.js](https://nodejs.org/) 18+ and npm
- [AWS CDK Toolkit](https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html#getting_started_install) v2.x

## Deployment

```bash
cd infra
npm install
npx cdk deploy --require-approval never
```

Or from the repo root:

```bash
make deploy
```

## Synthesize (Dry Run)

```bash
npx cdk synth
```

## Destroy

```bash
npx cdk destroy --force
```

## Tests

```bash
npm test
```

Asserts the stack creates S3 bucket, Lambda, ECR repository, Batch compute environment, job queue, and job definition.
