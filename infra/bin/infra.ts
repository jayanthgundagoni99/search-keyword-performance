#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { SearchKeywordPerformanceStack } from '../lib/infra-stack';

const app = new cdk.App();
const isProd = app.node.tryGetContext('prod') === 'true';

new SearchKeywordPerformanceStack(app, 'SearchKeywordPerformanceStack', {
  terminationProtection: isProd,
  // env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },
});
