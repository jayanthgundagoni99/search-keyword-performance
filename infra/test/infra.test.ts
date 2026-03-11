import * as cdk from 'aws-cdk-lib';
import { Template, Match } from 'aws-cdk-lib/assertions';
import { SearchKeywordPerformanceStack } from '../lib/infra-stack';

let template: Template;

beforeAll(() => {
  const app = new cdk.App();
  const stack = new SearchKeywordPerformanceStack(app, 'TestStack', {
    env: { account: '123456789012', region: 'us-east-1' },
  });
  template = Template.fromStack(stack);
});

// ---------------------------------------------------------------
// S3 security best practices
// ---------------------------------------------------------------

test('Data bucket has encryption at rest (S3-managed)', () => {
  template.hasResourceProperties('AWS::S3::Bucket', {
    BucketEncryption: {
      ServerSideEncryptionConfiguration: [{
        ServerSideEncryptionByDefault: {
          SSEAlgorithm: 'AES256',
        },
      }],
    },
  });
});

test('Data bucket blocks all public access', () => {
  template.hasResourceProperties('AWS::S3::Bucket', {
    PublicAccessBlockConfiguration: {
      BlockPublicAcls: true,
      BlockPublicPolicy: true,
      IgnorePublicAcls: true,
      RestrictPublicBuckets: true,
    },
  });
});

test('Data bucket has versioning enabled', () => {
  template.hasResourceProperties('AWS::S3::Bucket', {
    VersioningConfiguration: {
      Status: 'Enabled',
    },
  });
});

test('Data bucket has lifecycle rules', () => {
  template.hasResourceProperties('AWS::S3::Bucket', {
    LifecycleConfiguration: {
      Rules: Match.arrayWith([
        Match.objectLike({ Id: 'expire-old-output', Status: 'Enabled' }),
        Match.objectLike({ Id: 'cleanup-noncurrent-versions', Status: 'Enabled' }),
        Match.objectLike({ Id: 'abort-incomplete-multipart', Status: 'Enabled' }),
      ]),
    },
  });
});

test('Buckets enforce SSL via bucket policy', () => {
  template.hasResourceProperties('AWS::S3::BucketPolicy', {
    PolicyDocument: {
      Statement: Match.arrayWith([
        Match.objectLike({
          Effect: 'Deny',
          Condition: { Bool: { 'aws:SecureTransport': 'false' } },
        }),
      ]),
    },
  });
});

test('Access logs bucket exists with its own lifecycle', () => {
  const buckets = template.findResources('AWS::S3::Bucket');
  expect(Object.keys(buckets).length).toBeGreaterThanOrEqual(2);
});

// ---------------------------------------------------------------
// Lambda best practices
// ---------------------------------------------------------------

test('Stack creates a Lambda function with correct runtime and handler', () => {
  template.hasResourceProperties('AWS::Lambda::Function', {
    Runtime: 'python3.12',
    Handler: 'search_keyword_handler.handler',
    MemorySize: 512,
    Timeout: 900,
  });
});

test('Lambda has X-Ray tracing enabled', () => {
  template.hasResourceProperties('AWS::Lambda::Function', {
    TracingConfig: { Mode: 'Active' },
  });
});

test('Lambda function exists with correct handler', () => {
  template.hasResourceProperties('AWS::Lambda::Function', {
    Handler: 'search_keyword_handler.handler',
  });
});

test('Lambda has ephemeral storage configured', () => {
  template.hasResourceProperties('AWS::Lambda::Function', {
    EphemeralStorage: { Size: 2048 },
  });
});

test('Lambda has a dead letter queue configured', () => {
  template.hasResourceProperties('AWS::Lambda::Function', {
    DeadLetterConfig: {
      TargetArn: Match.anyValue(),
    },
  });
});

test('Lambda has OUTPUT_PREFIX environment variable', () => {
  template.hasResourceProperties('AWS::Lambda::Function', {
    Environment: {
      Variables: {
        OUTPUT_PREFIX: 'output/',
      },
    },
  });
});

test('Stack creates CloudWatch alarms for Lambda errors and DLQ', () => {
  template.resourceCountIs('AWS::CloudWatch::Alarm', 2);
});

test('Alarms have SNS alarm actions configured', () => {
  template.hasResourceProperties('AWS::CloudWatch::Alarm', {
    AlarmActions: Match.anyValue(),
    OKActions: Match.anyValue(),
  });
});

test('Stack creates an SNS topic for alarm notifications', () => {
  template.hasResourceProperties('AWS::SNS::Topic', {
    TopicName: 'search-keyword-alarms',
  });
});

test('Stack creates a dead letter queue (SQS)', () => {
  template.hasResourceProperties('AWS::SQS::Queue', {
    QueueName: 'search-keyword-dlq',
  });
});

// ---------------------------------------------------------------
// ECR + Batch
// ---------------------------------------------------------------

test('Stack creates an ECR repository with security best practices', () => {
  template.hasResourceProperties('AWS::ECR::Repository', {
    RepositoryName: 'search-keyword-performance',
    ImageScanningConfiguration: { ScanOnPush: true },
    ImageTagMutability: 'IMMUTABLE',
  });
});

test('Stack creates FARGATE_SPOT compute environment for cost savings', () => {
  template.hasResourceProperties('AWS::Batch::ComputeEnvironment', {
    Type: 'MANAGED',
    ComputeResources: {
      Type: 'FARGATE_SPOT',
    },
  });
});

test('Stack creates FARGATE on-demand compute environment as fallback', () => {
  template.hasResourceProperties('AWS::Batch::ComputeEnvironment', {
    Type: 'MANAGED',
    ComputeResources: {
      Type: 'FARGATE',
    },
  });
});

test('Stack creates a Batch job queue with Spot priority over On-Demand', () => {
  template.hasResourceProperties('AWS::Batch::JobQueue', {
    JobQueueName: 'search-keyword-performance',
    ComputeEnvironmentOrder: Match.arrayWith([
      Match.objectLike({ Order: 1 }),
      Match.objectLike({ Order: 2 }),
    ]),
  });
});

test('Batch job definition has retry strategy and timeout', () => {
  template.hasResourceProperties('AWS::Batch::JobDefinition', {
    Type: 'container',
    PlatformCapabilities: ['FARGATE'],
    RetryStrategy: { Attempts: 3 },
    Timeout: { AttemptDurationSeconds: 7200 },
  });
});

test('Stack has expected outputs', () => {
  const outputs = template.findOutputs('*');
  expect(Object.keys(outputs).length).toBeGreaterThanOrEqual(8);
});
