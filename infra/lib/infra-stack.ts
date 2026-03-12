import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as batch from 'aws-cdk-lib/aws-batch';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as cwActions from 'aws-cdk-lib/aws-cloudwatch-actions';
import { S3EventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as path from 'path';

const LIFECYCLE_EXPIRE_DAYS = 90;
const NONCURRENT_VERSION_EXPIRE_DAYS = 30;
const MULTIPART_ABORT_DAYS = 7;
const DLQ_RETENTION_DAYS = 14;
const LAMBDA_MEMORY_MB = 512;
const LAMBDA_EPHEMERAL_STORAGE_GB = 2;
// Reserved concurrency omitted for free-tier compatibility
const LAMBDA_RETRY_ATTEMPTS = 2;
const BATCH_MAX_VCPUS = 16;
const BATCH_JOB_VCPU = '2';
const BATCH_JOB_MEMORY_MB = '8192';
const BATCH_RETRY_ATTEMPTS = 3;
const BATCH_TIMEOUT_SECONDS = 7200;
const ECR_MAX_IMAGE_COUNT = 5;
const ALARM_PERIOD_MINUTES = 5;
const SESSION_TIMEOUT_SECONDS = '1800';

export class SearchKeywordPerformanceStack extends cdk.Stack {
  public readonly bucket: s3.Bucket;
  public readonly accessLogsBucket: s3.Bucket;
  public readonly handler: lambda.Function;
  public readonly deadLetterQueue: sqs.Queue;
  public readonly ecrRepository: ecr.Repository;
  public readonly jobQueue: batch.CfnJobQueue;
  public readonly jobDefinition: batch.CfnJobDefinition;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    cdk.Tags.of(this).add('Project', 'Search-Keyword-Performance');

    const imageTag = new cdk.CfnParameter(this, 'ImageTag', {
      type: 'String',
      default: 'latest',
      description: 'Docker image tag for the Batch job container (e.g. git SHA).',
    });

    // S3 buckets for data storage and access logging
    this.accessLogsBucket = new s3.Bucket(this, 'AccessLogsBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      lifecycleRules: [{
        expiration: cdk.Duration.days(LIFECYCLE_EXPIRE_DAYS),
      }],
    });

    this.bucket = new s3.Bucket(this, 'SearchKeywordDataBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      versioned: true,
      serverAccessLogsBucket: this.accessLogsBucket,
      serverAccessLogsPrefix: 'data-bucket-logs/',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      lifecycleRules: [
        {
          id: 'expire-old-output',
          prefix: 'output/',
          expiration: cdk.Duration.days(LIFECYCLE_EXPIRE_DAYS),
        },
        {
          id: 'cleanup-noncurrent-versions',
          noncurrentVersionExpiration: cdk.Duration.days(NONCURRENT_VERSION_EXPIRE_DAYS),
        },
        {
          id: 'abort-incomplete-multipart',
          abortIncompleteMultipartUploadAfter: cdk.Duration.days(MULTIPART_ABORT_DAYS),
        },
      ],
    });

    // Dead letter queue for failed Lambda invocations
    this.deadLetterQueue = new sqs.Queue(this, 'DeadLetterQueue', {
      queueName: 'search-keyword-dlq',
      retentionPeriod: cdk.Duration.days(DLQ_RETENTION_DAYS),
      enforceSSL: true,
    });

    // Lambda function for processing small files (< 2 GB, < 15 min)
    const lambdaLogGroup = new logs.LogGroup(this, 'LambdaLogGroup', {
      logGroupName: '/aws/lambda/search-keyword-performance',
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.handler = new lambda.Function(this, 'SearchKeywordFunction', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'search_keyword_handler.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../../code')),
      timeout: cdk.Duration.minutes(15),
      memorySize: LAMBDA_MEMORY_MB,
      ephemeralStorageSize: cdk.Size.gibibytes(LAMBDA_EPHEMERAL_STORAGE_GB),
      tracing: lambda.Tracing.ACTIVE,
      // reservedConcurrentExecutions omitted for free-tier compatibility
      retryAttempts: LAMBDA_RETRY_ATTEMPTS,
      deadLetterQueue: this.deadLetterQueue,
      logGroup: lambdaLogGroup,
      environment: {
        OUTPUT_PREFIX: 'output/',
        LOG_LEVEL: 'INFO',
      },
    });

    this.bucket.grantReadWrite(this.handler);

    this.handler.addEventSource(new S3EventSource(this.bucket, {
      events: [s3.EventType.OBJECT_CREATED],
      filters: [{ prefix: 'input/' }],
    }));

    // CloudWatch alarms with SNS notifications
    const alarmTopic = new sns.Topic(this, 'AlarmTopic', {
      topicName: 'search-keyword-alarms',
      displayName: 'Search Keyword Performance Alarms',
    });

    const lambdaErrorAlarm = new cloudwatch.Alarm(this, 'LambdaErrorAlarm', {
      alarmName: 'search-keyword-lambda-errors',
      alarmDescription: 'Triggered when Lambda error count exceeds threshold',
      metric: this.handler.metricErrors({ period: cdk.Duration.minutes(ALARM_PERIOD_MINUTES) }),
      threshold: 1,
      evaluationPeriods: 1,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
    lambdaErrorAlarm.addAlarmAction(new cwActions.SnsAction(alarmTopic));
    lambdaErrorAlarm.addOkAction(new cwActions.SnsAction(alarmTopic));

    const dlqAlarm = new cloudwatch.Alarm(this, 'DLQMessagesAlarm', {
      alarmName: 'search-keyword-dlq-messages',
      alarmDescription: 'Triggered when messages land in the dead letter queue',
      metric: this.deadLetterQueue.metricApproximateNumberOfMessagesVisible({
        period: cdk.Duration.minutes(ALARM_PERIOD_MINUTES),
      }),
      threshold: 1,
      evaluationPeriods: 1,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
    dlqAlarm.addAlarmAction(new cwActions.SnsAction(alarmTopic));
    dlqAlarm.addOkAction(new cwActions.SnsAction(alarmTopic));

    // ECR repository for Batch container images
    this.ecrRepository = new ecr.Repository(this, 'SearchKeywordECR', {
      repositoryName: 'search-keyword-performance',
      imageScanOnPush: true,
      imageTagMutability: ecr.TagMutability.IMMUTABLE,
      encryption: ecr.RepositoryEncryption.AES_256,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      emptyOnDelete: true,
      lifecycleRules: [{ maxImageCount: ECR_MAX_IMAGE_COUNT }],
    });

    // Batch on Fargate for large-file processing (Spot primary, on-demand fallback)
    const vpc = ec2.Vpc.fromLookup(this, 'DefaultVpc', { isDefault: true });

    const batchSecurityGroup = new ec2.SecurityGroup(this, 'BatchSecurityGroup', {
      vpc,
      description: 'Security group for Batch Fargate tasks',
      allowAllOutbound: true,
    });

    const batchExecutionRole = new iam.Role(this, 'BatchExecutionRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          'service-role/AmazonECSTaskExecutionRolePolicy'
        ),
      ],
    });

    const batchJobRole = new iam.Role(this, 'BatchJobRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
    });
    this.bucket.grantReadWrite(batchJobRole);

    const batchLogGroup = new logs.LogGroup(this, 'BatchLogGroup', {
      logGroupName: '/aws/batch/search-keyword-performance',
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const spotComputeEnv = new batch.CfnComputeEnvironment(this, 'FargateSpotComputeEnv', {
      type: 'MANAGED',
      computeResources: {
        type: 'FARGATE_SPOT',
        maxvCpus: BATCH_MAX_VCPUS,
        subnets: vpc.publicSubnets.map((s) => s.subnetId),
        securityGroupIds: [batchSecurityGroup.securityGroupId],
      },
    });

    const onDemandComputeEnv = new batch.CfnComputeEnvironment(this, 'FargateOnDemandComputeEnv', {
      type: 'MANAGED',
      computeResources: {
        type: 'FARGATE',
        maxvCpus: BATCH_MAX_VCPUS,
        subnets: vpc.publicSubnets.map((s) => s.subnetId),
        securityGroupIds: [batchSecurityGroup.securityGroupId],
      },
    });

    this.jobQueue = new batch.CfnJobQueue(this, 'SearchKeywordJobQueue', {
      jobQueueName: 'search-keyword-performance',
      priority: 1,
      computeEnvironmentOrder: [
        { computeEnvironment: spotComputeEnv.ref, order: 1 },
        { computeEnvironment: onDemandComputeEnv.ref, order: 2 },
      ],
    });

    this.jobDefinition = new batch.CfnJobDefinition(this, 'SearchKeywordJobDef', {
      jobDefinitionName: 'search-keyword-performance',
      type: 'container',
      platformCapabilities: ['FARGATE'],
      retryStrategy: { attempts: BATCH_RETRY_ATTEMPTS },
      timeout: { attemptDurationSeconds: BATCH_TIMEOUT_SECONDS },
      containerProperties: {
        image: `${this.ecrRepository.repositoryUri}:${imageTag.valueAsString}`,
        executionRoleArn: batchExecutionRole.roleArn,
        jobRoleArn: batchJobRole.roleArn,
        resourceRequirements: [
          { type: 'VCPU', value: BATCH_JOB_VCPU },
          { type: 'MEMORY', value: BATCH_JOB_MEMORY_MB },
        ],
        command: [
          'python', 'batch_handler.py',
        ],
        environment: [
          { name: 'OUTPUT_PREFIX', value: 'output/' },
          { name: 'SESSION_TIMEOUT', value: SESSION_TIMEOUT_SECONDS },
        ],
        logConfiguration: {
          logDriver: 'awslogs',
          options: {
            'awslogs-group': batchLogGroup.logGroupName,
            'awslogs-stream-prefix': 'batch',
          },
        },
        networkConfiguration: {
          assignPublicIp: 'ENABLED',
        },
      },
    });

    // Stack outputs
    new cdk.CfnOutput(this, 'BucketName', {
      value: this.bucket.bucketName,
      description: 'S3 bucket for input/output data files.',
    });

    // ---------------------------------------------------------------
    // Glue ETL job for large-scale (50+ GB) processing
    // ---------------------------------------------------------------

    void new s3deploy.BucketDeployment(this, 'GlueScriptDeployment', {
      sources: [s3deploy.Source.asset(path.join(__dirname, '..', '..', 'code', 'glue'))],
      destinationBucket: this.bucket,
      destinationKeyPrefix: 'scripts/',
    });

    const glueRole = new iam.Role(this, 'GlueJobRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
      ],
    });
    this.bucket.grantReadWrite(glueRole);

    void new glue.CfnJob(this, 'SearchKeywordGlueJob', {
      name: 'search-keyword-performance',
      role: glueRole.roleArn,
      command: {
        name: 'glueetl',
        scriptLocation: `s3://${this.bucket.bucketName}/scripts/search_keyword_glue.py`,
        pythonVersion: '3',
      },
      defaultArguments: {
        '--input_path': `s3://${this.bucket.bucketName}/input/`,
        '--output_path': `s3://${this.bucket.bucketName}/glue-output/`,
        '--session_timeout': '0',
        '--job-language': 'python',
      },
      glueVersion: '4.0',
      numberOfWorkers: 2,
      workerType: 'G.1X',
    });

    // ---------------------------------------------------------------
    // Stack outputs
    // ---------------------------------------------------------------

    new cdk.CfnOutput(this, 'GlueJobName', {
      value: 'search-keyword-performance',
      description: 'Name of the Glue ETL job for large-scale processing.',
    });

    new cdk.CfnOutput(this, 'AccessLogsBucketName', {
      value: this.accessLogsBucket.bucketName,
      description: 'S3 bucket for access logs.',
    });

    new cdk.CfnOutput(this, 'LambdaFunctionArn', {
      value: this.handler.functionArn,
      description: 'ARN of the Search Keyword Performance Lambda function.',
    });

    new cdk.CfnOutput(this, 'DeadLetterQueueUrl', {
      value: this.deadLetterQueue.queueUrl,
      description: 'SQS dead letter queue for failed Lambda invocations.',
    });

    new cdk.CfnOutput(this, 'AlarmTopicArn', {
      value: alarmTopic.topicArn,
      description: 'SNS topic for CloudWatch alarm notifications.',
    });

    new cdk.CfnOutput(this, 'ECRRepositoryUri', {
      value: this.ecrRepository.repositoryUri,
      description: 'ECR repository for the Batch container image.',
    });

    new cdk.CfnOutput(this, 'BatchJobQueueArn', {
      value: this.jobQueue.attrJobQueueArn,
      description: 'ARN of the Batch job queue for large-file processing.',
    });

    new cdk.CfnOutput(this, 'BatchJobDefinitionArn', {
      value: this.jobDefinition.ref,
      description: 'ARN of the Batch job definition.',
    });
  }
}
