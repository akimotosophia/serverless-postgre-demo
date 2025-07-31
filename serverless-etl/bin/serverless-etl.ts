#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { ServerlessEtlStack } from '../lib/serverless-etl-stack';

const app = new cdk.App();
new ServerlessEtlStack(app, 'ServerlessEtlStack');