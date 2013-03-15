#!/usr/bin/python
import boto.ec2

# AWS credentials
aws_key = 'AKIAI52ERUQP3UL5K2QA' 
aws_secret = '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'


# Instances that we want to stop
instances = [ 'i-b6dfb1fc', 'i-6eddb324', 'i-3adbb570' ]

# Connect to EC2 
conn = boto.ec2.connect_to_region("eu-west-1", aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)

# Start the instances
conn.start_instances( instance_ids=instances )