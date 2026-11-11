# k3rs on AWS - Production Deployment Guide

This guide provides comprehensive instructions for deploying k3rs on Amazon Web Services (AWS) for production use.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Infrastructure Setup](#infrastructure-setup)
- [Single-Node Deployment](#single-node-deployment)
- [High Availability Deployment](#high-availability-deployment)
- [Storage Configuration](#storage-configuration)
- [Networking Configuration](#networking-configuration)
- [Security Configuration](#security-configuration)
- [Monitoring and Logging](#monitoring-and-logging)
- [Backup and Disaster Recovery](#backup-and-disaster-recovery)
- [Scaling](#scaling)
- [Cost Optimization](#cost-optimization)
- [Troubleshooting](#troubleshooting)

## Architecture Overview

### Single-Node Architecture

```
┌─────────────────────────────────────┐
│         AWS VPC (10.0.0.0/16)       │
│                                     │
│  ┌───────────────────────────────┐ │
│  │  Public Subnet (10.0.1.0/24)  │ │
│  │                               │ │
│  │  ┌─────────────────────────┐  │ │
│  │  │   EC2 Instance          │  │ │
│  │  │   (t3.xlarge)           │  │ │
│  │  │                         │  │ │
│  │  │  - API Server :6443     │  │ │
│  │  │  - etcd :2379           │  │ │
│  │  │  - Scheduler            │  │ │
│  │  │  - Controller Manager   │  │ │
│  │  │  - Kubelet              │  │ │
│  │  │  - Kube-proxy           │  │ │
│  │  │  - CoreDNS              │  │ │
│  │  └─────────────────────────┘  │ │
│  │         │                     │ │
│  │    EBS Volume (gp3, 100GB)    │ │
│  └───────────────────────────────┘ │
│               │                     │
│       Elastic IP                    │
└─────────────────────────────────────┘
          │
    Internet Gateway
```

### High Availability Architecture

```
┌────────────────────────────────────────────────────────────┐
│              AWS VPC (10.0.0.0/16)                         │
│                                                            │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐     │
│  │    AZ-A      │ │    AZ-B      │ │    AZ-C      │     │
│  │  10.0.1.0/24 │ │  10.0.2.0/24 │ │  10.0.3.0/24 │     │
│  │              │ │              │ │              │     │
│  │  ┌────────┐  │ │  ┌────────┐  │ │  ┌────────┐  │     │
│  │  │ etcd-1 │  │ │  │ etcd-2 │  │ │  │ etcd-3 │  │     │
│  │  │ API-1  │  │ │  │ API-2  │  │ │  │ API-3  │  │     │
│  │  │ Ctl-1  │  │ │  │ Ctl-2  │  │ │  │ Ctl-3  │  │     │
│  │  │ Sch-1  │  │ │  │ Sch-2  │  │ │  │ Sch-3  │  │     │
│  │  └────────┘  │ │  └────────┘  │ │  └────────┘  │     │
│  │  EBS Volume  │ │  EBS Volume  │ │  EBS Volume  │     │
│  └──────────────┘ └──────────────┘ └──────────────┘     │
│         │                 │                 │             │
│         └─────────────────┴─────────────────┘             │
│                          │                                │
│              Application Load Balancer                    │
│                    (Port 6443)                            │
└────────────────────────────────────────────────────────────┘
                           │
                    Internet Gateway
```

## Prerequisites

### AWS Account Requirements

- Active AWS account with billing enabled
- IAM user with administrator access or specific permissions:
  - EC2: Full access
  - VPC: Full access
  - EBS: Full access
  - ELB: Full access (for HA setup)
  - Route53: Full access (for DNS)
  - CloudWatch: Full access (for monitoring)
  - S3: Full access (for backups)

### Local Tools

```bash
# Install AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Verify installation
aws --version

# Configure AWS credentials
aws configure
# Enter: Access Key ID, Secret Access Key, Region, Output format

# Verify credentials
aws sts get-caller-identity
```

### SSH Key Pair

```bash
# Generate SSH key pair (if you don't have one)
ssh-keygen -t rsa -b 4096 -f ~/.ssh/k3rs-aws -C "k3rs@aws"

# Import to AWS
aws ec2 import-key-pair \
  --key-name k3rs-keypair \
  --public-key-material fileb://~/.ssh/k3rs-aws.pub \
  --region us-east-1
```

## Infrastructure Setup

### 1. Create VPC

```bash
# Create VPC
VPC_ID=$(aws ec2 create-vpc \
  --cidr-block 10.0.0.0/16 \
  --tag-specifications 'ResourceType=vpc,Tags=[{Key=Name,Value=k3rs-vpc}]' \
  --query 'Vpc.VpcId' \
  --output text)

echo "VPC ID: $VPC_ID"

# Enable DNS hostnames
aws ec2 modify-vpc-attribute \
  --vpc-id $VPC_ID \
  --enable-dns-hostnames

# Enable DNS support
aws ec2 modify-vpc-attribute \
  --vpc-id $VPC_ID \
  --enable-dns-support
```

### 2. Create Internet Gateway

```bash
# Create Internet Gateway
IGW_ID=$(aws ec2 create-internet-gateway \
  --tag-specifications 'ResourceType=internet-gateway,Tags=[{Key=Name,Value=k3rs-igw}]' \
  --query 'InternetGateway.InternetGatewayId' \
  --output text)

echo "Internet Gateway ID: $IGW_ID"

# Attach to VPC
aws ec2 attach-internet-gateway \
  --internet-gateway-id $IGW_ID \
  --vpc-id $VPC_ID
```

### 3. Create Subnets

```bash
# Public subnet (for single-node or HA load balancer)
SUBNET_ID=$(aws ec2 create-subnet \
  --vpc-id $VPC_ID \
  --cidr-block 10.0.1.0/24 \
  --availability-zone us-east-1a \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=k3rs-public-1a}]' \
  --query 'Subnet.SubnetId' \
  --output text)

echo "Subnet ID: $SUBNET_ID"

# For HA: Create additional subnets in different AZs
SUBNET_ID_2=$(aws ec2 create-subnet \
  --vpc-id $VPC_ID \
  --cidr-block 10.0.2.0/24 \
  --availability-zone us-east-1b \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=k3rs-public-1b}]' \
  --query 'Subnet.SubnetId' \
  --output text)

SUBNET_ID_3=$(aws ec2 create-subnet \
  --vpc-id $VPC_ID \
  --cidr-block 10.0.3.0/24 \
  --availability-zone us-east-1c \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=k3rs-public-1c}]' \
  --query 'Subnet.SubnetId' \
  --output text)

# Enable auto-assign public IP
aws ec2 modify-subnet-attribute \
  --subnet-id $SUBNET_ID \
  --map-public-ip-on-launch

aws ec2 modify-subnet-attribute \
  --subnet-id $SUBNET_ID_2 \
  --map-public-ip-on-launch

aws ec2 modify-subnet-attribute \
  --subnet-id $SUBNET_ID_3 \
  --map-public-ip-on-launch
```

### 4. Create and Configure Route Table

```bash
# Get main route table
ROUTE_TABLE_ID=$(aws ec2 describe-route-tables \
  --filters "Name=vpc-id,Values=$VPC_ID" \
  --query 'RouteTables[0].RouteTableId' \
  --output text)

# Add route to Internet Gateway
aws ec2 create-route \
  --route-table-id $ROUTE_TABLE_ID \
  --destination-cidr-block 0.0.0.0/0 \
  --gateway-id $IGW_ID
```

### 5. Create Security Group

```bash
# Create security group
SG_ID=$(aws ec2 create-security-group \
  --group-name k3rs-sg \
  --description "Security group for k3rs cluster" \
  --vpc-id $VPC_ID \
  --tag-specifications 'ResourceType=security-group,Tags=[{Key=Name,Value=k3rs-sg}]' \
  --query 'GroupId' \
  --output text)

echo "Security Group ID: $SG_ID"

# Allow SSH
aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID \
  --protocol tcp \
  --port 22 \
  --cidr 0.0.0.0/0

# Allow Kubernetes API Server
aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID \
  --protocol tcp \
  --port 6443 \
  --cidr 0.0.0.0/0

# Allow etcd client
aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID \
  --protocol tcp \
  --port 2379-2380 \
  --cidr 10.0.0.0/16

# Allow all traffic within VPC (for pod-to-pod communication)
aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID \
  --protocol all \
  --source-group $SG_ID

# Allow NodePort services (optional)
aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID \
  --protocol tcp \
  --port 30000-32767 \
  --cidr 0.0.0.0/0
```

## Single-Node Deployment

### 1. Launch EC2 Instance

```bash
# Get latest Amazon Linux 2023 AMI ID
AMI_ID=$(aws ec2 describe-images \
  --owners amazon \
  --filters "Name=name,Values=al2023-ami-2023.*-x86_64" \
  --query 'sort_by(Images, &CreationDate)[-1].ImageId' \
  --output text)

echo "AMI ID: $AMI_ID"

# Create user data script for instance initialization
cat > user-data.sh <<'EOF'
#!/bin/bash
set -e

# Update system
dnf update -y

# Install Docker
dnf install -y docker git

# Start and enable Docker
systemctl start docker
systemctl enable docker

# Install Docker Compose
curl -SL https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source /root/.cargo/env

# Create k3rs user
useradd -m -s /bin/bash k3rs
usermod -aG docker k3rs

# Clone repository
cd /home/k3rs
git clone https://github.com/yourusername/k3rs.git
chown -R k3rs:k3rs k3rs

# Mark initialization complete
touch /var/log/k3rs-init-complete
EOF

# Launch instance
INSTANCE_ID=$(aws ec2 run-instances \
  --image-id $AMI_ID \
  --instance-type t3.xlarge \
  --key-name k3rs-keypair \
  --security-group-ids $SG_ID \
  --subnet-id $SUBNET_ID \
  --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":100,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
  --user-data file://user-data.sh \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=k3rs-node}]' \
  --query 'Instances[0].InstanceId' \
  --output text)

echo "Instance ID: $INSTANCE_ID"

# Wait for instance to be running
aws ec2 wait instance-running --instance-ids $INSTANCE_ID

# Get public IP
PUBLIC_IP=$(aws ec2 describe-instances \
  --instance-ids $INSTANCE_ID \
  --query 'Reservations[0].Instances[0].PublicIpAddress' \
  --output text)

echo "Instance is running at: $PUBLIC_IP"
```

### 2. Allocate Elastic IP (Optional, for static IP)

```bash
# Allocate Elastic IP
EIP_ALLOC=$(aws ec2 allocate-address \
  --domain vpc \
  --tag-specifications 'ResourceType=elastic-ip,Tags=[{Key=Name,Value=k3rs-eip}]' \
  --query 'AllocationId' \
  --output text)

# Associate with instance
aws ec2 associate-address \
  --instance-id $INSTANCE_ID \
  --allocation-id $EIP_ALLOC

# Get the Elastic IP
EIP=$(aws ec2 describe-addresses \
  --allocation-ids $EIP_ALLOC \
  --query 'Addresses[0].PublicIp' \
  --output text)

echo "Elastic IP: $EIP"
```

### 3. Connect and Setup

```bash
# Wait for user data script to complete (may take 5-10 minutes)
sleep 300

# SSH into instance
ssh -i ~/.ssh/k3rs-aws ec2-user@$PUBLIC_IP

# Check if initialization is complete
tail -f /var/log/cloud-init-output.log
# Wait for: "Cloud-init v. X.X.X finished"

# Switch to k3rs user
sudo su - k3rs
cd ~/k3rs

# Build Rust binaries
source $HOME/.cargo/env
cargo build --release

# Set volumes path
export KUBELET_VOLUMES_PATH=$(pwd)/.k3rs/volumes
echo "export KUBELET_VOLUMES_PATH=$(pwd)/.k3rs/volumes" >> ~/.bashrc

# Build container images
docker-compose build

# Start cluster
docker-compose up -d

# Bootstrap
cat bootstrap-cluster.yaml | envsubst > /tmp/bootstrap-expanded.yaml
KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify apply -f /tmp/bootstrap-expanded.yaml

# Verify
KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify get pods -A
```

### 4. Configure DNS (Optional)

```bash
# Create Route53 hosted zone (if you have a domain)
ZONE_ID=$(aws route53 create-hosted-zone \
  --name k3rs.example.com \
  --caller-reference $(date +%s) \
  --query 'HostedZone.Id' \
  --output text)

# Create A record pointing to your instance
aws route53 change-resource-record-sets \
  --hosted-zone-id $ZONE_ID \
  --change-batch '{
    "Changes": [{
      "Action": "CREATE",
      "ResourceRecordSet": {
        "Name": "api.k3rs.example.com",
        "Type": "A",
        "TTL": 300,
        "ResourceRecords": [{"Value": "'$PUBLIC_IP'"}]
      }
    }]
  }'
```

## High Availability Deployment

### 1. Launch Multiple Instances

For HA, launch 3 instances (one in each availability zone):

```bash
# Launch instance in AZ-A
INSTANCE_1=$(aws ec2 run-instances \
  --image-id $AMI_ID \
  --instance-type t3.xlarge \
  --key-name k3rs-keypair \
  --security-group-ids $SG_ID \
  --subnet-id $SUBNET_ID \
  --user-data file://user-data.sh \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=k3rs-master-1}]' \
  --query 'Instances[0].InstanceId' \
  --output text)

# Launch instance in AZ-B
INSTANCE_2=$(aws ec2 run-instances \
  --image-id $AMI_ID \
  --instance-type t3.xlarge \
  --key-name k3rs-keypair \
  --security-group-ids $SG_ID \
  --subnet-id $SUBNET_ID_2 \
  --user-data file://user-data.sh \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=k3rs-master-2}]' \
  --query 'Instances[0].InstanceId' \
  --output text)

# Launch instance in AZ-C
INSTANCE_3=$(aws ec2 run-instances \
  --image-id $AMI_ID \
  --instance-type t3.xlarge \
  --key-name k3rs-keypair \
  --security-group-ids $SG_ID \
  --subnet-id $SUBNET_ID_3 \
  --user-data file://user-data.sh \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=k3rs-master-3}]' \
  --query 'Instances[0].InstanceId' \
  --output text)

echo "Instances: $INSTANCE_1, $INSTANCE_2, $INSTANCE_3"
```

### 2. Create Application Load Balancer

```bash
# Create load balancer
LB_ARN=$(aws elbv2 create-load-balancer \
  --name k3rs-lb \
  --subnets $SUBNET_ID $SUBNET_ID_2 $SUBNET_ID_3 \
  --security-groups $SG_ID \
  --scheme internet-facing \
  --type application \
  --ip-address-type ipv4 \
  --query 'LoadBalancers[0].LoadBalancerArn' \
  --output text)

# Create target group for API server
TG_ARN=$(aws elbv2 create-target-group \
  --name k3rs-api-tg \
  --protocol HTTPS \
  --port 6443 \
  --vpc-id $VPC_ID \
  --health-check-protocol HTTPS \
  --health-check-path /healthz \
  --query 'TargetGroups[0].TargetGroupArn' \
  --output text)

# Register instances
aws elbv2 register-targets \
  --target-group-arn $TG_ARN \
  --targets Id=$INSTANCE_1 Id=$INSTANCE_2 Id=$INSTANCE_3

# Create listener
aws elbv2 create-listener \
  --load-balancer-arn $LB_ARN \
  --protocol HTTPS \
  --port 6443 \
  --default-actions Type=forward,TargetGroupArn=$TG_ARN

# Get load balancer DNS
LB_DNS=$(aws elbv2 describe-load-balancers \
  --load-balancer-arns $LB_ARN \
  --query 'LoadBalancers[0].DNSName' \
  --output text)

echo "Load Balancer DNS: $LB_DNS"
```

### 3. Configure HA Cluster

On each instance, use the HA compose file:

```bash
# SSH to each instance and run:
cd ~/k3rs

# Use HA compose file
docker-compose -f docker-compose.ha.yml up -d

# Configure etcd cluster on first node
# (See HIGH_AVAILABILITY.md for detailed etcd clustering steps)
```

## Storage Configuration

### EBS Volume Management

```bash
# Create additional EBS volume for etcd data
VOLUME_ID=$(aws ec2 create-volume \
  --availability-zone us-east-1a \
  --size 50 \
  --volume-type gp3 \
  --iops 3000 \
  --throughput 125 \
  --tag-specifications 'ResourceType=volume,Tags=[{Key=Name,Value=k3rs-etcd-data}]' \
  --query 'VolumeId' \
  --output text)

# Attach to instance
aws ec2 attach-volume \
  --volume-id $VOLUME_ID \
  --instance-id $INSTANCE_ID \
  --device /dev/sdf

# On instance: Format and mount
sudo mkfs.ext4 /dev/nvme1n1
sudo mkdir -p /mnt/etcd-data
sudo mount /dev/nvme1n1 /mnt/etcd-data
echo '/dev/nvme1n1 /mnt/etcd-data ext4 defaults,nofail 0 2' | sudo tee -a /etc/fstab
```

### EBS Snapshots for Backup

```bash
# Create snapshot
SNAPSHOT_ID=$(aws ec2 create-snapshot \
  --volume-id $VOLUME_ID \
  --description "k3rs etcd backup $(date +%Y-%m-%d)" \
  --tag-specifications 'ResourceType=snapshot,Tags=[{Key=Name,Value=k3rs-backup}]' \
  --query 'SnapshotId' \
  --output text)

# List snapshots
aws ec2 describe-snapshots \
  --owner-ids self \
  --filters "Name=tag:Name,Values=k3rs-backup"
```

## Networking Configuration

### VPC Peering (For Multi-Region)

```bash
# Create VPC peering connection
aws ec2 create-vpc-peering-connection \
  --vpc-id $VPC_ID \
  --peer-vpc-id $PEER_VPC_ID \
  --peer-region us-west-2
```

### Network Load Balancer (For NodePort Services)

```bash
# Create NLB for NodePort services
NLB_ARN=$(aws elbv2 create-load-balancer \
  --name k3rs-nodeport-lb \
  --type network \
  --subnets $SUBNET_ID $SUBNET_ID_2 $SUBNET_ID_3 \
  --query 'LoadBalancers[0].LoadBalancerArn' \
  --output text)
```

## Security Configuration

### IAM Roles for EC2

```bash
# Create IAM role for EC2 instances
cat > trust-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "ec2.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF

aws iam create-role \
  --role-name K3rsEC2Role \
  --assume-role-policy-document file://trust-policy.json

# Attach policies
aws iam attach-role-policy \
  --role-name K3rsEC2Role \
  --policy-arn arn:aws:iam::aws:policy/AmazonEC2ReadOnlyAccess

aws iam attach-role-policy \
  --role-name K3rsEC2Role \
  --policy-arn arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy

# Create instance profile
aws iam create-instance-profile \
  --instance-profile-name K3rsInstanceProfile

aws iam add-role-to-instance-profile \
  --instance-profile-name K3rsInstanceProfile \
  --role-name K3rsEC2Role

# Associate with instance
aws ec2 associate-iam-instance-profile \
  --instance-id $INSTANCE_ID \
  --iam-instance-profile Name=K3rsInstanceProfile
```

### TLS Certificates with ACM

```bash
# Request certificate from ACM
CERT_ARN=$(aws acm request-certificate \
  --domain-name api.k3rs.example.com \
  --validation-method DNS \
  --query 'CertificateArn' \
  --output text)

# Use this certificate with ALB
aws elbv2 modify-listener \
  --listener-arn $LISTENER_ARN \
  --certificates CertificateArn=$CERT_ARN
```

## Monitoring and Logging

### CloudWatch Setup

```bash
# Install CloudWatch agent
sudo yum install -y amazon-cloudwatch-agent

# Create config
sudo tee /opt/aws/amazon-cloudwatch-agent/etc/config.json > /dev/null <<'EOF'
{
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/k3rs/*.log",
            "log_group_name": "/k3rs/cluster",
            "log_stream_name": "{instance_id}"
          }
        ]
      }
    }
  },
  "metrics": {
    "namespace": "k3rs",
    "metrics_collected": {
      "cpu": {"measurement": [{"name": "cpu_usage_idle"}]},
      "disk": {"measurement": [{"name": "used_percent"}]},
      "mem": {"measurement": [{"name": "mem_used_percent"}]}
    }
  }
}
EOF

# Start agent
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config \
  -m ec2 \
  -s \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/config.json
```

## Backup and Disaster Recovery

### Automated Backup Script

```bash
# Create backup script
sudo tee /usr/local/bin/backup-k3rs.sh > /dev/null <<'EOF'
#!/bin/bash
INSTANCE_ID=$(ec2-metadata --instance-id | cut -d " " -f 2)
VOLUME_ID=$(aws ec2 describe-volumes \
  --filters "Name=attachment.instance-id,Values=$INSTANCE_ID" \
  --query 'Volumes[0].VolumeId' \
  --output text)

SNAPSHOT_ID=$(aws ec2 create-snapshot \
  --volume-id $VOLUME_ID \
  --description "Automated backup $(date +%Y-%m-%d-%H-%M)" \
  --tag-specifications 'ResourceType=snapshot,Tags=[{Key=Type,Value=automated}]' \
  --query 'SnapshotId' \
  --output text)

echo "Created snapshot: $SNAPSHOT_ID"

# Delete snapshots older than 7 days
aws ec2 describe-snapshots \
  --owner-ids self \
  --filters "Name=tag:Type,Values=automated" \
  --query 'Snapshots[?StartTime<=`'$(date -d '7 days ago' --iso-8601)'`].SnapshotId' \
  --output text | xargs -r -n 1 aws ec2 delete-snapshot --snapshot-id
EOF

sudo chmod +x /usr/local/bin/backup-k3rs.sh

# Add to cron (daily at 2 AM)
(crontab -l 2>/dev/null; echo "0 2 * * * /usr/local/bin/backup-k3rs.sh >> /var/log/k3rs-backup.log 2>&1") | crontab -
```

## Scaling

### Auto Scaling Group (For Worker Nodes)

```bash
# Create launch template
TEMPLATE_ID=$(aws ec2 create-launch-template \
  --launch-template-name k3rs-worker \
  --version-description "k3rs worker node" \
  --launch-template-data '{
    "ImageId": "'$AMI_ID'",
    "InstanceType": "t3.large",
    "KeyName": "k3rs-keypair",
    "SecurityGroupIds": ["'$SG_ID'"],
    "UserData": "'$(base64 -w 0 worker-user-data.sh)'"
  }' \
  --query 'LaunchTemplate.LaunchTemplateId' \
  --output text)

# Create auto scaling group
aws autoscaling create-auto-scaling-group \
  --auto-scaling-group-name k3rs-workers \
  --launch-template LaunchTemplateId=$TEMPLATE_ID \
  --min-size 1 \
  --max-size 10 \
  --desired-capacity 3 \
  --vpc-zone-identifier "$SUBNET_ID,$SUBNET_ID_2,$SUBNET_ID_3" \
  --tags Key=Name,Value=k3rs-worker,PropagateAtLaunch=true
```

## Cost Optimization

### Right-Sizing Recommendations

- **Development**: t3.medium (2 vCPU, 4 GB RAM)
- **Production Single-Node**: t3.xlarge (4 vCPU, 16 GB RAM)
- **Production HA**: 3x t3.xlarge or 3x t3.2xlarge

### Savings Plans

```bash
# Check savings plans
aws savingsplans describe-savings-plans

# Consider purchasing if running long-term
```

### Spot Instances (For Worker Nodes)

```bash
# Use spot instances for cost savings on worker nodes
aws ec2 request-spot-instances \
  --spot-price "0.10" \
  --instance-count 3 \
  --type "persistent" \
  --launch-specification '{
    "ImageId": "'$AMI_ID'",
    "InstanceType": "t3.large",
    "KeyName": "k3rs-keypair",
    "SecurityGroupIds": ["'$SG_ID'"],
    "SubnetId": "'$SUBNET_ID'"
  }'
```

## Troubleshooting

### Instance Not Reachable

```bash
# Check instance status
aws ec2 describe-instance-status --instance-ids $INSTANCE_ID

# Get system log
aws ec2 get-console-output --instance-id $INSTANCE_ID

# Get screenshot (if GUI is available)
aws ec2 get-console-screenshot --instance-id $INSTANCE_ID
```

### High Costs

```bash
# Check current costs
aws ce get-cost-and-usage \
  --time-period Start=2024-01-01,End=2024-01-31 \
  --granularity MONTHLY \
  --metrics "UnblendedCost"

# Enable cost anomaly detection
aws ce create-anomaly-monitor \
  --anomaly-monitor Name=k3rs-monitor,MonitorType=DIMENSIONAL
```

## Cleanup

To avoid charges, delete all resources:

```bash
# Terminate instances
aws ec2 terminate-instances --instance-ids $INSTANCE_ID $INSTANCE_2 $INSTANCE_3

# Delete load balancer
aws elbv2 delete-load-balancer --load-balancer-arn $LB_ARN

# Delete target group
aws elbv2 delete-target-group --target-group-arn $TG_ARN

# Release Elastic IP
aws ec2 release-address --allocation-id $EIP_ALLOC

# Delete volumes
aws ec2 delete-volume --volume-id $VOLUME_ID

# Delete snapshots
aws ec2 delete-snapshot --snapshot-id $SNAPSHOT_ID

# Delete security group
aws ec2 delete-security-group --group-id $SG_ID

# Delete subnets
aws ec2 delete-subnet --subnet-id $SUBNET_ID
aws ec2 delete-subnet --subnet-id $SUBNET_ID_2
aws ec2 delete-subnet --subnet-id $SUBNET_ID_3

# Detach and delete Internet Gateway
aws ec2 detach-internet-gateway --internet-gateway-id $IGW_ID --vpc-id $VPC_ID
aws ec2 delete-internet-gateway --internet-gateway-id $IGW_ID

# Delete VPC
aws ec2 delete-vpc --vpc-id $VPC_ID
```

## Summary Checklist

✅ AWS account configured
✅ VPC and networking created
✅ Security groups configured
✅ EC2 instances launched
✅ Storage configured
✅ k3rs deployed
✅ HA setup (if applicable)
✅ Monitoring enabled
✅ Backups configured
✅ DNS configured (optional)
✅ TLS certificates (optional)

## Cost Estimate

**Single-Node Setup (us-east-1):**
- t3.xlarge instance: ~$150/month
- 100 GB gp3 EBS: ~$8/month
- Data transfer: ~$10-20/month
- **Total**: ~$170-180/month

**HA Setup (3 nodes):**
- 3x t3.xlarge instances: ~$450/month
- 3x 100 GB gp3 EBS: ~$24/month
- Application Load Balancer: ~$20/month
- Data transfer: ~$30-50/month
- **Total**: ~$525-545/month

## Support

- **AWS Documentation**: https://docs.aws.amazon.com/
- **k3rs Docs**: See `docs/` directory
- **Issues**: Report on GitHub
- **HA Setup**: See [HIGH_AVAILABILITY.md](HIGH_AVAILABILITY.md)

---

**Production Ready**: This guide provides a production-grade deployment on AWS with high availability, monitoring, backups, and security best practices.
