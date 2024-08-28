variable "aws_region" {
  description = "The AWS region to deploy resources in"
  type        = string
  default = "us-east-1"
}

variable "prefix" {
  description = "The prefix for naming resources"
  type        = string
  default = "dev"
}

provider "aws" {
  region = var.aws_region
  profile = "aidan-personal"
}

# Create a VPC
resource "aws_vpc" "serverless_otel" {
  cidr_block = "10.0.0.0/16"
  tags = {
    Name = "${var.prefix}-serverless-otel"
  }
}

# Create a subnet
resource "aws_subnet" "serverless_otel_subnet" {
  vpc_id            = aws_vpc.serverless_otel.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = "${var.aws_region}a"  # Adjust the availability zone as needed
  tags = {
    Name = "${var.prefix}-serverless-otel-subnet"
  }
}

# Create an EFS file system
resource "aws_efs_file_system" "otel_hot_efs" {
  lifecycle_policy {
    transition_to_ia = "AFTER_30_DAYS"
  }
  tags = {
    Name = "${var.prefix}-otel-hot"
  }
}

# Create a mount target for the EFS in the subnet
resource "aws_efs_mount_target" "otel_hot_mount" {
  file_system_id = aws_efs_file_system.otel_hot_efs.id
  subnet_id      = aws_subnet.serverless_otel_subnet.id
  security_groups = [aws_security_group.ingest_lambda_sg.id]
}

resource "aws_efs_access_point" "otel_hot_access_point" {
  file_system_id = aws_efs_file_system.otel_hot_efs.id

  posix_user {
    uid = 1000
    gid = 1000
  }

  root_directory {
    path = "/otel-hot"
    creation_info {
      owner_uid   = 1000
      owner_gid   = 1000
      permissions = "0755"
    }
  }
}

# Create a security group for the Lambda function
resource "aws_security_group" "ingest_lambda_sg" {
  vpc_id = aws_vpc.serverless_otel.id

  ingress {
    from_port   = 2049
    to_port     = 2049
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.prefix}-lambda-sg"
  }
}

# Create an IAM role for the Lambda function
resource "aws_iam_role" "ingest_lambda_exec_role" {
  name = "${var.prefix}-serverless-otel-ingest-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

# Attach the AWSLambdaVPCAccessExecutionRole policy to the IAM role
resource "aws_iam_role_policy_attachment" "ingest_lambda_vpc_access" {
  role       = aws_iam_role.ingest_lambda_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# Attach the AWSLambdaEFSAccessExecutionRole policy to the IAM role
resource "aws_iam_role_policy_attachment" "ingest_lambda_efs_access" {
  role       = aws_iam_role.ingest_lambda_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonElasticFileSystemClientFullAccess"
}


# Create the Lambda function
resource "aws_lambda_function" "serverless_otel_ingest" {
  function_name = "${var.prefix}-serverless-otel-ingest"
  role          = aws_iam_role.ingest_lambda_exec_role.arn
  handler       = "ingest_lambda.lambda_handler"
  runtime       = "python3.12"
  filename      = "../ingest-lambda/lambda.zip"

  environment {
    variables = {
      SHARED_STORAGE_BASEDIR = "/mnt/otel-hot"
    }
  }

  vpc_config {
    subnet_ids         = [aws_subnet.serverless_otel_subnet.id]
    security_group_ids = [aws_security_group.ingest_lambda_sg.id]
  }

  file_system_config {
    arn              = aws_efs_access_point.otel_hot_access_point.arn
    local_mount_path = "/mnt/otel-hot"
  }
}

resource "aws_lambda_permission" "ingest_lambda_public_access" {
  action        = "lambda:InvokeFunctionUrl"
  function_name = aws_lambda_function.serverless_otel_ingest.function_name
  function_url_auth_type = "NONE"
  principal     = "*"
}

resource "aws_lambda_function_url" "otel-ingest-public-url" {
  function_name      = aws_lambda_function.serverless_otel_ingest.function_name
  authorization_type = "NONE"  # Set to "NONE" for public access
}
