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
resource "aws_efs_file_system" "otel_hot" {
  lifecycle_policy {
    transition_to_ia = "AFTER_30_DAYS"
  }
  tags = {
    Name = "${var.prefix}-otel-hot"
  }
}

# Create a mount target for the EFS in the subnet
resource "aws_efs_mount_target" "otel_hot_mount" {
  file_system_id = aws_efs_file_system.otel_hot.id
  subnet_id      = aws_subnet.serverless_otel_subnet.id
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
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaEFSAccessExecutionRole"
}

# Create the Lambda function
resource "aws_lambda_function" "serverless_otel_ingest" {
  function_name = "${var.prefix}-serverless-otel-ingest"
  role          = aws_iam_role.ingest_lambda_exec_role.arn
  handler       = "lambda.lambda_handler"
  runtime       = "python3.12"

  vpc_config {
    subnet_ids         = [aws_subnet.serverless_otel_subnet.id]
    security_group_ids = [aws_security_group.ingest_lambda_sg.id]
  }

  file_system_config {
    arn              = aws_efs_file_system.otel_hot.arn
    local_mount_path = "/mnt/otel-hot"
  }
}

# Create an API Gateway to accept HTTP traffic
resource "aws_apigatewayv2_api" "http_api" {
  name          = "${var.prefix}-serverless-otel-http-api"
  protocol_type = "HTTP"
}

# Create an integration with the Lambda function
resource "aws_apigatewayv2_integration" "lambda_integration" {
  api_id           = aws_apigatewayv2_api.http_api.id
  integration_type = "AWS_PROXY"
  integration_uri  = aws_lambda_function.serverless_otel_ingest.invoke_arn
}

# Create a route for the API
resource "aws_apigatewayv2_route" "default_route" {
  api_id    = aws_apigatewayv2_api.http_api.id
  route_key = "ANY /{proxy+}"
  target    = "integrations/${aws_apigatewayv2_integration.lambda_integration.id}"
}

# Deploy the API
resource "aws_apigatewayv2_stage" "api_stage" {
  api_id      = aws_apigatewayv2_api.http_api.id
  name        = "$default"
  auto_deploy = true
}