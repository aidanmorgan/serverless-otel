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
      USE_FILESYSTEM_MUTEX = "True"
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

resource "aws_api_gateway_rest_api" "ingest_lambda_rest_api" {
  name        = "${var.prefix}-serverless-otel-ingest-api"
  description = "API Gateway for ingest_lambda"
}

resource "aws_api_gateway_resource" "ingest_lambda_rest_api_resource" {
  rest_api_id = aws_api_gateway_rest_api.ingest_lambda_rest_api.id
  parent_id   = aws_api_gateway_rest_api.ingest_lambda_rest_api.root_resource_id
  path_part   = "ingest"
}

resource "aws_api_gateway_method" "ingest_lambda_rest_api_resource_method" {
  rest_api_id   = aws_api_gateway_rest_api.ingest_lambda_rest_api.id
  resource_id   = aws_api_gateway_resource.ingest_lambda_rest_api_resource.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "ingest_lambda_rest_api_resource_integration" {
  rest_api_id             = aws_api_gateway_rest_api.ingest_lambda_rest_api.id
  resource_id             = aws_api_gateway_resource.ingest_lambda_rest_api_resource.id
  http_method             = aws_api_gateway_method.ingest_lambda_rest_api_resource_method.http_method
  uri                     = aws_lambda_function.serverless_otel_ingest.invoke_arn
  integration_http_method = "POST"
  type                    = "AWS"
  request_parameters      = {
    "integration.request.header.X-Amz-Invocation-Type" = "'Event'"
  }
}

# Create API Gateway method response
resource "aws_api_gateway_method_response" "ingest_lambda_rest_api_response_200" {
  rest_api_id = aws_api_gateway_rest_api.ingest_lambda_rest_api.id
  resource_id = aws_api_gateway_resource.ingest_lambda_rest_api_resource.id
  http_method = aws_api_gateway_method.ingest_lambda_rest_api_resource_method.http_method
  status_code = "200"
}

# Create API Gateway integration response
resource "aws_api_gateway_integration_response" "ingest_lambda_rest_api_integration_response" {
  rest_api_id = aws_api_gateway_rest_api.ingest_lambda_rest_api.id
  resource_id = aws_api_gateway_resource.ingest_lambda_rest_api_resource.id
  http_method = aws_api_gateway_method.ingest_lambda_rest_api_resource_method.http_method
  status_code = aws_api_gateway_method_response.ingest_lambda_rest_api_response_200.status_code

  depends_on = [
    aws_api_gateway_integration.ingest_lambda_rest_api_resource_integration
  ]
}

# Deploy the API Gateway
resource "aws_api_gateway_deployment" "ingest_lambda_rest_api_deployment" {
  rest_api_id = aws_api_gateway_rest_api.ingest_lambda_rest_api.id
  stage_name  = "prod"

  depends_on = [
    aws_api_gateway_integration.ingest_lambda_rest_api_resource_integration
  ]
}

# Allow API Gateway to invoke the Lambda function
resource "aws_lambda_permission" "api_gateway_lambda" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.serverless_otel_ingest.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.ingest_lambda_rest_api.execution_arn}/*/*"
}


output "api_gateway_url" {
  value = "${aws_api_gateway_deployment.ingest_lambda_rest_api_deployment.invoke_url}${aws_api_gateway_resource.ingest_lambda_rest_api_resource.path}"
}


