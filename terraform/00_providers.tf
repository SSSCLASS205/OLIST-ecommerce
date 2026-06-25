terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  default = "us-east-1"
}

variable "project" {
  default = "olist-pipeline"
}

variable "db_username" {
  default = "olist_admin"
}

variable "db_password" {
  description = "RDS master password"
  type        = string
  sensitive   = true
}

variable "alert_email" {
  description = "Email to receive MWAA failure alerts"
  type        = string
}

variable "key_pair_name" {
  description = "Existing EC2 key pair for Airbyte host SSH access"
  type        = string
}

data "aws_availability_zones" "available" {
  state = "available"
}
