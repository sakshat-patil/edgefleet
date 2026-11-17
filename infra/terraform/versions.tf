terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }
  # Uncomment and point at your own bucket/table for shared state.
  # backend "s3" {
  #   bucket         = "edgefleet-tfstate"
  #   key            = "edgefleet/terraform.tfstate"
  #   region         = "us-west-2"
  #   dynamodb_table = "edgefleet-tflock"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project   = "edgefleet"
      ManagedBy = "terraform"
      Env       = var.env
    }
  }
}
