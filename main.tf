# [Resume point 8] Terraform (Infrastructure as Code)
# Databricks workspace + cluster, S3 storage, IAM, and streaming/ingestion
# services (MSK, Glue, DMS) — versioned in Git, reproducible across dev/test/prod.

terraform {
  required_providers {
    aws        = { source = "hashicorp/aws", version = "~> 5.0" }
    databricks = { source = "databricks/databricks", version = "~> 1.0" }
  }
}

variable "env" { default = "dev" }

# --- storage ---
resource "aws_s3_bucket" "lakehouse" {
  bucket = "hca-clinical-lakehouse-${var.env}"
}

# --- IAM role for Databricks/Glue/DMS ---
resource "aws_iam_role" "data_platform" {
  name = "clinical-data-platform-${var.env}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

# --- streaming: Amazon MSK (Kafka) ---
resource "aws_msk_cluster" "clinical_events" {
  cluster_name           = "clinical-events-${var.env}"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 3
  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = var.subnets
    security_groups = var.security_groups
    storage_info { ebs_storage_info { volume_size = 100 } }
  }
}

# --- CDC: AWS DMS ---
resource "aws_dms_replication_instance" "cdc" {
  replication_instance_id    = "clinical-cdc-${var.env}"
  replication_instance_class = "dms.c5.large"
  allocated_storage          = 100
}

# --- ETL: AWS Glue database ---
resource "aws_glue_catalog_database" "silver" {
  name = "clinical_silver_${var.env}"
}

# --- Databricks cluster (autoscaling + Photon) ---
resource "databricks_cluster" "lakehouse" {
  cluster_name  = "clinical-lakehouse-${var.env}"
  spark_version = "14.3.x-photon-scala2.12"
  node_type_id  = "i3.xlarge"
  autoscale { min_workers = 2, max_workers = 8 }
  spark_conf = { "spark.sql.adaptive.enabled" = "true" }
}

variable "subnets" { type = list(string), default = [] }
variable "security_groups" { type = list(string), default = [] }
