# Cloud foundation only. Not applied by CI; this creates billable resources.
terraform {
  required_version = ">= 1.6.0"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.91.0"
    }
  }
}

# Credentials: CONFLUENT_CLOUD_API_KEY and CONFLUENT_CLOUD_API_SECRET.
provider "confluent" {}

variable "region" {
  type    = string
  default = "us-east-2"
}

resource "confluent_environment" "payments" {
  display_name = "payments-lab"
  stream_governance {
    package = "ESSENTIALS"
  }
}

resource "confluent_kafka_cluster" "payments" {
  display_name = "payments"
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = var.region
  basic {}
  environment {
    id = confluent_environment.payments.id
  }
}

resource "confluent_service_account" "identity" {
  for_each     = toset(["admin", "writer", "reader"])
  display_name = "payments-${each.key}"
  description  = "Isolated payments lab ${each.key}"
}

resource "confluent_role_binding" "admin" {
  principal   = "User:${confluent_service_account.identity["admin"].id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.payments.rbac_crn
}

resource "confluent_api_key" "kafka" {
  for_each     = confluent_service_account.identity
  display_name = "payments-${each.key}-kafka"
  owner {
    id          = each.value.id
    api_version = each.value.api_version
    kind        = each.value.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.payments.id
    api_version = confluent_kafka_cluster.payments.api_version
    kind        = confluent_kafka_cluster.payments.kind
    environment {
      id = confluent_environment.payments.id
    }
  }
  depends_on = [confluent_role_binding.admin]
}

resource "confluent_kafka_topic" "topic" {
  for_each         = toset(["payments.public.accounts", "payments.public.transactions", "payments.enriched", "payments.alerts"])
  topic_name       = each.key
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.payments.rest_endpoint
  kafka_cluster {
    id = confluent_kafka_cluster.payments.id
  }
  credentials {
    key    = confluent_api_key.kafka["admin"].id
    secret = confluent_api_key.kafka["admin"].secret
  }
}

locals {
  permissions = {
    writer_topic = { identity = "writer", type = "TOPIC", name = "payments.public.", operation = "WRITE" }
    reader_topic = { identity = "reader", type = "TOPIC", name = "payments.", operation = "READ" }
    reader_group = { identity = "reader", type = "GROUP", name = "payments-reader-", operation = "READ" }
  }
}

resource "confluent_kafka_acl" "access" {
  for_each      = local.permissions
  resource_type = each.value.type
  resource_name = each.value.name
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.identity[each.value.identity].id}"
  host          = "*"
  operation     = each.value.operation
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.payments.rest_endpoint
  kafka_cluster {
    id = confluent_kafka_cluster.payments.id
  }
  credentials {
    key    = confluent_api_key.kafka["admin"].id
    secret = confluent_api_key.kafka["admin"].secret
  }
}

data "confluent_schema_registry_cluster" "payments" {
  environment {
    id = confluent_environment.payments.id
  }
  depends_on = [confluent_kafka_cluster.payments]
}

# Opt-in: a compute pool alone does NOT deploy the local SQL or CDC connector.
variable "create_compute_pool" {
  type    = bool
  default = false
}

resource "confluent_flink_compute_pool" "payments" {
  count        = var.create_compute_pool ? 1 : 0
  display_name = "payments"
  cloud        = "AWS"
  region       = var.region
  max_cfu      = 5
  environment {
    id = confluent_environment.payments.id
  }
}

output "endpoints" {
  value = {
    environment_id   = confluent_environment.payments.id
    cluster_id       = confluent_kafka_cluster.payments.id
    bootstrap_server = confluent_kafka_cluster.payments.bootstrap_endpoint
    schema_registry  = data.confluent_schema_registry_cluster.payments.rest_endpoint
  }
}

# Keep terraform.tfstate private: generated API secrets are in it.
output "kafka_credentials" {
  sensitive = true
  value = { for name, key in confluent_api_key.kafka : name => {
    api_key = key.id, api_secret = key.secret
  } }
}
