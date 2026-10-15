# Opt-in only: requires a Confluent Cloud account and billable managed Tableflow.
variable "enable_tableflow" {
  type        = bool
  default     = false
  description = "Enable Iceberg Tableflow for the Avro IoT enriched topic. Not applied by CI."
}

resource "confluent_role_binding" "iot_governance" {
  count       = var.enable_tableflow ? 1 : 0
  principal   = "User:${confluent_service_account.identity["admin"].id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.payments.resource_name
}

resource "confluent_api_key" "iot_registry" {
  count        = var.enable_tableflow ? 1 : 0
  display_name = "iot-bootstrap-registry"
  owner {
    id          = confluent_service_account.identity["admin"].id
    api_version = confluent_service_account.identity["admin"].api_version
    kind        = confluent_service_account.identity["admin"].kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.payments.id
    api_version = data.confluent_schema_registry_cluster.payments.api_version
    kind        = data.confluent_schema_registry_cluster.payments.kind
    environment {
      id = confluent_environment.payments.id
    }
  }
  depends_on = [confluent_role_binding.iot_governance]
}

resource "confluent_api_key" "iot_tableflow" {
  count        = var.enable_tableflow ? 1 : 0
  display_name = "iot-bootstrap-tableflow"
  owner {
    id          = confluent_service_account.identity["admin"].id
    api_version = confluent_service_account.identity["admin"].api_version
    kind        = confluent_service_account.identity["admin"].kind
  }
  managed_resource {
    id          = "tableflow"
    api_version = "tableflow/v1"
    kind        = "Tableflow"
    environment {
      id = confluent_environment.payments.id
    }
  }
  depends_on = [confluent_role_binding.iot_governance]
}

resource "confluent_schema" "iot_enriched" {
  count         = var.enable_tableflow ? 1 : 0
  subject_name  = "iot.enriched-value"
  format        = "AVRO"
  schema        = file("${path.module}/../../schemas/iot-enriched.avsc")
  rest_endpoint = data.confluent_schema_registry_cluster.payments.rest_endpoint
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.payments.id
  }
  credentials {
    key    = confluent_api_key.iot_registry[0].id
    secret = confluent_api_key.iot_registry[0].secret
  }
  depends_on = [confluent_kafka_topic.topic]
}

resource "confluent_tableflow_topic" "iot_enriched" {
  count                         = var.enable_tableflow ? 1 : 0
  display_name                  = confluent_kafka_topic.topic["iot.enriched"].topic_name
  table_formats                 = ["ICEBERG"]
  metadata_column_naming_scheme = "PORTABLE"
  environment {
    id = confluent_environment.payments.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.payments.id
  }
  managed_storage {}
  credentials {
    key    = confluent_api_key.iot_tableflow[0].id
    secret = confluent_api_key.iot_tableflow[0].secret
  }
  depends_on = [confluent_schema.iot_enriched]
}

output "iot_iceberg" {
  value = [for table in confluent_tableflow_topic.iot_enriched : {
    topic        = table.display_name
    table_path   = table.table_path
    table_format = table.table_formats
    write_mode   = table.write_mode
  }]
}
