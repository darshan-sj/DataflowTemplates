

# Autogenerated file. DO NOT EDIT.
#
# Copyright (C) 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#


variable "on_delete" {
  type        = string
  description = "One of \"drain\" or \"cancel\". Specifies behavior of deletion during terraform destroy."
}

variable "project" {
  type        = string
  description = "The Google Cloud Project ID within which this module provisions resources."
}

variable "region" {
  type        = string
  description = "The region in which the created job should run."
}

variable "inputSubscription" {
  type        = string
  description = "Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name' (Example: projects/your-project-id/subscriptions/your-subscription-name)"

}

variable "apiKey" {
  type        = string
  description = "Datadog Logs API key. Must be provided if the apiKeySource is set to PLAINTEXT or KMS. See: https://docs.datadoghq.com/account_management/api-app-keys"
  default     = null
}

variable "url" {
  type        = string
  description = "Datadog Logs API URL. This should be routable from the VPC in which the pipeline runs. See: https://docs.datadoghq.com/api/latest/logs/#send-logs (Example: https://http-intake.logs.datadoghq.com)"

}

variable "batchCount" {
  type        = number
  description = "Batch size for sending multiple events to Datadog Logs API. Min is 10. Max is 1000. Defaults to 100."
  default     = null
}

variable "parallelism" {
  type        = number
  description = "Maximum number of parallel requests. Default 1 (no parallelism)."
  default     = null
}

variable "includePubsubMessage" {
  type        = bool
  description = "Include full Pub/Sub message in the payload (true/false). Defaults to true (all elements, including data element, are included in the payload)."
  default     = null
}

variable "apiKeyKMSEncryptionKey" {
  type        = string
  description = "The Cloud KMS key to decrypt the Logs API key. This parameter must be provided if the apiKeySource is set to KMS. If this parameter is provided, apiKey string should be passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. The Key should be in the format projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}. See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt  (Example: projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name)"
  default     = null
}

variable "apiKeySecretId" {
  type        = string
  description = "Secret Manager secret ID for the apiKey. This parameter should be provided if the apiKeySource is set to SECRET_MANAGER. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}. (Example: projects/your-project-id/secrets/your-secret/versions/your-secret-version)"
  default     = null
}

variable "apiKeySource" {
  type        = string
  description = "Source of the API key. One of PLAINTEXT, KMS or SECRET_MANAGER. This parameter must be provided if secret manager is used. If apiKeySource is set to KMS, apiKeyKMSEncryptionKey and encrypted apiKey must be provided. If apiKeySource is set to SECRET_MANAGER, apiKeySecretId must be provided. If apiKeySource is set to PLAINTEXT, apiKey must be provided."
  default     = null
}

variable "javascriptTextTransformGcsPath" {
  type        = string
  description = "The Cloud Storage path pattern for the JavaScript code containing your user-defined functions."
  default     = null
}

variable "javascriptTextTransformFunctionName" {
  type        = string
  description = "The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: transform_udf1)"
  default     = null
}

variable "javascriptTextTransformReloadIntervalMinutes" {
  type        = number
  description = "Define the interval that workers may check for JavaScript UDF changes to reload the files. Defaults to: 0."
  default     = null
}

variable "outputDeadletterTopic" {
  type        = string
  description = "The Pub/Sub topic to publish deadletter records to. The name should be in the format of `projects/your-project-id/topics/your-topic-name`."

}


provider "google" {
  project = var.project
}

variable "additional_experiments" {
  type        = set(string)
  description = "List of experiments that should be used by the job. An example value is  'enable_stackdriver_agent_metrics'."
  default     = null
}

variable "enable_streaming_engine" {
  type        = bool
  description = "Indicates if the job should use the streaming engine feature."
  default     = null
}

variable "ip_configuration" {
  type        = string
  description = "The configuration for VM IPs. Options are 'WORKER_IP_PUBLIC' or 'WORKER_IP_PRIVATE'."
  default     = null
}

variable "kms_key_name" {
  type        = string
  description = "The name for the Cloud KMS key for the job. Key format is: projects/PROJECT_ID/locations/LOCATION/keyRings/KEY_RING/cryptoKeys/KEY"
  default     = null
}

variable "labels" {
  type        = map(string)
  description = "User labels to be specified for the job. Keys and values should follow the restrictions specified in the labeling restrictions page. NOTE: This field is non-authoritative, and will only manage the labels present in your configuration.				Please refer to the field 'effective_labels' for all of the labels present on the resource."
  default     = null
}

variable "machine_type" {
  type        = string
  description = "The machine type to use for the job."
  default     = null
}

variable "max_workers" {
  type        = number
  description = "The number of workers permitted to work on the job. More workers may improve processing speed at additional cost."
  default     = null
}

variable "name" {
  type        = string
  description = "A unique name for the resource, required by Dataflow."
}

variable "network" {
  type        = string
  description = "The network to which VMs will be assigned. If it is not provided, 'default' will be used."
  default     = null
}

variable "service_account_email" {
  type        = string
  description = "The Service Account email used to create the job."
  default     = null
}

variable "skip_wait_on_job_termination" {
  type        = bool
  description = "If true, treat DRAINING and CANCELLING as terminal job states and do not wait for further changes before removing from terraform state and moving on. WARNING: this will lead to job name conflicts if you do not ensure that the job names are different, e.g. by embedding a release ID or by using a random_id."
  default     = null
}

variable "subnetwork" {
  type        = string
  description = "The subnetwork to which VMs will be assigned. Should be of the form 'regions/REGION/subnetworks/SUBNETWORK'."
  default     = null
}

variable "temp_gcs_location" {
  type        = string
  description = "A writeable location on Google Cloud Storage for the Dataflow job to dump its temporary data."
}

variable "zone" {
  type        = string
  description = "The zone in which the created job should run. If it is not provided, the provider zone is used."
  default     = null
}

resource "google_project_service" "required" {
  service            = "dataflow.googleapis.com"
  disable_on_destroy = false
}

resource "google_dataflow_job" "generated" {
  depends_on        = [google_project_service.required]
  provider          = google
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/Cloud_PubSub_to_Datadog"
  parameters = {
    inputSubscription                            = var.inputSubscription
    apiKey                                       = var.apiKey
    url                                          = var.url
    batchCount                                   = tostring(var.batchCount)
    parallelism                                  = tostring(var.parallelism)
    includePubsubMessage                         = tostring(var.includePubsubMessage)
    apiKeyKMSEncryptionKey                       = var.apiKeyKMSEncryptionKey
    apiKeySecretId                               = var.apiKeySecretId
    apiKeySource                                 = var.apiKeySource
    javascriptTextTransformGcsPath               = var.javascriptTextTransformGcsPath
    javascriptTextTransformFunctionName          = var.javascriptTextTransformFunctionName
    javascriptTextTransformReloadIntervalMinutes = tostring(var.javascriptTextTransformReloadIntervalMinutes)
    outputDeadletterTopic                        = var.outputDeadletterTopic
  }

  additional_experiments       = var.additional_experiments
  enable_streaming_engine      = var.enable_streaming_engine
  ip_configuration             = var.ip_configuration
  kms_key_name                 = var.kms_key_name
  labels                       = var.labels
  machine_type                 = var.machine_type
  max_workers                  = var.max_workers
  name                         = var.name
  network                      = var.network
  service_account_email        = var.service_account_email
  skip_wait_on_job_termination = var.skip_wait_on_job_termination
  subnetwork                   = var.subnetwork
  temp_gcs_location            = var.temp_gcs_location
  zone                         = var.zone
  region                       = var.region
}

output "dataflow_job_url" {
  value = "https://console.cloud.google.com/dataflow/jobs/${var.region}/${google_dataflow_job.generated.job_id}"
}

