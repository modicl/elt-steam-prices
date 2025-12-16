variable "creds" {
  description = "Project credentials"
  default     = "./keys/my-creds.json"
}

variable "project" {
  description = "My Project ID"
  default     = "my_project_id"

}

variable "region" {
  description = "Region"
  default     = "us-central1"

}

variable "location" {
  description = "Project Location"
  default     = "US"

}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "my_dataset_name"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "my_bucket_name"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}


