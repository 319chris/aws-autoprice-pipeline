variable "region" {
  description = "AWS region to deploy"
  type        = string
  default     = "ap-southeast-2"
}

variable "project" {
  description = "Project prefix for naming"
  type        = string
  default     = "autoprice"
}

variable "env" {
  description = "Environment (dev/stg/prod)"
  type        = string
  default     = "dev"
}
