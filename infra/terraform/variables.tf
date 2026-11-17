variable "region" {
  type    = string
  default = "us-west-2"
}

variable "env" {
  type    = string
  default = "dev"
}

variable "cluster_version" {
  type    = string
  default = "1.31"
}

variable "node_instance_types" {
  description = "Instance types for the general node group (control plane, Kafka, simulators)."
  type        = list(string)
  default     = ["m6i.large"]
}

variable "node_min" {
  type    = number
  default = 3
}

variable "node_max" {
  type    = number
  default = 6
}

variable "redis_node_type" {
  type    = string
  default = "cache.t4g.small"
}

variable "admin_cidrs" {
  description = "CIDRs allowed to reach the public EKS API endpoint."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}
