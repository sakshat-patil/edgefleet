output "cluster_name" {
  value = module.eks.cluster_name
}

output "kubeconfig_command" {
  value = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}

output "redis_endpoint" {
  value = "${aws_elasticache_replication_group.redis.primary_endpoint_address}:6379"
}

output "sqs_queue_url" {
  value = aws_sqs_queue.commands.url
}

output "control_plane_role_arn" {
  value = module.control_plane_irsa.iam_role_arn
}

output "firmware_bucket" {
  value = aws_s3_bucket.firmware.bucket
}

output "ecr_repositories" {
  value = { for k, r in aws_ecr_repository.images : k => r.repository_url }
}
