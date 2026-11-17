# IRSA: the control-plane service account may only touch its own queues.
data "aws_iam_policy_document" "control_plane" {
  statement {
    actions = [
      "sqs:SendMessage", "sqs:ReceiveMessage", "sqs:DeleteMessage",
      "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility",
    ]
    resources = [aws_sqs_queue.commands.arn]
  }
}

resource "aws_iam_policy" "control_plane" {
  name   = "${local.name}-control-plane"
  policy = data.aws_iam_policy_document.control_plane.json
}

module "control_plane_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.44"

  role_name        = "${local.name}-control-plane"
  role_policy_arns = { sqs = aws_iam_policy.control_plane.arn }
  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["edgefleet:control-plane"]
    }
  }
}

# Firmware images for OTA, served to devices via presigned URLs.
resource "aws_s3_bucket" "firmware" {
  bucket_prefix = "${local.name}-firmware-"
}

resource "aws_s3_bucket_versioning" "firmware" {
  bucket = aws_s3_bucket.firmware.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "firmware" {
  bucket                  = aws_s3_bucket.firmware.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_ecr_repository" "images" {
  for_each             = toset(["control-plane", "simulator", "loadgen"])
  name                 = "edgefleet/${each.key}"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true
  image_scanning_configuration {
    scan_on_push = true
  }
}
