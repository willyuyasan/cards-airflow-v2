# The following is a piece of example code to initialize the AWS infrastructure that should be paired with this repo.
# Please note, that as of 2020-08-01, there are still a number of components of this setup that must be manually done
# by someone with either Account Owner or Embedded PE access within a teams account.

# NOTE: When running multiple airflow installations, you need only declare the following code block once.
locals {
  airflow_account = "cards-data-developers"
  airflow_dns_zone_name = {
    testing     = "cards-nonprod.rvapps.io",
    development = "cards-nonprod.rvapps.io",
    staging     = "cards-nonprod.rvapps.io",
    production  = "cards.rvapps.io"
  }[var.workspace]
  airflow_deployments = [
    "development",
    "production"
  ]

  airflow_nonprod_image_version = "1.10.9-latest"
  airflow_prod_image_version    = "1.10.9-latest"
  airflow_team_name             = "cards-data-developers"
  airflow_owner                 = "cards-data-developers@RedVentures.com"
}

data "aws_route53_zone" "main" {
  name = local.airflow_dns_zone_name
  # private_zone = true
}

# cards-airflow Install
## ACN Certificate
resource "aws_acm_certificate" "cards-airflow_cert" {
  count             = contains(local.airflow_deployments, var.workspace) ? 1 : 0
  domain_name       = var.workspace == "production" ? "*.cards-airflow.${local.airflow_dns_zone_name}" : "*.cards-airflow-${var.workspace}.${local.airflow_dns_zone_name}"
  validation_method = "DNS"
}

resource "aws_route53_record" "cards-airflow_cert_validation" {
  count   = contains(local.airflow_deployments, var.workspace) ? 1 : 0
  name    = aws_acm_certificate.cards-airflow_cert[0].domain_validation_options.*.resource_record_name[count.index]
  type    = aws_acm_certificate.cards-airflow_cert[0].domain_validation_options.*.resource_record_type[count.index]
  zone_id = data.aws_route53_zone.main.zone_id
  records = [aws_acm_certificate.cards-airflow_cert[0].domain_validation_options.*.resource_record_value[count.index]]
  ttl     = 60
}

resource "aws_acm_certificate_validation" "cards-airflow_cert_verify" {
  count                   = contains(local.airflow_deployments, var.workspace) ? 1 : 0
  certificate_arn         = aws_acm_certificate.cards-airflow_cert[0].arn
  validation_record_fqdns = aws_route53_record.cards-airflow_cert_validation.*.fqdn
}

# Gathering details about secrets
data "aws_secretsmanager_secret" "airflow_artifactory" {
  name = "artifactory-pull"
}

# Actually spin up the Airflow implementation
module "cards-airflow" {
  source  = "app.terraform.io/RVStandard/airflow/aws"
  version = "~> 1.0"

  enable           = contains(local.airflow_deployments, var.workspace)
  environment      = var.workspace
  project_name     = "airflow"
  owner            = local.airflow_owner
  team_name        = local.airflow_team_name
  image_version    = var.workspace == "production" ? local.airflow_prod_image_version : local.airflow_nonprod_image_version
  github_repo_name = "cards-airflow"

  # Secrets
  artifactory_arn = data.aws_secretsmanager_secret.airflow_artifactory.arn

  # Service Parameter Overrides
  acm_certificate_arn = aws_acm_certificate.cards-airflow_cert[0].arn
  dns_zone_name       = local.airflow_dns_zone_name

  # Network Details
  # vpc_id         = data.terraform_remote_state.core.outputs.vpc_id
  # app_subnet_ids = data.terraform_remote_state.core.outputs.app_subnet_ids

  ## Optional Components
  # Enable these if you decide to use a custom Docker image created within your DAG repo's Dockerfile
  # webserver_image = "airflow-${var.workspace}"
  # flower_image    = "airflow-${var.workspace}"
  # scheduler_image = "airflow-${var.workspace}"
  # worker_image    = "airflow-${var.workspace}"

  # Set a specific fernet_key if you don't want Terraform to handle that for you automatically
  # generate_fernet_key = false
  # ssm__fernet_key = "arn:aws:ssm:us-east-1:${data.terraform_remote_state.core.outputs.account_id}:parameter/${local.airflow_team_name}-airflow-${var.workspace}/fernet_key"

  # If you have custom vpc tagging, use this to override which VPC to use for ECS placement
  vpc_tag_key_override = local.airflow_account

  # Enable/disable possible components: db redis ecr efs webserver scheduler worker flower
  # components    = ["db", "redis", "ecr", "efs", "webserver", "scheduler", "worker", "flower"]

  # Set an alterate location for the CircleCI token. Defaults to /TEAM_NAME-PROJECT_NAME-ENVIRONMENT/circle-token by default
  # circleci_token  = "/airflow/${var.workspace}/circle-token"

  # If you find you are exhausting on memory or CPU within ECS use the following to customize your setup:
  # | Resource  | vCPU  | Memory |
  # | --------- | ----- | ------ |
  # | low       | 0.25  | 512    |
  # | medium    | 0.5   | 1024   |
  # | high      | 1.0   | 2048   |
  # | very-high | 2.0   | 8192   |
  # webserver_task_allocation = "medium"
  # flower_task_allocation = "medium"
  # scheduler_task_allocation = "medium"
  # worker_task_allocation = "medium"
  
  # Enable Spot Instances
  spot_enabled = var.workspace == "production"
}
