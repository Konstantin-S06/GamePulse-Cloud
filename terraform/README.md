# GamePulse Terraform

Creates the S3 data lake bucket, scoped IAM user, and monthly cost budget per PROJECT_SPEC.md section 3.5. Terraform does not create access keys.

## Prerequisites

- Terraform 1.6 or newer
- AWS CLI v2 configured with credentials that can create S3, IAM, and Budgets resources

## Usage

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars: bucket_suffix, budget_email, aws_region

terraform init
terraform plan
terraform apply
```

Note the `bucket_name` and `data_root` outputs.

## Credentials (required)

1. Create an access key for the pipeline user:

```bash
aws iam create-access-key --user-name gamepulse-pipeline
```

2. Copy `AccessKeyId` and `SecretAccessKey` into the project `.env` at the repository root (gitignored), along with:

```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
DATA_ROOT=s3://<bucket_name from terraform output>
```

Do not commit `.env`. Do not put keys in Terraform, docs, or chat. Do not set `AWS_SESSION_TOKEN` in `.env`.

3. Run `make s3-smoke` from the project root (inside the runner container) to verify S3 access.

## Revoke and destroy

When you are done with the project:

```bash
aws iam delete-access-key --user-name gamepulse-pipeline --access-key-id <KEY_ID>
cd terraform
terraform destroy
```

Remove AWS keys from local `.env`. See PROJECT_SPEC.md section 3.6 for full cleanup steps.