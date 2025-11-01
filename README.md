# Serverless Data Lake Ingestion Analytics Pipeline (AWS)

Stack: **S3 (upload/curated/results) + Lambda + Glue Crawler + Athena v3 + IAM (+ SNS optional)**

### What it does
- Ingest CSV to `s3://autoprice-upload/`
- S3 event → Lambda
- Lambda starts Glue Crawler & ensures DB → runs Athena CTAS/INSERT to build `autoprice_summary` into `s3://autoprice-curated-bucket/summary/`
- Query via Athena workgroup `autoprice-wg` (results → `s3://autoprice-results-athena/results/`)

### Lambda env vars
- `GLUE_CRAWLER_NAME`: `autoprice-crawler`
- `ATHENA_WORKGROUP`: `autoprice-wg`
- `ATHENA_OUTPUT`: `s3://autoprice-results-athena/results/`
- `DB_NAME`: `autoprice_db`
- `SRC_TABLE`: `autoprice_upload`
- `SUMMARY_TABLE`: `autoprice_summary`
- `SUMMARY_S3`: `s3://autoprice-curated-bucket/summary/`
- `SNS_TOPIC_ARN` (optional)
