import os
import time
import json
import boto3
import botocore
from urllib.parse import unquote_plus
from datetime import date

# ---------- Clients ----------
glue   = boto3.client("glue")
athena = boto3.client("athena")
sns    = boto3.client("sns")
s3     = boto3.client("s3")  # 用于删除当天分区的物理文件

# ---------- Environment Variables ----------
CRAWLER        = os.environ.get("GLUE_CRAWLER_NAME")               # 例：autoprice-crawler
ATHENA_WG      = os.environ.get("ATHENA_WORKGROUP", "primary").strip()
ATHENA_OUTPUT  = os.environ.get("ATHENA_OUTPUT")                   # 例：s3://autoprice-results-athena/results/
SNS_TOPIC_ARN  = os.environ.get("SNS_TOPIC_ARN")

DB_NAME        = os.environ.get("DB_NAME", "autoprice_db")
SRC_TABLE      = os.environ.get("SRC_TABLE", "autoprice_upload")
SUMMARY_TABLE  = os.environ.get("SUMMARY_TABLE", "autoprice_summary")
SUMMARY_S3     = os.environ.get("SUMMARY_S3")                      # 例：s3://autoprice-curated-bucket/summary/
ALERT_THRESHOLD_PCT = float(os.environ.get("ALERT_THRESHOLD_PCT", "10"))

TODAY = date.today().strftime("%Y-%m-%d")


# ---------------- SNS ----------------
def _publish(subject: str, message: str):
    print(f"[SNS] {subject}\n{message}")
    if SNS_TOPIC_ARN:
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)


# ---------------- Athena ----------------
def run_athena(sql: str, wait: bool = True):
    """统一以 DB_NAME 作为默认 schema 执行 SQL（Athena v3 语法）"""
    assert ATHENA_OUTPUT, "ATHENA_OUTPUT env is required"
    params = {
        "QueryString": sql,
        "ResultConfiguration": {"OutputLocation": ATHENA_OUTPUT},
        "QueryExecutionContext": {"Database": DB_NAME}
    }
    if ATHENA_WG:
        params["WorkGroup"] = ATHENA_WG

    try:
        resp = athena.start_query_execution(**params)
        qid = resp["QueryExecutionId"]
        print(f"[ATHENA] started qid={qid}")
    except botocore.exceptions.ClientError as e:
        err = e.response["Error"]
        print("[ATHENA ERROR]", err["Code"], err.get("Message"))
        raise

    if not wait:
        return "SUBMITTED", qid

    while True:
        qe = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state  = qe["Status"]["State"]
        reason = qe["Status"].get("StateChangeReason", "")
        used   = qe.get("Statistics", {}).get("EngineExecutionTimeInMillis")
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            print(f"[ATHENA] state={state} ms={used} reason={reason}")
            if state != "SUCCEEDED":
                raise RuntimeError(reason or state)
            return state, qid
        time.sleep(1.2)


# ---------------- Glue ----------------
def ensure_database_glue():
    """用 Glue API 保证 DB 存在（避免 SQL 方言差异）"""
    try:
        glue.get_database(Name=DB_NAME)
        print(f"[DB] exists: {DB_NAME}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": DB_NAME})
        print(f"[DB] created in Glue: {DB_NAME}")

def wait_glue_table(db: str, table: str, timeout_sec: int = 120) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout_sec:
        try:
            glue.get_table(DatabaseName=db, Name=table)
            print(f"[GLUE] table found: {db}.{table}")
            return True
        except glue.exceptions.EntityNotFoundException:
            time.sleep(2)
        except botocore.exceptions.ClientError as e:
            print("[GLUE ERROR]", e.response["Error"]["Code"], e.response["Error"]["Message"])
            time.sleep(2)
    print(f"[GLUE] wait table timeout: {db}.{table}")
    return False


# ---------------- 业务 SQL ----------------
def ensure_summary_table():
    """若不存在 summary 表，则用 CTAS 创建（Athena v3 语法）"""
    try:
        glue.get_table(DatabaseName=DB_NAME, Name=SUMMARY_TABLE)
        print(f"[SUMMARY] exists {DB_NAME}.{SUMMARY_TABLE}")
        return
    except glue.exceptions.EntityNotFoundException:
        print("[SUMMARY] not exists, CTAS ...")

    assert SUMMARY_S3 and SUMMARY_S3.startswith("s3://"), "SUMMARY_S3 must be an s3 URI"
    out_path = SUMMARY_S3 if SUMMARY_S3.endswith("/") else SUMMARY_S3 + "/"

    ctas = f"""
    CREATE TABLE "{SUMMARY_TABLE}"
    WITH (
      format = 'PARQUET',
      external_location = '{out_path}',
      partitioned_by = ARRAY['ingest_date']
    ) AS
    SELECT
      make,
      country,
      CAST(AVG(price) AS DOUBLE) AS avg_price,
      DATE('{TODAY}') AS ingest_date
    FROM "{SRC_TABLE}"
    GROUP BY make, country
    """
    run_athena(ctas)
    print(f"[SUMMARY] CTAS created at {out_path}")


def drop_and_purge_today_partition():
    """幂等：删除当天分区元数据（v3 语法，无 IF EXISTS）+ 清理 S3 物理文件。"""
    # 1) 删 Glue Catalog 分区（分区不存在会抛错，吞掉即可）
    try:
        drop_sql = (
            f'ALTER TABLE "{SUMMARY_TABLE}" '
            f"DROP PARTITION (ingest_date = DATE '{TODAY}')"
        )
        run_athena(drop_sql)
        print(f"[SUMMARY] dropped partition {TODAY}")
    except Exception as e:
        print(f"[SUMMARY] drop partition warn: {e}")

    # 2) 清理 S3 物理前缀
    if SUMMARY_S3 and SUMMARY_S3.startswith("s3://"):
        bucket = SUMMARY_S3.replace("s3://", "").split("/")[0]
        # e.g. summary/ingest_date=YYYY-MM-DD/
        key_prefix_root = SUMMARY_S3.replace(f"s3://{bucket}/", "").rstrip("/")
        prefix = f"{key_prefix_root}/ingest_date={TODAY}/"
        print(f"[SUMMARY] purge s3://{bucket}/{prefix}")

        paginator = s3.get_paginator("list_objects_v2")
        to_delete = {"Objects": []}
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                to_delete["Objects"].append({"Key": obj["Key"]})
                if len(to_delete["Objects"]) == 1000:
                    s3.delete_objects(Bucket=bucket, Delete=to_delete)
                    to_delete = {"Objects": []}
        if to_delete["Objects"]:
            s3.delete_objects(Bucket=bucket, Delete=to_delete)
    else:
        print("[SUMMARY] skip purge: SUMMARY_S3 not configured correctly")


def insert_today_snapshot():
    """把今天的汇总快照写入分区表（需要 Glue 分区写权限）"""
    sql = f"""
    INSERT INTO "{SUMMARY_TABLE}"
    SELECT
      make,
      country,
      CAST(AVG(price) AS DOUBLE) AS avg_price,
      DATE('{TODAY}') AS ingest_date
    FROM "{SRC_TABLE}"
    GROUP BY make, country
    """
    run_athena(sql)
    print("[SUMMARY] inserted today")


def detect_and_alert():
    """对比今天 vs 最近一天；超过阈值通过 SNS 告警"""
    q_prev = f"""
    SELECT MAX(ingest_date) AS prev_day
    FROM "{SUMMARY_TABLE}"
    WHERE ingest_date < DATE('{TODAY}')
    """
    _, qid = run_athena(q_prev)
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    if len(rows) <= 1 or not rows[1]["Data"][0].get("VarCharValue"):
        print("[ALERT] no previous partition, skip")
        return
    prev_day = rows[1]["Data"][0]["VarCharValue"]

    q_cmp = f"""
    WITH cur AS (
      SELECT make,country,avg_price FROM "{SUMMARY_TABLE}" WHERE ingest_date = DATE('{TODAY}')
    ), prev AS (
      SELECT make,country,avg_price AS prev_avg_price FROM "{SUMMARY_TABLE}" WHERE ingest_date = DATE('{prev_day}')
    )
    SELECT
      COALESCE(cur.make, prev.make)     AS make,
      COALESCE(cur.country, prev.country) AS country,
      cur.avg_price,
      prev.prev_avg_price,
      CASE
        WHEN prev.prev_avg_price IS NULL OR prev.prev_avg_price = 0 THEN NULL
        ELSE ROUND((cur.avg_price - prev.prev_avg_price)/prev.prev_avg_price*100, 2)
      END AS pct_change
    FROM cur
    FULL OUTER JOIN prev
      ON cur.make=prev.make AND cur.country=prev.country
    """
    _, qid2 = run_athena(q_cmp)
    rs  = athena.get_query_results(QueryExecutionId=qid2)["ResultSet"]["Rows"]
    if len(rs) <= 1:
        print("[ALERT] empty compare result")
        return

    hdr = [c["VarCharValue"] for c in rs[0]["Data"]]
    def row2dict(r):
        return {hdr[i]: r["Data"][i].get("VarCharValue") for i in range(len(hdr))}

    lines = []
    for r in rs[1:]:
        d = row2dict(r)
        try:
            pc = d.get("pct_change")
            if pc is not None and abs(float(pc)) >= ALERT_THRESHOLD_PCT:
                lines.append(f"{d['make']} | {d['country']} | today={d['avg_price']} | prev={d['prev_avg_price']} | Δ%={d['pct_change']}")
        except Exception:
            pass

    if not lines:
        print(f"[ALERT] no change ≥ {ALERT_THRESHOLD_PCT}%")
        return

    _publish(f"[AutoPrice Trend] {TODAY} change ≥ {ALERT_THRESHOLD_PCT}% ({len(lines)} affected)",
             "Significant price change:\n" + "\n".join(lines))


# ---------------- Handler ----------------
def lambda_handler(event, context):
    # 允许 Console Test 无 S3 事件
    try:
        rec = event["Records"][0]["s3"]
        bucket = rec["bucket"]["name"]
        key    = unquote_plus(rec["object"]["key"])
        print(f"🪣 New file uploaded: s3://{bucket}/{key}")
    except Exception:
        bucket = key = None
        print("[WARN] No S3 record; console test mode")

    # 只处理 CSV
    if key and not key.lower().endswith(".csv"):
        print("Skip: not a CSV")
        return {"statusCode": 200, "body": json.dumps({"message": "skipped"})}

    # 1) 触发 Crawler
    if CRAWLER:
        try:
            glue.start_crawler(Name=CRAWLER)
            print(f"Glue crawler started: {CRAWLER}")
        except glue.exceptions.CrawlerRunningException:
            print(f"Glue crawler already running: {CRAWLER}")
        except botocore.exceptions.ClientError as e:
            err = e.response["Error"]
            print("[GLUE ERROR]", err["Code"], err["Message"])
            _publish("[AutoPrice] Glue Error", f"{err['Code']}: {err['Message']}")

    # 2) 确保 DB 存在，并等待源表注册
    ensure_database_glue()
    if not wait_glue_table(DB_NAME, SRC_TABLE, 120):
        raise RuntimeError(f"Source table not ready: {DB_NAME}.{SRC_TABLE}")

    # 3) 首次建表 / 幂等清理 / 日增快照 / 对比预警
    try:
        ensure_summary_table()
        drop_and_purge_today_partition()   # v3 语法，无 IF EXISTS；并清理 S3 目录
        insert_today_snapshot()
        detect_and_alert()
    except Exception as e:
        print("[SUMMARY] error:", repr(e))
        _publish("[AutoPrice] Summary Build Error", str(e))
        raise

    print("✅ Done")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "bucket": bucket, "key": key})}

