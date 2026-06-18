#!/usr/bin/env python3
import os
import re
import sys
import shutil
import signal
import subprocess
import tempfile
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

import boto3
import yaml
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("ch-backup")

REQUIRED_VARS = [
    "CLICKHOUSE_HOST",
    "S3_BUCKET",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
]


def get_env() -> dict:
    missing = [v for v in REQUIRED_VARS if not os.environ.get(v)]
    if missing:
        log.error("Отсутствуют обязательные переменные окружения: %s", ", ".join(missing))
        sys.exit(1)

    return {
        "ch_host":      os.environ["CLICKHOUSE_HOST"],
        "ch_tcp_port":  int(os.environ.get("CLICKHOUSE_TCP_PORT", "9000")),
        "ch_user":      os.environ.get("CLICKHOUSE_USER", "default"),
        "ch_password":  os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "ch_database":  os.environ.get("CLICKHOUSE_DATABASE", "ALL"),
        "ch_data_path": os.environ.get("CLICKHOUSE_DATA_PATH", "/var/lib/clickhouse"),
        "ch_secure":    os.environ.get("CLICKHOUSE_SECURE", "false").lower() == "true",
        "s3_bucket":           os.environ["S3_BUCKET"],
        "s3_access_key":       os.environ["S3_ACCESS_KEY"],
        "s3_secret_key":       os.environ["S3_SECRET_KEY"],
        "s3_region":           os.environ.get("S3_REGION", "us-east-1"),
        "s3_endpoint":         os.environ.get("S3_ENDPOINT", ""),
        "s3_path_prefix":      os.environ.get("S3_PATH_PREFIX", "clickhouse-backups"),
        "s3_force_path_style": os.environ.get("S3_FORCE_PATH_STYLE", "false").lower() == "true",
        "s3_disable_ssl":      os.environ.get("S3_DISABLE_SSL", "false").lower() == "true",
        "cb_binary": os.environ.get("CLICKHOUSE_BACKUP_BINARY", "clickhouse-backup"),
        "dry_run":        os.environ.get("DRY_RUN", "false").lower() == "true",
        "retention_days": int(os.environ.get("BACKUP_RETENTION_DAYS", "30")),
        "backup_timeout": int(os.environ.get("BACKUP_TIMEOUT_SECONDS", "3600")),
    }


def build_backup_name(cfg: dict) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    db_label = cfg["ch_database"].lower().replace(" ", "_")
    return f"{db_label}_{ts}"


def _build_config(cfg: dict) -> dict:
    s3_cfg: dict[str, Any] = {
        "access_key":        cfg["s3_access_key"],
        "secret_key":        cfg["s3_secret_key"],
        "bucket":            cfg["s3_bucket"],
        "region":            cfg["s3_region"],
        "path":              cfg["s3_path_prefix"].strip("/"),
        "disable_ssl":       cfg["s3_disable_ssl"],
        "force_path_style":  cfg["s3_force_path_style"],
        "compression_format": os.environ.get("S3_COMPRESSION_FORMAT", "tar"),
        "compression_level":  int(os.environ.get("S3_COMPRESSION_LEVEL", "1")),
        "concurrency":        int(os.environ.get("S3_CONCURRENCY", "2")),
        "part_size":          512 * 1024 * 1024,
        "overwrite":          False,
    }

    if cfg["s3_endpoint"]:
        s3_cfg["endpoint"] = cfg["s3_endpoint"].rstrip("/")

    return {
        "general": {
            "remote_storage":         "s3",
            "disable_progress_bar":   False,
            "backups_to_keep_local":  1,
            "backups_to_keep_remote": 0,
        },
        "clickhouse": {
            "username":               cfg["ch_user"],
            "password":               cfg["ch_password"],
            "host":                   cfg["ch_host"],
            "port":                   cfg["ch_tcp_port"],
            "data_path":              cfg["ch_data_path"],
            "secure":                 cfg["ch_secure"],
            "skip_verify":            False,
            "sync_replicated_tables": True,
            "skip_tables": [
                "system.*",
                "information_schema.*",
                "INFORMATION_SCHEMA.*",
            ],
            "timeout": "5m",
        },
        "s3": s3_cfg,
    }


def write_config(cfg: dict) -> str:
    config_dict = _build_config(cfg)

    fd, path = tempfile.mkstemp(prefix="ch_backup_config_", suffix=".yml")
    try:
        with os.fdopen(fd, "w") as f:
            yaml.dump(config_dict, f, default_flow_style=False, allow_unicode=True)
    except Exception:
        os.unlink(path)
        raise

    return path


def run_backup(cfg: dict, backup_name: str, config_path: str) -> subprocess.Popen:
    binary = cfg["cb_binary"]

    if not shutil.which(binary):
        log.error(
            "Бинарник '%s' не найден. Установите clickhouse-backup v1.x:\n"
            "  https://github.com/Altinity/clickhouse-backup/releases",
            binary,
        )
        sys.exit(1)

    cmd = [binary, "create_remote", "--config", config_path]

    if cfg["ch_database"].upper() != "ALL":
        cmd += ["--tables", f"{cfg['ch_database']}.*"]

    cmd.append(backup_name)

    log.info("Запускаем: %s", " ".join(cmd))

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    log.info("Процесс запущен, PID=%d", proc.pid)
    return proc


def wait_backup_status(proc: subprocess.Popen, backup_name: str, cfg: dict) -> bool:
    log.info(
        "Мониторим выполнение backup_name=%s (таймаут=%ds)...",
        backup_name,
        cfg["backup_timeout"],
    )

    timeout = cfg["backup_timeout"]
    proc_stdout = proc.stdout

    try:
        for line in proc_stdout:
            log.info("[clickhouse-backup] %s", line.rstrip())

        proc.wait(timeout=timeout)

    except subprocess.TimeoutExpired:
        log.error(
            "Таймаут %ds истёк. Принудительно завершаем процесс PID=%d.",
            timeout,
            proc.pid,
        )
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        return False

    if proc.returncode == 0:
        log.info("clickhouse-backup завершился успешно (exit code=0)")
        return True

    log.error(
        "clickhouse-backup завершился с ошибкой (exit code=%d)",
        proc.returncode,
    )
    return False


_BACKUP_TS_RE = re.compile(r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})$")


def _parse_backup_timestamp(name: str) -> Optional[datetime]:
    match = _BACKUP_TS_RE.search(name)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d_%H-%M-%S").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None


def _delete_s3_prefix(s3_client: Any, bucket: str, prefix: str) -> int:
    paginator = s3_client.get_paginator("list_objects_v2")
    deleted_total = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get("Contents", [])
        if not objects:
            continue

        batch = {"Objects": [{"Key": obj["Key"]} for obj in objects]}
        response = s3_client.delete_objects(Bucket=bucket, Delete=batch)

        deleted_total += len(response.get("Deleted", []))
        for err in response.get("Errors", []):
            log.warning("Ошибка удаления '%s': %s", err.get("Key"), err.get("Message"))

    return deleted_total


def cleanup_old_backups(cfg: dict) -> None:
    retention = cfg["retention_days"]
    if retention <= 0:
        log.info("Retention отключён (BACKUP_RETENTION_DAYS=%d), пропускаем очистку.", retention)
        return

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention)
    parent_prefix = cfg["s3_path_prefix"].strip("/") + "/"

    log.info(
        "Ищем бэкапы старше %d дней (до %s UTC) в s3://%s/%s",
        retention,
        cutoff.strftime("%Y-%m-%d"),
        cfg["s3_bucket"],
        parent_prefix,
    )

    if cfg["dry_run"]:
        log.info("[DRY-RUN] Очистка S3 не выполняется.")
        return

    s3_kwargs: dict[str, Any] = {
        "aws_access_key_id":     cfg["s3_access_key"],
        "aws_secret_access_key": cfg["s3_secret_key"],
        "region_name":           cfg["s3_region"],
    }
    if cfg["s3_endpoint"]:
        s3_kwargs["endpoint_url"] = cfg["s3_endpoint"].rstrip("/")

    s3 = boto3.client("s3", **s3_kwargs)

    paginator = s3.get_paginator("list_objects_v2")
    backup_prefixes: list[str] = []
    for page in paginator.paginate(
        Bucket=cfg["s3_bucket"], Prefix=parent_prefix, Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes", []):
            backup_prefixes.append(cp["Prefix"])

    if not backup_prefixes:
        log.info("В s3://%s/%s нет бэкапов для очистки.", cfg["s3_bucket"], parent_prefix)
        return

    deleted_count = 0
    for bp in backup_prefixes:
        backup_dir = bp.rstrip("/").split("/")[-1]
        backup_dt = _parse_backup_timestamp(backup_dir)

        if backup_dt is None:
            log.warning("Не удалось распарсить дату из '%s', пропускаем.", backup_dir)
            continue

        if backup_dt >= cutoff:
            log.debug("Оставляем бэкап: %s (%s)", backup_dir, backup_dt.date())
            continue

        log.info(
            "Удаляем устаревший бэкап: %s (создан %s UTC)",
            backup_dir,
            backup_dt.strftime("%Y-%m-%d"),
        )
        removed = _delete_s3_prefix(s3, cfg["s3_bucket"], bp)
        log.info("  → Удалено объектов: %d", removed)
        deleted_count += 1

    log.info("Очистка завершена. Удалено бэкапов: %d", deleted_count)


def main() -> None:
    log.info("=== ClickHouse Backup Script started ===")

    cfg = get_env()
    backup_name = build_backup_name(cfg)

    log.info(
        "База: %s  |  Имя бэкапа: %s  |  S3: s3://%s/%s/%s",
        cfg["ch_database"],
        backup_name,
        cfg["s3_bucket"],
        cfg["s3_path_prefix"],
        backup_name,
    )

    if cfg["dry_run"]:
        tables_flag = (
            ""
            if cfg["ch_database"].upper() == "ALL"
            else f" --tables {cfg['ch_database']}.*"
        )
        log.info(
            "[DRY-RUN] Команда: %s create_remote --config <config.yml>%s %s",
            cfg["cb_binary"],
            tables_flag,
            backup_name,
        )
        log.info("[DRY-RUN] S3 path: s3://%s/%s/%s/", cfg["s3_bucket"], cfg["s3_path_prefix"], backup_name)
        log.info("[DRY-RUN] Бэкап не запущен.")
        cleanup_old_backups(cfg)
        sys.exit(0)

    config_path: Optional[str] = None
    proc: Optional[subprocess.Popen] = None
    success = False

    try:
        config_path = write_config(cfg)
        log.debug("Временный конфиг: %s", config_path)

        proc = run_backup(cfg, backup_name, config_path)
        success = wait_backup_status(proc, backup_name, cfg)

    except Exception as exc:
        log.exception("Неожиданная ошибка во время бэкапа: %s", exc)
        if proc and proc.poll() is None:
            proc.kill()
        sys.exit(1)

    finally:
        if config_path and os.path.exists(config_path):
            os.unlink(config_path)
            log.debug("Временный конфиг удалён.")

    if not success:
        log.error("Бэкап завершился неуспешно.")
        sys.exit(1)

    try:
        cleanup_old_backups(cfg)
    except Exception as exc:
        log.warning("Ошибка при очистке старых бэкапов: %s", exc)

    log.info("=== Бэкап успешно завершён: %s ===", backup_name)
    sys.exit(0)


if __name__ == "__main__":
    main()
