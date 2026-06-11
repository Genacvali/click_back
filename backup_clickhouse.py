#!/usr/bin/env python3
"""
ClickHouse → S3 Backup Script
Аналог Percona Backup for MongoDB, но для ClickHouse.
Использует нативную SQL-команду ASYNC BACKUP DATABASE TO S3(...)
с отслеживанием статуса через system.backups по backup_id.
"""

import os
import re
import sys
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

import boto3
from botocore.exceptions import ClientError

try:
    import clickhouse_connect
except ImportError:
    clickhouse_connect = None

try:
    from clickhouse_driver import Client as TCPClient
except ImportError:
    TCPClient = None

# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("ch-backup")


# ---------------------------------------------------------------------------
# 1. Получение конфигурации из переменных окружения
# ---------------------------------------------------------------------------

REQUIRED_VARS = [
    "CLICKHOUSE_HOST",
    "S3_ENDPOINT",
    "S3_BUCKET",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
]


def _normalize_endpoint(url: str) -> str:
    """Добавляет https:// если схема не указана (совместимо с форматом Percona/OBS)."""
    if url and not url.startswith(("http://", "https://")):
        return "https://" + url
    return url


def get_env() -> dict:
    """Читает и валидирует конфигурацию из переменных окружения."""
    missing = [v for v in REQUIRED_VARS if not os.environ.get(v)]
    if missing:
        log.error("Отсутствуют обязательные переменные окружения: %s", ", ".join(missing))
        sys.exit(1)

    return {
        # ClickHouse
        "ch_host":     os.environ["CLICKHOUSE_HOST"],
        "ch_port":     int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        "ch_user":     os.environ.get("CLICKHOUSE_USER", "default"),
        "ch_password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "ch_database": os.environ.get("CLICKHOUSE_DATABASE", "default"),
        # "auto" — выбрать автоматически; "connect" — HTTP; "driver" — TCP
        "ch_client":   os.environ.get("CLICKHOUSE_CLIENT", "auto"),
        # S3 — добавляем https:// если схема не указана (как в конфиге Percona)
        "s3_endpoint":    _normalize_endpoint(os.environ["S3_ENDPOINT"]),
        "s3_bucket":      os.environ["S3_BUCKET"],
        "s3_access_key":  os.environ["S3_ACCESS_KEY"],
        "s3_secret_key":  os.environ["S3_SECRET_KEY"],
        "s3_path_prefix": os.environ.get("S3_PATH_PREFIX", "clickhouse-backups"),
        # Поведение
        "dry_run":        os.environ.get("DRY_RUN", "false").lower() == "true",
        "retention_days": int(os.environ.get("BACKUP_RETENTION_DAYS", "30")),
        # Таймауты
        "backup_timeout": int(os.environ.get("BACKUP_TIMEOUT_SECONDS", "3600")),
        "poll_interval":  int(os.environ.get("BACKUP_POLL_INTERVAL_SECONDS", "10")),
    }


# ---------------------------------------------------------------------------
# 2. Формирование имени и пути бэкапа
# ---------------------------------------------------------------------------

def build_backup_name(cfg: dict) -> tuple[str, str]:
    """
    Возвращает (backup_name, s3_url).

    backup_name: metrics_db_2026-06-08_02-30-00
    s3_url:      https://endpoint/bucket/prefix/metrics_db_2026-06-08_02-30-00

    ВАЖНО: расширение .zip НЕ добавляется.
    При бэкапе в S3 ClickHouse хранит набор объектов под указанным префиксом
    (несколько файлов: .backup, data/...), а не единый zip-архив.
    Zip используется только для File-бэкендов (BACKUP TO File(...)).
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    db_label = cfg["ch_database"].lower().replace(" ", "_")
    backup_name = f"{db_label}_{ts}"

    s3_url = "{endpoint}/{bucket}/{prefix}/{name}".format(
        endpoint=cfg["s3_endpoint"].rstrip("/"),
        bucket=cfg["s3_bucket"],
        prefix=cfg["s3_path_prefix"].strip("/"),
        name=backup_name,
    )
    return backup_name, s3_url


# ---------------------------------------------------------------------------
# 3. Подключение к ClickHouse
# ---------------------------------------------------------------------------

class ClickHouseConnection:
    """
    Унифицированная обёртка над clickhouse-connect (HTTP) и clickhouse-driver (TCP).
    Скрывает различия в API обоих клиентов.
    """

    def __init__(self, client: Any, driver: str) -> None:
        self._client = client
        self._driver = driver  # "connect" или "driver"

    def execute(self, query: str) -> list[tuple]:
        """Выполняет SQL и возвращает строки как list of tuples."""
        if self._driver == "connect":
            return self._client.query(query).result_rows
        return self._client.execute(query)

    def close(self) -> None:
        """Закрывает соединение без выброса исключений."""
        try:
            if self._driver == "connect":
                self._client.close()
            else:
                self._client.disconnect()
        except Exception:
            pass

    def __repr__(self) -> str:
        return f"ClickHouseConnection(driver={self._driver!r})"


def _get_connection(cfg: dict) -> ClickHouseConnection:
    """
    Создаёт соединение с ClickHouse.
    Приоритет: clickhouse-connect (HTTP) → clickhouse-driver (TCP).
    Переопределяется через CLICKHOUSE_CLIENT=connect|driver|auto.
    """
    prefer = cfg["ch_client"]

    if prefer != "driver" and clickhouse_connect is not None:
        client = clickhouse_connect.get_client(
            host=cfg["ch_host"],
            port=cfg["ch_port"],
            username=cfg["ch_user"],
            password=cfg["ch_password"],
            connect_timeout=30,
            # max_execution_time передаётся как настройка ClickHouse сессии
            settings={"max_execution_time": cfg["backup_timeout"]},
        )
        log.info("Клиент: clickhouse-connect (HTTP, порт=%d)", cfg["ch_port"])
        return ClickHouseConnection(client, "connect")

    if prefer != "connect" and TCPClient is not None:
        # HTTP-порт 8123 по умолчанию заменяем на TCP 9000; иначе берём указанный порт
        tcp_port = cfg["ch_port"] if cfg["ch_port"] not in (8123, 80, 443) else 9000
        client = TCPClient(
            host=cfg["ch_host"],
            port=tcp_port,
            user=cfg["ch_user"],
            password=cfg["ch_password"],
            connect_timeout=30,
            send_receive_timeout=cfg["backup_timeout"],
        )
        log.info("Клиент: clickhouse-driver (TCP, порт=%d)", tcp_port)
        return ClickHouseConnection(client, "driver")

    log.error(
        "Не найден ни clickhouse-connect, ни clickhouse-driver. "
        "Установите пакет: pip install clickhouse-connect"
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# 4. Запуск ASYNC BACKUP
# ---------------------------------------------------------------------------

def run_backup(cfg: dict, s3_url: str) -> tuple[ClickHouseConnection, str]:
    """
    Выполняет ASYNC BACKUP и возвращает (conn, backup_id).
    Секретный ключ в лог не попадает.
    """
    db = cfg["ch_database"].upper()
    subject = "ALL" if db == "ALL" else f"DATABASE `{cfg['ch_database']}`"

    sql_safe = (
        f"BACKUP {subject} "
        f"TO S3('{s3_url}', '{cfg['s3_access_key']}', '<***>') "
        f"ASYNC"
    )
    sql_real = (
        f"BACKUP {subject} "
        f"TO S3('{s3_url}', '{cfg['s3_access_key']}', '{cfg['s3_secret_key']}') "
        f"ASYNC"
    )

    log.info("SQL (секрет скрыт): %s", sql_safe)

    conn = _get_connection(cfg)
    rows = conn.execute(sql_real)

    # BACKUP ... ASYNC возвращает одну строку: (id UUID, status String)
    if not rows:
        log.error(
            "BACKUP не вернул backup_id. "
            "Требуется ClickHouse >= 22.4 и право BACKUP для пользователя."
        )
        conn.close()
        sys.exit(1)

    backup_id = str(rows[0][0])
    log.info("ASYNC BACKUP запущен, backup_id=%s", backup_id)
    return conn, backup_id


# ---------------------------------------------------------------------------
# 5. Ожидание завершения бэкапа через system.backups
# ---------------------------------------------------------------------------

# ИСПРАВЛЕНО: ClickHouse использует "BACKUP_FAILED", а не просто "FAILED"
TERMINAL_STATUSES = {"BACKUP_CREATED", "BACKUP_FAILED", "ABORTED"}


def wait_backup_status(conn: ClickHouseConnection, backup_id: str, cfg: dict) -> str:
    """
    Polling system.backups по backup_id до терминального статуса.
    Логирует прогресс: количество файлов и сжатый размер.
    Возвращает финальный статус или завершает процесс с кодом 1 по таймауту.
    """
    deadline = time.monotonic() + cfg["backup_timeout"]
    interval = cfg["poll_interval"]

    log.info(
        "Ожидаем завершения бэкапа (таймаут=%ds, опрос каждые %ds)...",
        cfg["backup_timeout"],
        interval,
    )

    while time.monotonic() < deadline:
        # Ждём перед первым опросом — ASYNC BACKUP нужно время для старта
        time.sleep(interval)

        try:
            rows = conn.execute(
                f"SELECT status, error, num_files, formatReadableSize(compressed_size) "
                f"FROM system.backups WHERE id = '{backup_id}'"
            )
        except Exception as exc:
            log.warning("Ошибка при опросе system.backups: %s", exc)
            continue

        if not rows:
            log.warning("backup_id=%s ещё не найден в system.backups, ждём...", backup_id)
            continue

        status, error, num_files, compressed_size = rows[0]
        log.info(
            "backup_id=%s  статус=%-20s  файлов=%s  размер=%s",
            backup_id, status, num_files, compressed_size,
        )

        if status in TERMINAL_STATUSES:
            if status != "BACKUP_CREATED":
                log.error(
                    "Бэкап завершился с ошибкой: статус=%s, ошибка=%s",
                    status, error,
                )
            else:
                log.info("Бэкап успешно создан: файлов=%s, размер=%s", num_files, compressed_size)
            return status

    log.error(
        "Таймаут %ds истёк. Бэкап backup_id=%s так и не завершился.",
        cfg["backup_timeout"],
        backup_id,
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# 6. Удаление старых бэкапов из S3 (retention)
# ---------------------------------------------------------------------------

# Шаблон для парсинга timestamp из имени бэкапа: "db_2026-06-08_02-30-00"
_BACKUP_TS_RE = re.compile(r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})$")


def _parse_backup_timestamp(name: str) -> Optional[datetime]:
    """Парсит timestamp из имени бэкапа вида 'db_name_2026-06-08_02-30-00'."""
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
    """
    Пакетно удаляет все S3-объекты под указанным префиксом (до 1000 за запрос).
    Возвращает суммарное количество удалённых объектов.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    deleted_total = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get("Contents", [])
        if not objects:
            continue

        # delete_objects удаляет до 1000 объектов за один API-вызов
        batch = {"Objects": [{"Key": obj["Key"]} for obj in objects]}
        response = s3_client.delete_objects(Bucket=bucket, Delete=batch)

        deleted_total += len(response.get("Deleted", []))
        for err in response.get("Errors", []):
            log.warning("Ошибка удаления '%s': %s", err.get("Key"), err.get("Message"))

    return deleted_total


def cleanup_old_backups(cfg: dict) -> None:
    """
    Удаляет бэкапы старше BACKUP_RETENTION_DAYS дней из S3.

    Логика: получаем список «директорий» бэкапов через S3 delimiter=/,
    парсим дату создания из имени директории (не из LastModified объектов),
    удаляем все объекты устаревших бэкапов пакетно через delete_objects.
    """
    retention = cfg["retention_days"]
    if retention <= 0:
        log.info("Retention отключён (BACKUP_RETENTION_DAYS=%d), пропускаем очистку.", retention)
        return

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention)
    parent_prefix = cfg["s3_path_prefix"].strip("/") + "/"

    log.info(
        "Поиск бэкапов старше %d дней (до %s UTC) в s3://%s/%s",
        retention,
        cutoff.strftime("%Y-%m-%d"),
        cfg["s3_bucket"],
        parent_prefix,
    )

    if cfg["dry_run"]:
        log.info("[DRY-RUN] Очистка S3 не выполняется.")
        return

    s3 = boto3.client(
        "s3",
        endpoint_url=cfg["s3_endpoint"],
        aws_access_key_id=cfg["s3_access_key"],
        aws_secret_access_key=cfg["s3_secret_key"],
    )

    # Получаем список «директорий» бэкапов через delimiter
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

    deleted_backups = 0
    for bp in backup_prefixes:
        # bp: "clickhouse-backups/metrics_db_2026-06-08_02-30-00/"
        backup_dir = bp.rstrip("/").split("/")[-1]
        backup_dt = _parse_backup_timestamp(backup_dir)

        if backup_dt is None:
            log.warning("Не удалось распарсить дату из '%s', пропускаем.", backup_dir)
            continue

        if backup_dt >= cutoff:
            continue

        log.info(
            "Удаляем устаревший бэкап: %s (создан %s UTC)",
            backup_dir,
            backup_dt.strftime("%Y-%m-%d"),
        )
        count = _delete_s3_prefix(s3, cfg["s3_bucket"], bp)
        log.info("  → Удалено объектов: %d", count)
        deleted_backups += 1

    log.info("Очистка завершена. Удалено бэкапов: %d", deleted_backups)


# ---------------------------------------------------------------------------
# 7. Точка входа
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("=== ClickHouse Backup Script started ===")

    cfg = get_env()
    backup_name, s3_url = build_backup_name(cfg)

    log.info(
        "База: %s  |  Имя бэкапа: %s",
        cfg["ch_database"],
        backup_name,
    )

    # Dry-run: показать что будет сделано и выйти, не подключаясь к CH
    if cfg["dry_run"]:
        db = cfg["ch_database"].upper()
        subject = "ALL" if db == "ALL" else f"DATABASE `{cfg['ch_database']}`"
        log.info(
            "[DRY-RUN] SQL: BACKUP %s TO S3('%s', '%s', '<***>') ASYNC",
            subject,
            s3_url,
            cfg["s3_access_key"],
        )
        log.info("[DRY-RUN] Бэкап не запущен.")
        cleanup_old_backups(cfg)
        sys.exit(0)

    conn: Optional[ClickHouseConnection] = None
    final_status: Optional[str] = None

    try:
        conn, backup_id = run_backup(cfg, s3_url)
        final_status = wait_backup_status(conn, backup_id, cfg)
    except Exception as exc:
        log.exception("Неожиданная ошибка во время бэкапа: %s", exc)
        sys.exit(1)
    finally:
        if conn is not None:
            conn.close()

    if final_status != "BACKUP_CREATED":
        log.error("Бэкап завершился неуспешно, статус: %s", final_status)
        sys.exit(1)

    # Retention запускается только после успешного бэкапа
    try:
        cleanup_old_backups(cfg)
    except Exception as exc:
        # Ошибка очистки не должна ломать exit code успешного бэкапа
        log.warning("Ошибка при очистке старых бэкапов: %s", exc)

    log.info("=== Бэкап успешно завершён ===")
    sys.exit(0)


if __name__ == "__main__":
    main()
