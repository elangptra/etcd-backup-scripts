#!/usr/bin/env python3
"""
ETCD Backup Service
====================
Automated, production-ready ETCD snapshot backup for a single control plane
Kubernetes cluster.

Alur:
    load config -> lock -> health check -> quorum check -> snapshot save
    -> validate -> compress -> checksum -> local retention -> copy to NFS
    -> NFS retention -> write metrics -> exit

Exit codes (sesuai PRD):
    0 Success
    1 Config Error
    2 Health Check Failed
    3 Quorum Lost
    4 Snapshot Failed
    5 Snapshot Invalid
    6 Compression Failed
    7 NFS Failed
    8 Unexpected Error
"""

import os
import sys
import json
import time
import gzip
import shutil
import hashlib
import logging
import fcntl
import subprocess
import errno
from datetime import datetime, timezone
from pathlib import Path

try:
    import yaml
except ImportError:
    print("Missing dependency: PyYAML. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(8)


class ExitCode:
    SUCCESS = 0
    CONFIG_ERROR = 1
    HEALTH_CHECK_FAILED = 2
    QUORUM_LOST = 3
    SNAPSHOT_FAILED = 4
    SNAPSHOT_INVALID = 5
    COMPRESSION_FAILED = 6
    NFS_FAILED = 7
    UNEXPECTED_ERROR = 8


class BackupError(Exception):
    """Error terkendali yang membawa exit code spesifik."""
    def __init__(self, message, exit_code):
        super().__init__(message)
        self.exit_code = exit_code


# -----------------------------------------------------------------------
# JSON structured logging
# -----------------------------------------------------------------------
class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
        }
        # Field tambahan yang dikirim lewat extra={...}
        for key in ("cluster", "endpoint", "action", "status", "duration", "size"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(cfg):
    log_cfg = cfg.get("logging", {})
    log_file = log_cfg.get("file", "/var/log/etcd-backup/etcd-backup.log")
    level_name = log_cfg.get("level", "INFO")
    level = getattr(logging, level_name.upper(), logging.INFO)

    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("etcd-backup")
    logger.setLevel(level)
    logger.handlers.clear()

    formatter = JsonFormatter()

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def load_config(config_path):
    try:
        with open(config_path, "r") as f:
            cfg = yaml.safe_load(f)
    except FileNotFoundError as e:
        raise BackupError(f"Config file not found: {config_path}", ExitCode.CONFIG_ERROR) from e
    except yaml.YAMLError as e:
        raise BackupError(f"Invalid YAML config: {e}", ExitCode.CONFIG_ERROR) from e

    if not cfg:
        raise BackupError("Config file is empty", ExitCode.CONFIG_ERROR)

    required_sections = ["cluster", "backup", "storage", "etcd", "lock", "logging"]
    for section in required_sections:
        if section not in cfg:
            raise BackupError(f"Missing config section: {section}", ExitCode.CONFIG_ERROR)

    endpoints = cfg["etcd"].get("endpoints") or []
    if not endpoints:
        raise BackupError("etcd.endpoints must contain at least one endpoint", ExitCode.CONFIG_ERROR)

    return cfg


# -----------------------------------------------------------------------
# Lock mechanism - hanya boleh ada satu proses backup berjalan
# -----------------------------------------------------------------------
class ProcessLock:
    def __init__(self, lock_path, logger):
        self.lock_path = lock_path
        self.logger = logger
        self._fd = None

    def acquire(self):
        Path(self.lock_path).parent.mkdir(parents=True, exist_ok=True)
        self._fd = open(self.lock_path, "w")
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            if e.errno in (errno.EACCES, errno.EAGAIN):
                raise BackupError(
                    f"Another backup process is already running (lock: {self.lock_path})",
                    ExitCode.UNEXPECTED_ERROR,
                ) from e
            raise
        self._fd.write(str(os.getpid()))
        self._fd.flush()
        self.logger.info("Lock acquired", extra={"action": "lock_acquire", "status": "ok"})

    def release(self):
        if self._fd:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                self._fd.close()
                Path(self.lock_path).unlink(missing_ok=True)
                self.logger.info("Lock released", extra={"action": "lock_release", "status": "ok"})
            except Exception:
                self.logger.warning("Failed to cleanly release lock", extra={"action": "lock_release", "status": "warning"})


# -----------------------------------------------------------------------
# crictl exec helper - menjalankan etcdctl DI DALAM container etcd,
# bukan lewat binary etcdctl di host (host tidak install etcdctl).
# -----------------------------------------------------------------------
def find_etcd_container_id(cfg, logger):
    """
    Cari container ID etcd yang sedang Running lewat crictl.
    Nama container static pod kubeadm untuk etcd adalah 'etcd' persis.
    """
    etcd_cfg = cfg["etcd"]
    crictl_path = etcd_cfg.get("crictl_path", "crictl")
    container_name = etcd_cfg.get("container_name", "etcd")
    timeout = etcd_cfg.get("command_timeout_seconds", 30)

    args = [crictl_path, "ps", "--name", f"^{container_name}$", "--state", "Running", "-q"]
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        raise BackupError("crictl ps timed out while locating etcd container", ExitCode.UNEXPECTED_ERROR) from e
    except FileNotFoundError as e:
        raise BackupError(f"crictl binary not found: {crictl_path}", ExitCode.UNEXPECTED_ERROR) from e

    if result.returncode != 0:
        raise BackupError(f"crictl ps failed: {result.stderr.strip()}", ExitCode.UNEXPECTED_ERROR)

    container_ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not container_ids:
        raise BackupError(
            f"No running container found matching name '{container_name}' (is etcd static pod healthy?)",
            ExitCode.HEALTH_CHECK_FAILED,
        )
    if len(container_ids) > 1:
        logger.warning(
            f"Multiple containers matched name '{container_name}', using the first one: {container_ids[0]}",
            extra={"action": "find_etcd_container", "status": "warning"},
        )

    container_id = container_ids[0]
    logger.info(
        f"Found etcd container: {container_id}",
        extra={"action": "find_etcd_container", "status": "ok"},
    )
    return container_id


def etcdctl_base_args(etcd_cfg, endpoint):
    return [
        etcd_cfg.get("etcdctl_path", "etcdctl"),
        f"--endpoints={endpoint}",
        f"--cacert={etcd_cfg['ca_cert']}",
        f"--cert={etcd_cfg['cert_file']}",
        f"--key={etcd_cfg['key_file']}",
    ]


def run_etcdctl_in_container(container_id, etcdctl_args, cfg, logger, action):
    """Bungkus perintah etcdctl dengan `crictl exec <container_id> ...`."""
    etcd_cfg = cfg["etcd"]
    crictl_path = etcd_cfg.get("crictl_path", "crictl")
    timeout = etcd_cfg.get("command_timeout_seconds", 30)

    args = [crictl_path, "exec", container_id] + etcdctl_args
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return result
    except subprocess.TimeoutExpired:
        logger.error(f"{action} timed out after {timeout}s", extra={"action": action, "status": "timeout"})
        raise
    except FileNotFoundError as e:
        raise BackupError(f"crictl binary not found: {crictl_path}", ExitCode.UNEXPECTED_ERROR) from e


# -----------------------------------------------------------------------
# 5.3 / 5.4 - Health check & endpoint selection
# -----------------------------------------------------------------------
def check_endpoint_health(endpoint, container_id, cfg, logger):
    """Return status string: healthy | unhealthy | timeout | connection_failed"""
    etcd_args = etcdctl_base_args(cfg["etcd"], endpoint) + ["endpoint", "health"]

    try:
        result = run_etcdctl_in_container(container_id, etcd_args, cfg, logger, "health_check")
    except subprocess.TimeoutExpired:
        return "timeout"

    if result.returncode == 0 and "is healthy" in result.stdout.lower():
        status = "healthy"
    elif result.returncode != 0 and (
        "connection error" in result.stderr.lower() or "connection refused" in result.stderr.lower()
    ):
        status = "connection_failed"
    else:
        status = "unhealthy"

    logger.info(
        f"Health check for {endpoint}: {status}",
        extra={"action": "health_check", "endpoint": endpoint, "status": status},
    )
    return status


def select_endpoint(cfg, logger):
    """
    5.4 Endpoint selection - untuk single control plane hanya ada 1 endpoint,
    fungsi tetap mengikuti alur priority-selection agar kompatibel bila
    endpoint ditambah di masa depan (multi control plane).

    5.5 Quorum awareness (single control plane):
        healthy == 1 -> lanjut backup
        healthy == 0 -> abort (quorum lost)
    """
    endpoints = cfg["etcd"]["endpoints"]

    # Container etcd dicari sekali di awal, dipakai untuk semua endpoint
    container_id = find_etcd_container_id(cfg, logger)

    healthy_endpoints = []
    for endpoint in endpoints:
        status = check_endpoint_health(endpoint, container_id, cfg, logger)
        if status == "healthy":
            healthy_endpoints.append(endpoint)

    healthy_count = len(healthy_endpoints)
    total_count = len(endpoints)

    logger.info(
        f"Healthy members: {healthy_count}/{total_count}",
        extra={"action": "quorum_check", "status": "info"},
    )

    if healthy_count == 0:
        raise BackupError("No healthy ETCD endpoint available (quorum lost)", ExitCode.QUORUM_LOST)

    # Priority selection: ambil endpoint sehat pertama sesuai urutan config
    selected = healthy_endpoints[0]
    logger.info(
        f"Selected endpoint: {selected}",
        extra={"action": "endpoint_selection", "endpoint": selected, "status": "ok"},
    )
    return selected, container_id, healthy_count, total_count


# -----------------------------------------------------------------------
# 5.6 / 5.7 - Snapshot save & validation
#
# Karena etcdctl dijalankan DI DALAM container (crictl exec), file hasil
# snapshot harus ditulis ke path yang memang ter-mount di container -
# yaitu data_dir ETCD (hostPath, defaultnya /var/lib/etcd), karena path
# tersebut identik antara host dan container (bind mount 1:1 ala kubeadm).
# Setelah snapshot divalidasi, file dipindah keluar ke local backup dir.
# -----------------------------------------------------------------------
def snapshot_save(endpoint, container_id, cfg, logger):
    etcd_cfg = cfg["etcd"]
    cluster_name = cfg["cluster"]["name"]
    data_dir = Path(etcd_cfg.get("data_dir", "/var/lib/etcd"))
    local_dir = Path(cfg["backup"]["local_directory"])
    local_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    snapshot_name = f"{cluster_name}-{timestamp}.db"
    tmp_container_path = data_dir / f".etcd-backup-tmp-{timestamp}.db"

    etcd_args = etcdctl_base_args(etcd_cfg, endpoint) + ["snapshot", "save", str(tmp_container_path)]

    start = time.time()
    try:
        result = run_etcdctl_in_container(container_id, etcd_args, cfg, logger, "snapshot_save")
    except subprocess.TimeoutExpired as e:
        raise BackupError("Snapshot save timed out", ExitCode.SNAPSHOT_FAILED) from e
    duration = round(time.time() - start, 2)

    # tmp_container_path adalah hostPath yang sama persis di host,
    # karena itu bisa langsung dicek/dipindah dari sisi host.
    if result.returncode != 0 or not tmp_container_path.exists():
        logger.error(
            f"Snapshot save failed: {result.stderr.strip()}",
            extra={"action": "snapshot_save", "status": "failed", "duration": duration},
        )
        # bersihkan sisa file jika ada
        tmp_container_path.unlink(missing_ok=True)
        raise BackupError(f"Snapshot save failed: {result.stderr.strip()}", ExitCode.SNAPSHOT_FAILED)

    size = tmp_container_path.stat().st_size
    logger.info(
        f"Snapshot saved: {tmp_container_path} ({size} bytes)",
        extra={"action": "snapshot_save", "status": "ok", "duration": duration, "size": size},
    )
    return tmp_container_path, snapshot_name, duration


def validate_snapshot(tmp_container_path, container_id, cfg, logger):
    """
    Validasi HARUS dilakukan sebelum file dipindah keluar dari data_dir,
    karena etcdctl di dalam container hanya bisa melihat path di data_dir.

    CATATAN: sejak etcd 3.6, subcommand `etcdctl snapshot status` DIHAPUS
    dan dipindah ke tool terpisah `etcdutl`. etcdutl bekerja offline
    langsung terhadap file snapshot, jadi TIDAK butuh --cacert/--cert/--key
    /--endpoints sama sekali.
    """
    etcd_cfg = cfg["etcd"]
    etcdutl_args = [
        etcd_cfg.get("etcdutl_path", "etcdutl"),
        "snapshot", "status", str(tmp_container_path),
        "--write-out=json",
    ]

    try:
        result = run_etcdctl_in_container(container_id, etcdutl_args, cfg, logger, "snapshot_validate")
    except subprocess.TimeoutExpired as e:
        raise BackupError("Snapshot validation timed out", ExitCode.SNAPSHOT_INVALID) from e

    if result.returncode != 0:
        logger.error(
            f"Snapshot validation failed: {result.stderr.strip()}",
            extra={"action": "snapshot_validate", "status": "failed"},
        )
        raise BackupError("Snapshot failed validation (etcdutl snapshot status)", ExitCode.SNAPSHOT_INVALID)

    # Jaga-jaga bila ada noise/warning sebelum payload JSON pada build tertentu.
    # Ambil substring mulai dari '{' pertama agar noise tersebut tidak
    # membuat parsing JSON gagal.
    raw_output = result.stdout
    json_start = raw_output.find("{")
    if json_start == -1:
        logger.error(
            f"No JSON object found in snapshot status output: {raw_output!r}",
            extra={"action": "snapshot_validate", "status": "failed"},
        )
        raise BackupError("Snapshot status output does not contain JSON", ExitCode.SNAPSHOT_INVALID)

    if json_start > 0:
        logger.warning(
            f"Ignoring non-JSON prefix in snapshot status output: {raw_output[:json_start].strip()!r}",
            extra={"action": "snapshot_validate", "status": "warning"},
        )

    try:
        info = json.loads(raw_output[json_start:])
    except json.JSONDecodeError as e:
        logger.error(
            f"Snapshot status output is not valid JSON: {raw_output!r}",
            extra={"action": "snapshot_validate", "status": "failed"},
        )
        raise BackupError("Snapshot status output is not valid JSON", ExitCode.SNAPSHOT_INVALID) from e

    logger.info(
        f"Snapshot validated: hash={info.get('hash')} revision={info.get('revision')}",
        extra={"action": "snapshot_validate", "status": "ok"},
    )
    return info


def move_snapshot_to_local(tmp_container_path, snapshot_name, cfg, logger):
    """Pindahkan snapshot yang sudah tervalidasi keluar dari data_dir ETCD
    ke direktori backup lokal. Dijalankan SETELAH validasi sukses agar
    data_dir ETCD tidak menumpuk file sementara jika validasi gagal."""
    local_dir = Path(cfg["backup"]["local_directory"])
    local_dir.mkdir(parents=True, exist_ok=True)
    dest_path = local_dir / snapshot_name

    try:
        shutil.move(str(tmp_container_path), str(dest_path))
    except OSError as e:
        raise BackupError(f"Failed to move snapshot out of etcd data_dir: {e}", ExitCode.SNAPSHOT_FAILED) from e

    logger.info(
        f"Snapshot moved to local backup dir: {dest_path}",
        extra={"action": "snapshot_move", "status": "ok"},
    )
    return dest_path


# -----------------------------------------------------------------------
# 5.8 - Compression
# -----------------------------------------------------------------------
def compress_file(file_path, logger):
    gz_path = Path(str(file_path) + ".gz")
    try:
        with open(file_path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    except OSError as e:
        raise BackupError(f"Compression failed: {e}", ExitCode.COMPRESSION_FAILED) from e

    if not gz_path.exists() or gz_path.stat().st_size == 0:
        raise BackupError("Compression produced empty/invalid file", ExitCode.COMPRESSION_FAILED)

    # Snapshot mentah tidak perlu disimpan lagi setelah dikompres
    file_path.unlink()
    logger.info(
        f"Compressed snapshot: {gz_path} ({gz_path.stat().st_size} bytes)",
        extra={"action": "compression", "status": "ok", "size": gz_path.stat().st_size},
    )
    return gz_path


# -----------------------------------------------------------------------
# 5.9 - SHA256 checksum
# -----------------------------------------------------------------------
def sha256_file(file_path):
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def write_checksum_file(gz_path, checksum, logger):
    checksum_path = Path(str(gz_path) + ".sha256")
    checksum_path.write_text(f"{checksum}  {gz_path.name}\n")
    logger.info(
        f"Checksum written: {checksum_path}",
        extra={"action": "checksum", "status": "ok"},
    )
    return checksum_path


# -----------------------------------------------------------------------
# 5.10 - Local backup permissions + retention
# -----------------------------------------------------------------------
def apply_permissions(gz_path, checksum_path, config_path, logger):
    try:
        os.chmod(gz_path, 0o600)
        os.chmod(checksum_path, 0o600)
        if Path(config_path).exists():
            os.chmod(config_path, 0o640)
    except OSError as e:
        logger.warning(f"Failed to set permissions: {e}", extra={"action": "permissions", "status": "warning"})


def apply_local_retention(local_dir, retention_days, logger):
    cutoff = time.time() - (retention_days * 86400)
    removed = 0
    for pattern in ("*.db.gz", "*.db.gz.sha256"):
        for f in Path(local_dir).glob(pattern):
            if f.stat().st_mtime < cutoff:
                f.unlink()
                removed += 1
    logger.info(
        f"Local retention applied: removed {removed} file(s) older than {retention_days}d",
        extra={"action": "local_retention", "status": "ok"},
    )


# -----------------------------------------------------------------------
# 5.11 - NFS copy (structure: cluster/year/month/snapshot.gz)
# -----------------------------------------------------------------------
def copy_to_nfs(gz_path, checksum_path, checksum, cfg, logger):
    storage_cfg = cfg["storage"]
    cluster_name = cfg["cluster"]["name"]
    nfs_root = Path(storage_cfg["nfs_path"])

    now = datetime.now()
    dest_dir = nfs_root / cluster_name / str(now.year) / f"{now.month:02d}"

    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise BackupError(f"Cannot create NFS destination directory: {e}", ExitCode.NFS_FAILED) from e

    dest_gz = dest_dir / gz_path.name
    dest_checksum = dest_dir / checksum_path.name

    try:
        shutil.copy2(gz_path, dest_gz)
        shutil.copy2(checksum_path, dest_checksum)
    except OSError as e:
        raise BackupError(f"NFS copy failed: {e}", ExitCode.NFS_FAILED) from e

    # Validasi transfer: bandingkan checksum sumber vs tujuan
    dest_checksum_value = sha256_file(dest_gz)
    if dest_checksum_value != checksum:
        raise BackupError("NFS copy validation failed: checksum mismatch", ExitCode.NFS_FAILED)

    os.chmod(dest_gz, 0o600)
    os.chmod(dest_checksum, 0o600)

    logger.info(
        f"Copied to NFS: {dest_gz}",
        extra={"action": "nfs_copy", "status": "ok"},
    )
    return dest_gz


def apply_nfs_retention(cfg, logger):
    storage_cfg = cfg["storage"]
    cluster_name = cfg["cluster"]["name"]
    nfs_root = Path(storage_cfg["nfs_path"]) / cluster_name
    retention_days = storage_cfg.get("retention_days", 90)
    cutoff = time.time() - (retention_days * 86400)

    removed = 0
    if nfs_root.exists():
        for f in nfs_root.rglob("*.db.gz"):
            if f.stat().st_mtime < cutoff:
                f.unlink()
                checksum_sibling = Path(str(f) + ".sha256")
                if checksum_sibling.exists():
                    checksum_sibling.unlink()
                removed += 1
    logger.info(
        f"NFS retention applied: removed {removed} file(s) older than {retention_days}d",
        extra={"action": "nfs_retention", "status": "ok"},
    )


# -----------------------------------------------------------------------
# 6 - Prometheus metrics (node_exporter textfile collector)
# -----------------------------------------------------------------------
def write_metrics(cfg, metrics, logger):
    """
    Tulis metric ke file .prom untuk node_exporter textfile collector.
    Ditulis atomic (tmp file + rename) agar tidak pernah dibaca setengah jadi.
    """
    prom_cfg = cfg.get("prometheus", {})
    directory = Path(prom_cfg.get("textfile_directory", "/var/lib/node_exporter/textfile_collector"))
    filename = prom_cfg.get("metric_filename", "etcd_backup.prom")
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / filename
    tmp_target = directory / f".{filename}.tmp"

    lines = [
        "# HELP etcd_backup_success Whether the last backup run succeeded (1) or failed (0)",
        "# TYPE etcd_backup_success gauge",
        f"etcd_backup_success {metrics['success']}",
        "# HELP etcd_backup_duration_seconds Duration of the last backup run",
        "# TYPE etcd_backup_duration_seconds gauge",
        f"etcd_backup_duration_seconds {metrics['duration']}",
        "# HELP etcd_backup_size_bytes Size of the last backup snapshot",
        "# TYPE etcd_backup_size_bytes gauge",
        f"etcd_backup_size_bytes {metrics['size']}",
        "# HELP etcd_backup_last_success_timestamp Unix timestamp of the last successful backup",
        "# TYPE etcd_backup_last_success_timestamp gauge",
        f"etcd_backup_last_success_timestamp {metrics['last_success_timestamp']}",
        "# HELP etcd_snapshot_verified Whether the snapshot passed validation",
        "# TYPE etcd_snapshot_verified gauge",
        f"etcd_snapshot_verified {metrics['snapshot_verified']}",
        "# HELP etcd_nfs_copy_success Whether the NFS copy succeeded",
        "# TYPE etcd_nfs_copy_success gauge",
        f"etcd_nfs_copy_success {metrics['nfs_copy_success']}",
        "# HELP etcd_healthy_members Number of healthy ETCD members at run time",
        "# TYPE etcd_healthy_members gauge",
        f"etcd_healthy_members {metrics['healthy_members']}",
        "# HELP etcd_selected_endpoint_info Endpoint selected for this backup run",
        "# TYPE etcd_selected_endpoint_info gauge",
        f'etcd_selected_endpoint_info{{endpoint="{metrics["selected_endpoint"]}"}} 1',
        "# HELP etcd_backup_running Whether a backup run is currently in progress",
        "# TYPE etcd_backup_running gauge",
        f"etcd_backup_running {metrics['running']}",
        "# HELP etcd_backup_total Total number of backup runs",
        "# TYPE etcd_backup_total counter",
        f"etcd_backup_total {metrics['total_runs']}",
        "# HELP etcd_backup_failed_total Total number of failed backup runs",
        "# TYPE etcd_backup_failed_total counter",
        f"etcd_backup_failed_total {metrics['failed_runs']}",
        "",
    ]

    tmp_target.write_text("\n".join(lines))
    tmp_target.replace(target)
    logger.info(f"Metrics written to {target}", extra={"action": "write_metrics", "status": "ok"})


def load_run_counters(cfg):
    """Counter total_runs/failed_runs disimpan sederhana di file JSON kecil
    di samping direktori textfile collector agar bertahan antar-run."""
    prom_cfg = cfg.get("prometheus", {})
    directory = Path(prom_cfg.get("textfile_directory", "/var/lib/node_exporter/textfile_collector"))
    counters_path = directory / ".etcd_backup_counters.json"
    if counters_path.exists():
        try:
            return json.loads(counters_path.read_text()), counters_path
        except json.JSONDecodeError:
            pass
    return {"total_runs": 0, "failed_runs": 0, "last_success_timestamp": 0}, counters_path


def save_run_counters(counters, counters_path):
    counters_path.parent.mkdir(parents=True, exist_ok=True)
    counters_path.write_text(json.dumps(counters))


# -----------------------------------------------------------------------
# Main orchestration
# -----------------------------------------------------------------------
def main():
    config_path = os.environ.get("ETCD_BACKUP_CONFIG", "/etc/etcd-backup/config.yaml")
    if len(sys.argv) > 1:
        config_path = sys.argv[1]

    # Config error harus tertangani meski logger belum siap
    try:
        cfg = load_config(config_path)
    except BackupError as e:
        print(json.dumps({"level": "ERROR", "message": str(e), "exit_code": e.exit_code}))
        sys.exit(e.exit_code)

    logger = setup_logging(cfg)
    logger.info("Starting ETCD backup run", extra={"cluster": cfg["cluster"]["name"], "action": "start", "status": "info"})

    lock = ProcessLock(cfg["lock"]["file"], logger)
    counters, counters_path = load_run_counters(cfg)
    run_start = time.time()

    metrics = {
        "success": 0,
        "duration": 0,
        "size": 0,
        "last_success_timestamp": counters.get("last_success_timestamp", 0),
        "snapshot_verified": 0,
        "nfs_copy_success": 0,
        "healthy_members": 0,
        "selected_endpoint": "",
        "running": 1,
        "total_runs": counters["total_runs"] + 1,
        "failed_runs": counters["failed_runs"],
    }

    exit_code = ExitCode.SUCCESS
    try:
        lock.acquire()

        endpoint, container_id, healthy_count, total_count = select_endpoint(cfg, logger)
        metrics["healthy_members"] = healthy_count
        metrics["selected_endpoint"] = endpoint

        tmp_container_path, snapshot_name, snapshot_duration = snapshot_save(endpoint, container_id, cfg, logger)
        validate_snapshot(tmp_container_path, container_id, cfg, logger)
        metrics["snapshot_verified"] = 1

        snapshot_path = move_snapshot_to_local(tmp_container_path, snapshot_name, cfg, logger)

        gz_path = compress_file(snapshot_path, logger)
        checksum = sha256_file(gz_path)
        checksum_path = write_checksum_file(gz_path, checksum, logger)
        apply_permissions(gz_path, checksum_path, config_path, logger)

        apply_local_retention(cfg["backup"]["local_directory"], cfg["backup"]["retention_days"], logger)

        copy_to_nfs(gz_path, checksum_path, checksum, cfg, logger)
        metrics["nfs_copy_success"] = 1
        apply_nfs_retention(cfg, logger)

        metrics["success"] = 1
        metrics["size"] = gz_path.stat().st_size
        metrics["last_success_timestamp"] = int(time.time())

        logger.info(
            "Backup run completed successfully",
            extra={"action": "complete", "status": "ok", "duration": round(time.time() - run_start, 2)},
        )

    except BackupError as e:
        exit_code = e.exit_code
        counters["failed_runs"] += 1
        metrics["failed_runs"] = counters["failed_runs"]
        logger.error(str(e), extra={"action": "backup_run", "status": "failed"})
    except Exception as e:
        exit_code = ExitCode.UNEXPECTED_ERROR
        counters["failed_runs"] += 1
        metrics["failed_runs"] = counters["failed_runs"]
        logger.error(f"Unexpected error: {e}", extra={"action": "backup_run", "status": "failed"}, exc_info=True)
    finally:
        metrics["duration"] = round(time.time() - run_start, 2)
        metrics["running"] = 0
        counters["total_runs"] += 1
        counters["last_success_timestamp"] = metrics["last_success_timestamp"]
        save_run_counters(counters, counters_path)

        try:
            write_metrics(cfg, metrics, logger)
        except Exception as e:
            logger.warning(f"Failed to write metrics: {e}", extra={"action": "write_metrics", "status": "warning"})

        lock.release()

    sys.exit(exit_code)


if __name__ == "__main__":
    main()