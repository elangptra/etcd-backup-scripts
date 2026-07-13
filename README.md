# ETCD Backup Service

Automated, production-ready ETCD snapshot backup untuk Kubernetes cluster
**single control plane**. Struktur kode dirancang agar mudah diperluas ke
multi control plane di masa depan (lihat komentar `select_endpoint()`).

## Struktur

```
etcd-backup-service/
├── config/
│   └── config.yaml            # Konfigurasi (tidak ada nilai hardcoded di kode)
├── scripts/
│   └── etcd_backup.py         # Script utama
├── systemd/
│   ├── etcd-backup.service
│   └── etcd-backup.timer
├── docs/
│   └── RESTORE.md             # Panduan restore manual (out of scope: restore otomatis)
└── requirements.txt
```

## Instalasi

```bash
sudo mkdir -p /opt/etcd-backup-service
sudo cp -r scripts /opt/etcd-backup-service/
sudo mkdir -p /etc/etcd-backup
sudo cp config/config.yaml /etc/etcd-backup/config.yaml
sudo chmod 640 /etc/etcd-backup/config.yaml

pip install -r requirements.txt --break-system-packages

sudo mkdir -p /backup/etcd /var/log/etcd-backup /var/lib/node_exporter/textfile_collector

sudo cp systemd/etcd-backup.service systemd/etcd-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now etcd-backup.timer
```

> Service ini dijalankan sebagai **root** karena `crictl exec` butuh akses
> ke socket containerd, dan sertifikat ETCD (`server.key`) hanya bisa dibaca
> root. Lihat komentar di `etcd-backup.service` untuk detail.

## Konfigurasi

Edit `/etc/etcd-backup/config.yaml`:

- `etcd.endpoints`: untuk single control plane, cukup 1 endpoint
  (`https://127.0.0.1:2379`)
- `etcd.ca_cert` / `cert_file` / `key_file`: biasanya di
  `/etc/kubernetes/pki/etcd/`
- `storage.nfs_path`: pastikan sudah di-mount sebelum service berjalan
  (tambahkan dependency mount di systemd bila perlu)

## Menjalankan Manual

```bash
sudo -u etcd-backup python3 /opt/etcd-backup-service/scripts/etcd_backup.py /etc/etcd-backup/config.yaml
echo "Exit code: $?"
```

## Exit Codes

| Code | Arti               |
|------|---------------------|
| 0    | Success              |
| 1    | Config Error         |
| 2    | Health Check Failed  |
| 3    | Quorum Lost          |
| 4    | Snapshot Failed       |
| 5    | Snapshot Invalid     |
| 6    | Compression Failed   |
| 7    | NFS Failed            |
| 8    | Unexpected Error     |

## Monitoring

Metric ditulis ke file `.prom` (node_exporter textfile collector) karena job
ini one-shot lewat systemd timer, bukan long-running process — jadi lebih
tepat pakai textfile collector daripada `start_http_server` yang cuma hidup
sesaat.

Arahkan node_exporter:

```bash
node_exporter --collector.textfile.directory=/var/lib/node_exporter/textfile_collector
```

Metric yang tersedia: `etcd_backup_success`, `etcd_backup_duration_seconds`,
`etcd_backup_size_bytes`, `etcd_backup_last_success_timestamp`,
`etcd_snapshot_verified`, `etcd_nfs_copy_success`, `etcd_healthy_members`,
`etcd_selected_endpoint_info`, `etcd_backup_running`, `etcd_backup_total`,
`etcd_backup_failed_total`.

Contoh Alertmanager rule ada di PRD bagian 8 — bisa langsung dipetakan ke
metric-metric di atas (mis. `etcd_backup_success == 0`,
`time() - etcd_backup_last_success_timestamp > 86400`,
`etcd_healthy_members < 1`).

## Restore

Lihat [`docs/RESTORE.md`](docs/RESTORE.md). Restore **manual**, tidak
otomatis (sesuai *Out of Scope* PRD).

## Testing

Script sudah diuji manual dengan mock `etcdctl` untuk skenario:
- Single CP sehat → backup sukses (exit 0)
- Single CP down → quorum lost (exit 3)

Untuk automated test lengkap (semua skenario di PRD bagian 11), sarankan
tambahkan `pytest` dengan mocking `subprocess.run` — beri tahu saya bila
ingin saya buatkan suite test-nya juga.
