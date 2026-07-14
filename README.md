# ETCD Backup Service

Automated, production-ready ETCD snapshot backup untuk Kubernetes cluster
**single control plane**. Struktur kode dirancang agar mudah diperluas ke
multi control plane di masa depan (lihat komentar `select_endpoint()`).

Dijalankan dan diuji di lingkungan: kubeadm cluster, Debian 13 (trixie),
containerd, ETCD 3.6.x (etcdctl/etcdutl terpisah sejak versi ini).

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

## Cara Kerja Singkat

Karena binary `etcdctl` umumnya **tidak terinstal di host** pada cluster
kubeadm (ETCD berjalan sebagai static pod di container), script ini
menjalankan seluruh perintah ETCD lewat `crictl exec` ke dalam container
`etcd` yang sedang berjalan — bukan lewat binary host.

Alur singkat:

1. `crictl ps --name ^etcd$ --state Running -q` → cari container ID etcd
2. `crictl exec <container_id> etcdctl endpoint health` → cek kesehatan endpoint
3. `crictl exec <container_id> etcdctl snapshot save <data_dir>/tmp.db` →
   snapshot ditulis dulu ke **data dir ETCD** (`/var/lib/etcd`), karena
   itu satu-satunya path yang identik antara host dan container (hostPath
   mount bawaan kubeadm)
4. `crictl exec <container_id> etcdutl snapshot status <tmp.db>` → validasi
   **sebelum** file dipindah keluar dari data dir (lihat catatan versi
   ETCD di bawah)
5. File dipindah ke `backup.local_directory`, lalu compress → SHA256 →
   local retention → copy ke NFS → NFS retention — semuanya berjalan di
   host, container tidak tersentuh lagi setelah langkah ini

> **Catatan penting soal versi ETCD**: sejak ETCD 3.6, subcommand
> `etcdctl snapshot status` **dihapus** dan dipindah ke tool terpisah
> `etcdutl` (yang bekerja offline langsung terhadap file, tanpa perlu
> `--cacert`/`--cert`/`--key`/`--endpoints`). Script ini sudah menyesuaikan
> dengan memanggil `etcdutl` khusus untuk validasi snapshot. Kalau cluster
> Anda memakai ETCD ≤ 3.5, `etcdctl snapshot status` masih ada — tapi
> memanggil `etcdutl` untuk versi lama biasanya tetap berfungsi juga
> (tool ini sudah ada sejak 3.4), jadi tidak perlu diubah.

## Instalasi

```bash
sudo mkdir -p /opt/etcd-backup-service
sudo cp -r scripts /opt/etcd-backup-service/
sudo mkdir -p /etc/etcd-backup
sudo cp config/config.yaml /etc/etcd-backup/config.yaml
sudo chmod 640 /etc/etcd-backup/config.yaml

# Install dependency (PyYAML) via apt, BUKAN pip --break-system-packages,
# supaya tidak menulis langsung ke Python sistem di luar package manager.
sudo apt update
sudo apt install -y python3-yaml

sudo mkdir -p /backup/etcd /var/log/etcd-backup /var/lib/node_exporter/textfile_collector

sudo cp systemd/etcd-backup.service systemd/etcd-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now etcd-backup.timer
```

> Service ini dijalankan sebagai **root** karena `crictl exec` butuh akses
> ke socket containerd (`/run/containerd/containerd.sock`), dan sertifikat
> ETCD (`server.key`) hanya bisa dibaca root. Lihat komentar di
> `etcd-backup.service` untuk detail.

### Prasyarat sebelum instalasi

Pastikan hal berikut sudah dicek di node control plane:

```bash
# crictl sudah terinstal dan mengarah ke socket containerd yang benar
which crictl
cat /etc/crictl.yaml

# container etcd sedang running, dan HANYA satu yang cocok
sudo crictl ps --name '^etcd$' --state Running -q

# etcdctl & etcdutl tersedia di dalam container (ganti <id> dengan hasil di atas)
sudo crictl exec <id> etcdctl version
sudo crictl exec <id> etcdutl version
```

## Konfigurasi

Edit `/etc/etcd-backup/config.yaml`. Field yang paling sering perlu
disesuaikan per environment:

| Field | Keterangan |
|---|---|
| `etcd.endpoints` | Untuk single control plane, cukup 1 endpoint `https://127.0.0.1:2379` — tetap `127.0.0.1` walau dieksekusi lewat `crictl exec`, karena static pod etcd kubeadm pakai `hostNetwork: true` |
| `etcd.ca_cert` / `cert_file` / `key_file` | Path sertifikat, biasanya `/etc/kubernetes/pki/etcd/` |
| `etcd.container_name` | Nama container etcd di `crictl ps`, default kubeadm persis `etcd` |
| `etcd.crictl_path` | Path binary `crictl` di host — **cek dengan `which crictl`**, tidak selalu di `/usr/local/bin` |
| `etcd.data_dir` | Data dir ETCD, dipakai sebagai lokasi sementara snapshot sebelum divalidasi & dipindah |
| `storage.nfs_path` | Di setup ini berupa **folder lokal** di node yang sama (`/srv/nfs/kubedata`), bukan NFS client mount. Kalau di environment lain ini benar-benar NFS mount, tambahkan `RequiresMountsFor=<path>` di `etcd-backup.service` |

## Menjalankan Manual

```bash
sudo python3 /opt/etcd-backup-service/scripts/etcd_backup.py /etc/etcd-backup/config.yaml
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

### Alerting (Grafana Alerting)

Rule berikut sudah diimplementasikan dan diverifikasi berjalan (grup
`1-minute`, evaluasi tiap 1 menit):

- `etcdBackupFailed` — `etcd_backup_success == 0`
- `etcdBackupStale` — `time() - etcd_backup_last_success_timestamp > 86400`
- `etcdQuorumLostDuringBackup` — `etcd_healthy_members < 1`
- `etcdNfsCopyFailed` — `etcd_nfs_copy_success == 0`
- `etcdSnapshotInvalid` — `etcd_snapshot_verified == 0`

Silakan sesuaikan threshold (`> 86400` dsb.) dan label `instance` sesuai kebutuhan multi-cluster di masa depan.

## Restore

Lihat [`docs/RESTORE.md`](docs/RESTORE.md). Restore **manual**, tidak
otomatis.

## Testing

Script sudah diuji end-to-end di:

1. **Cluster produksi single control plane** (kubeadm, Debian 13,
   containerd, ETCD 3.6.6) — backup end-to-end berhasil (exit 0), file
   snapshot terverifikasi di local backup dir dan NFS, metric Prometheus
   ter-generate dengan benar, alerting Grafana aktif seperti terlihat di
   atas.