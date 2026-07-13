# Restore Documentation — ETCD Backup Service

> **Catatan penting**: sesuai PRD, restore **tidak dilakukan otomatis**.
> Dokumen ini adalah panduan manual untuk operator saat terjadi kondisi
> darurat (control plane rusak / ETCD corrupt).

## 1. Kapan Restore Dibutuhkan

- ETCD data directory corrupt / tidak bisa start
- Kehilangan data akibat kesalahan operasional (misalnya salah `kubectl delete`
  pada resource kritikal skala besar)
- Disaster recovery ke cluster baru

## 2. Prasyarat

- Snapshot `.db.gz` dan file `.sha256` pasangannya (dari `/backup/etcd/` atau
  dari NFS di `/mnt/datacenter/<cluster>/<year>/<month>/`)
- Akses root ke node control plane
- `etcdctl` versi yang sama/kompatibel dengan versi ETCD yang berjalan
- Downtime control plane (restore ETCD **mematikan** API server sementara)

## 3. Verifikasi Integritas Snapshot

Sebelum restore, **selalu** verifikasi checksum terlebih dahulu:

```bash
cd /mnt/datacenter/production/2026/07/
sha256sum -c testcluster-20260713-063609.db.gz.sha256
```

Jika checksum tidak cocok, **jangan gunakan file tersebut** — cari snapshot
lain (misalnya hari sebelumnya).

## 4. Langkah Restore (Single Control Plane)

### 4.1 Backup kondisi saat ini (jaga-jaga)

```bash
sudo systemctl stop kubelet
sudo cp -r /var/lib/etcd /var/lib/etcd.broken-$(date +%Y%m%d-%H%M%S)
```

### 4.2 Ekstrak snapshot

```bash
gunzip -k testcluster-20260713-063609.db.gz
```

### 4.3 Stop komponen yang menggunakan ETCD

```bash
sudo mv /etc/kubernetes/manifests/etcd.yaml /tmp/etcd.yaml.bak
sudo mv /etc/kubernetes/manifests/kube-apiserver.yaml /tmp/kube-apiserver.yaml.bak
# tunggu static pod etcd & apiserver benar-benar berhenti
sudo crictl ps | grep -E "etcd|kube-apiserver"
```

### 4.4 Restore snapshot ke data directory baru

```bash
sudo etcdctl snapshot restore testcluster-20260713-063609.db \
  --name <nama-member-etcd> \
  --initial-cluster <nama-member-etcd>=https://127.0.0.1:2380 \
  --initial-cluster-token etcd-cluster-restore \
  --initial-advertise-peer-urls https://127.0.0.1:2380 \
  --data-dir /var/lib/etcd-restored
```

Sesuaikan `--name`, `--initial-cluster`, dan `--initial-advertise-peer-urls`
dengan konfigurasi ETCD yang tercatat di
`/etc/kubernetes/manifests/etcd.yaml` sebelumnya.

### 4.5 Ganti data directory

```bash
sudo mv /var/lib/etcd /var/lib/etcd.old
sudo mv /var/lib/etcd-restored /var/lib/etcd
sudo chown -R etcd:etcd /var/lib/etcd   # sesuaikan user/group sesuai environment
```

### 4.6 Aktifkan kembali static pod

```bash
sudo mv /tmp/etcd.yaml.bak /etc/kubernetes/manifests/etcd.yaml
# tunggu etcd sehat
sudo etcdctl endpoint health
sudo mv /tmp/kube-apiserver.yaml.bak /etc/kubernetes/manifests/kube-apiserver.yaml
sudo systemctl start kubelet
```

### 4.7 Verifikasi cluster

```bash
kubectl get nodes
kubectl get pods -A
kubectl get events -A --sort-by=.lastTimestamp | tail -30
```

## 5. Rollback Jika Restore Gagal

```bash
sudo mv /var/lib/etcd /var/lib/etcd.failed-restore
sudo mv /var/lib/etcd.old /var/lib/etcd
sudo systemctl restart kubelet
```

## 6. Catatan Penting

- Restore **hanya mengembalikan state ETCD** (objek Kubernetes), **bukan**
  Persistent Volume, data aplikasi di dalam Pod, atau manifest custom di luar
  ETCD — sesuai *Out of Scope* pada PRD.
- Setelah restore, revisi resourceVersion berubah; beberapa controller mungkin
  memicu reconciliation ulang — pantau workload selama beberapa menit.
- Selalu uji prosedur restore ini secara berkala di lingkungan staging (lihat
  skenario testing di PRD bagian 11), jangan tunggu insiden nyata untuk tahu
  apakah backup Anda benar-benar bisa dipulihkan.
