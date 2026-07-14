# Restore Documentation — ETCD Backup Service

> **Restore dilakukan manual, tidak otomatis.** Dokumen ini adalah panduan
> operator saat terjadi kondisi darurat (control plane rusak / ETCD corrupt).

## 1. Kapan Restore Dibutuhkan

- ETCD data directory corrupt / tidak bisa start
- Kehilangan data akibat kesalahan operasional (misalnya salah `kubectl delete`
  pada resource kritikal skala besar)
- Disaster recovery ke cluster baru

## 2. Prasyarat

- Snapshot `.db.gz` dan file `.sha256` pasangannya, dari salah satu lokasi:
  - Local: `/backup/etcd/`
  - Local copy kedua (disebut "NFS" di konfigurasi, walau di setup ini berupa
    folder lokal): `/srv/nfs/kubedata/<cluster>/<year>/<month>/`
- Akses root ke node control plane
- Downtime control plane (restore ETCD **mematikan** API server sementara)
- **Binary `etcdutl` yang berdiri sendiri di host** (bukan lewat container) —
  lihat bagian 2.1 di bawah. Ini wajib disiapkan **sebelum** insiden terjadi,
  bukan saat insiden sedang berlangsung.

### 2.1 Kenapa butuh `etcdutl` terpisah di host

Script backup sehari-hari menjalankan `etcdctl`/`etcdutl` lewat
`crictl exec` ke dalam container `etcd` yang sedang berjalan — itu cukup
untuk backup karena container-nya memang harus hidup untuk melayani
cluster. Tapi untuk **restore**, itu bermasalah karena dua hal:

1. Sejak ETCD 3.6, subcommand `restore` (dan `status`) **dihapus** dari
   `etcdctl` dan dipindah ke tool terpisah `etcdutl`.
2. Skenario paling umum yang butuh restore adalah **ETCD sedang crash atau
   tidak bisa start** — artinya container-nya kemungkinan **tidak
   `Running`**, sehingga `crictl exec` tidak bisa dipakai sama sekali di
   momen paling kritis.

Solusinya: siapkan binary `etcdutl` versi yang sama dengan cluster
(`3.6.6` di setup ini) langsung di host. `etcdutl` adalah tool offline
murni — hanya membaca/menulis file, tidak perlu koneksi ke ETCD yang
sedang berjalan maupun ke containerd — sehingga bekerja walau ETCD dan
seluruh control plane sedang mati total.

```bash
ETCD_VER=v3.6.6   # samakan dengan versi ETCD di cluster Anda
cd /tmp
wget https://github.com/etcd-io/etcd/releases/download/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz
tar xzf etcd-${ETCD_VER}-linux-amd64.tar.gz
sudo cp etcd-${ETCD_VER}-linux-amd64/etcdutl /usr/local/bin/etcdutl
etcdutl version
```

> Simpan binary ini di host **sekarang**, sebelum insiden terjadi. Kalau
> node tidak punya akses internet saat darurat, Anda tidak akan bisa
> download di tengah insiden.

## 3. Verifikasi Integritas Snapshot

Sebelum restore, **selalu** verifikasi checksum terlebih dahulu:

```bash
cd /srv/nfs/kubedata/k8s-prod/2026/07/
sha256sum -c k8s-prod-20260713-150402.db.gz.sha256
```

Jika checksum tidak cocok, **jangan gunakan file tersebut** — cari snapshot
lain (misalnya hari sebelumnya).

## 4. Langkah Restore (Single Control Plane)

Semua perintah `etcdutl` di bawah dijalankan **langsung di host**
(bukan lewat `crictl exec`), memakai binary yang sudah disiapkan di
bagian 2.1.

### 4.1 Backup kondisi saat ini (jaga-jaga)

```bash
sudo cp -r /var/lib/etcd /var/lib/etcd.broken-$(date +%Y%m%d-%H%M%S)
```

### 4.2 Ekstrak & restore snapshot ke data directory baru

```bash
cd /srv/nfs/kubedata/k8s-prod/2026/07/
gunzip -k k8s-prod-20260713-150402.db.gz

sudo etcdutl snapshot restore k8s-prod-20260713-150402.db \
  --name <nama-member-etcd> \
  --initial-cluster <nama-member-etcd>=https://127.0.0.1:2380 \
  --initial-cluster-token etcd-cluster-restore \
  --initial-advertise-peer-urls https://127.0.0.1:2380 \
  --data-dir /var/lib/etcd-restored
```

Cek nilai `--name` dan `--initial-advertise-peer-urls` yang benar dari
konfigurasi ETCD yang sedang berjalan sebelumnya:

```bash
sudo grep -E "initial-cluster=|name=|initial-advertise-peer-urls=" /etc/kubernetes/manifests/etcd.yaml
```

### 4.3 Stop komponen yang menggunakan ETCD

```bash
sudo mv /etc/kubernetes/manifests/etcd.yaml /tmp/etcd.yaml.bak
sudo mv /etc/kubernetes/manifests/kube-apiserver.yaml /tmp/kube-apiserver.yaml.bak
# tunggu static pod etcd & apiserver benar-benar berhenti
sudo crictl ps | grep -E "etcd|kube-apiserver"
```

### 4.4 Ganti data directory

```bash
sudo mv /var/lib/etcd /var/lib/etcd.old
sudo mv /var/lib/etcd-restored /var/lib/etcd

# Samakan ownership dengan yang lama - cek dulu owner sebelumnya:
sudo stat -c '%U:%G' /var/lib/etcd.old
sudo chown -R <user>:<group> /var/lib/etcd   # isi sesuai hasil stat di atas
```

### 4.5 Aktifkan kembali static pod

```bash
sudo mv /tmp/etcd.yaml.bak /etc/kubernetes/manifests/etcd.yaml
# tunggu etcd sehat - pakai crictl exec seperti biasa karena container sudah hidup lagi
sudo crictl ps --name '^etcd$' --state Running -q
sudo crictl exec <container_id_baru> etcdctl endpoint health

sudo mv /tmp/kube-apiserver.yaml.bak /etc/kubernetes/manifests/kube-apiserver.yaml
```

### 4.6 Verifikasi cluster

```bash
kubectl get nodes
kubectl get pods -A
kubectl get events -A --sort-by=.lastTimestamp | tail -30
```

## 5. Rollback Jika Restore Gagal

```bash
sudo mv /var/lib/etcd /var/lib/etcd.failed-restore
sudo mv /var/lib/etcd.old /var/lib/etcd
sudo mv /tmp/etcd.yaml.bak /etc/kubernetes/manifests/etcd.yaml
sudo mv /tmp/kube-apiserver.yaml.bak /etc/kubernetes/manifests/kube-apiserver.yaml
```

## 6. Catatan Penting

- Restore **hanya mengembalikan state ETCD** (objek Kubernetes), **bukan**
  Persistent Volume, data aplikasi di dalam Pod, atau manifest custom di luar
  ETCD.
- Setelah restore, revisi `resourceVersion` berubah; beberapa controller
  mungkin memicu reconciliation ulang — pantau workload selama beberapa
  menit setelahnya.
- Uji prosedur restore ini secara berkala di lingkungan staging (bukan
  cluster produksi), termasuk memverifikasi binary `etcdutl` di host masih
  ada dan versinya masih cocok dengan cluster — jangan tunggu insiden nyata
  untuk tahu apakah backup Anda benar-benar bisa dipulihkan.