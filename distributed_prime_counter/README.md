# Distributed Prime Range Counter

Aplikasi komputasi terdistribusi pencari dan penghitung bilangan prima berbasis **Distributed Memory** menggunakan 2 PC fisik melalui protokol **TCP/IP Socket** (Python Standard Library).

---

## 1. Arsitektur Distributed Memory

Sistem menggunakan topologi **1 Master (PC 1) + 1 Worker (PC 2)** yang terhubung dalam satu jaringan LAN fisik (Wi-Fi atau kabel Ethernet switch):

```text
                                Jaringan LAN Fisik (Wi-Fi / Switch)
+-------------------------------+                                   +-------------------------------+
|         PC 1: MASTER          |                                   |         PC 2: WORKER          |
|      (Coordinator / Client)   |                                   |     (Processor / Server)      |
|       IP: 192.168.1.10        |                                   |       IP: 192.168.1.11        |
+-------------------------------+                                   +-------------------------------+
               |                                                                    |
               |============== [TCP CONNECT: Port 65432] ==========================>| (Listen 0.0.0.0)
               |                                                                    |
               |-------- TASK: range [worker_start .. N] (JSON Length-Prefix) ----->|
               |                                                                    |
    [KOMPUTASI PC 1 (T_m)]                                               [KOMPUTASI PC 2 (T_w)]
    count_primes(master_range)                                           count_primes(worker_range)
    (berjalan serentak)                                                  (berjalan serentak)
               |                                                                    |
               |<------- RESULT: C2 & T_w (JSON Length-Prefix) ---------------------|
               |                                                                    |
        Total = C1 + C2                                                             |
    T_dist = t_end - t_start                                                        |
               |                                                                    |
               |-------- STOP (JSON) [dikirim di akhir sesi] ---------------------->|
               |<------- ACK (JSON) ------------------------------------------------|
               |                                                                    |
           [CLOSE]                                                               [CLOSE]
```

### Prinsip Utama:
- **True Distributed Memory**: PC 1 dan PC 2 adalah dua komputer fisik terpisah. Masing-masing hanya mengakses memori RAM dan core CPU lokalnya sendiri. Data dibagikan murni melalui transmisi paket TCP.
- **Zero External Dependencies**: $100\%$ Python Standard Library (`socket`, `struct`, `json`, `math`, `time`, `statistics`, `csv`, `argparse`).
- **Length-Prefix Framing**: Header 4-byte unsigned integer big-endian (`!I`) menjamin integritas transmisi data terbingkai tanpa terpengaruh fragmentasi paket TCP.
- **Fair Concurrency**: Master langsung mengeksekusi komputasi partisi lokal begitu paket `TASK` terkirim ke jaringan, tanpa menunggu Worker selesai terlebih dahulu.

---

## 2. Struktur Proyek

```text
distributed_prime_counter/
├── common.py        # Protokol soket, algoritma is_prime(), count_primes(), split_range(), KNOWN_PRIMES
├── master.py        # Program Master: mode seq & dist interaktif dengan demo display
├── worker.py        # Program Worker: TCP Server mandiri bind 0.0.0.0:65432
├── benchmark.py     # Suite benchmark otomatis (warm-up, 5 runs, statistik, CSV logger, --summary)
├── results.csv      # Log data mentah hasil benchmark (format CSV RFC 4180)
└── README.md        # Panduan arsitektur, pengujian LAN, demo script, dan troubleshooting
```

---

## 3. Konfigurasi Jaringan & Firewall

### A. Mengetahui IP LAN PC 2 (Worker)
Jalankan di **PC 2**:
* **Linux:**
  ```bash
  ip addr
  # atau
  hostname -I
  ```
* **Windows (Command Prompt / PowerShell):**
  ```cmd
  ipconfig
  ```
Catat alamat **IPv4 Address** aktif (contoh: `192.168.1.11`).
> **Peringatan:** Jangan gunakan alamat `127.0.0.1` (loopback) atau IP virtual adapter (WSL, VirtualBox, VMware, Docker).

### B. Uji Konektivitas LAN
Dari **PC 1 (Master)**:
1. **Uji Ping:**
   ```bash
   ping <IP_WORKER>
   # Contoh: ping 192.168.1.11
   ```
2. **Uji Port Soket TCP:**
   - **Linux:** `nc -zv 192.168.1.11 65432`
   - **Windows (PowerShell):** `Test-NetConnection 192.168.1.11 -Port 65432`

### C. Konfigurasi Port Firewall di PC 2
Buka izin port 65432 TCP secara aman:
* **Linux (UFW di PC 2):**
  ```bash
  sudo ufw allow 65432/tcp
  ```
* **Windows (PowerShell Administrator di PC 2):**
  ```powershell
  New-NetFirewallRule -DisplayName "PrimeWorker" -Direction Inbound -LocalPort 65432 -Protocol TCP -Action Allow
  ```

---

## 4. Panduan Eksekusi Program

### Langkah 1: Jalankan Worker di PC 2
Di terminal **PC 2**:
```bash
python worker.py --bind 0.0.0.0 --port 65432
```

### Langkah 2: Jalankan Master di PC 1 (Manual Run)
* **Mode Sequential Baseline (1 PC lokal):**
  ```bash
  python master.py --mode seq --n 2000000
  ```
* **Mode Distributed (2 PC via LAN, default weighted):**
  ```bash
  python master.py --mode dist --host 192.168.1.11 --port 65432 --n 2000000
  ```
* **Mode Distributed dengan Equal Split:**
  ```bash
  python master.py --mode dist --host 192.168.1.11 --port 65432 --n 2000000 --split equal
  ```

### Langkah 3: Jalankan Automated Benchmark Suite di PC 1
```bash
python benchmark.py --host 192.168.1.11 --port 65432 --runs 5 --split weighted
```
Untuk melihat tabel rekapitulasi data dari file `results.csv` kapan saja:
```bash
python benchmark.py --summary
```

---

## 5. Matriks Pengujian Fungsional

| Test ID | Skenario N | Mode | Split | Expected Status | Keterangan |
|---|---:|---|---|---|---|
| **T1** | 10 | Sequential | N/A | SUCCESS | Baseline lokal kecil (4 prima) |
| **T2** | 10 | Distributed | weighted | MATCH | Verifikasi distributed handshake LAN |
| **T3** | 100 | Distributed | weighted | MATCH | Verifikasi ground truth (25 prima) |
| **T4** | 1,000 | Distributed | weighted | MATCH | Verifikasi ground truth (168 prima) |
| **T5** | 2,000,000 | Distributed | weighted | MATCH | Skenario S1 benchmark (148,933 prima) |
| **T6** | 6,000,000 | Distributed | weighted | MATCH | Skenario S2 benchmark (412,849 prima) |
| **T7** | 12,000,000 | Distributed | weighted | MATCH | Skenario S3 benchmark (788,060 prima) |

---

## 6. Format Rekapitulasi Laporan Benchmark

Tabel hasil resmi yang dihasilkan oleh `python benchmark.py --summary` dari `results.csv`:

| N | Mean T_seq | Mean T_dist | Speedup | Efficiency | Validation |
|---:|---:|---:|---:|---:|:---|
| 2,000,000 | *(T_seq)* | *(T_dist)* | *(Speedup)* | *(Efficiency)* | MATCH |
| 6,000,000 | *(T_seq)* | *(T_dist)* | *(Speedup)* | *(Efficiency)* | MATCH |
| 12,000,000 | *(T_seq)* | *(T_dist)* | *(Speedup)* | *(Efficiency)* | MATCH |

> **Catatan Analisis:** 
> - Jika $\text{Speedup} > 1$, komputasi terdistribusi berhasil mempercepat eksekusi program.
> - Jika $\text{Speedup} \approx 1$, *gain parallelism* seimbang dengan overhead.
> - Jika $\text{Speedup} < 1$, overhead transmisi jaringan LAN dan serialisasi soket mendominasi dibanding waktu komputasi murni.
> Angka pada laporan wajib diambil murni dari file `results.csv` hasil pengukuran nyata pada 2 PC fisik.

---

## 7. Panduan Rekaman Video Demo (Maksimal 10 Menit)

Gunakan struktur skenario berikut saat merekam video demonstrasi:

| Waktu | Bagian Demo | Aktivitas yang Ditampilkan di Video |
|---|---|---|
| **00:00 – 00:45** | Pendahuluan | Penjelasan judul project, tujuan distributed memory 2 PC, dan peran PC 1 (Master) serta PC 2 (Worker). |
| **00:45 – 02:00** | Verifikasi Jaringan Fisik | Tampilkan kedua layar laptop/PC fisik. Perlihatkan IP LAN PC 2 via `ip addr` / `ipconfig` dan lakukan tes `ping` dari PC 1. |
| **02:00 – 03:00** | Menjalankan Worker | Di PC 2, jalankan `python worker.py --bind 0.0.0.0 --port 65432`. Tunjukkan status *Listening on 0.0.0.0:65432*. |
| **03:00 – 04:30** | Uji Coba Distributed Interaktif | Di PC 1, jalankan `python master.py --mode dist --host <IP_PC2> --n 2000000`. |
| **04:30 – 06:00** | Pembuktian Komputasi Bersamaan | Tunjukkan layar PC 2 saat menerima `TASK`, memproses komputasi, dan mengirim `RESULT`. Tunjukkan layar PC 1 mengagregasi hasil dan mencetak `MATCH`. |
| **06:00 – 07:30** | Eksekusi Benchmark Suite | Di PC 1, jalankan `python benchmark.py --host <IP_PC2> --runs 5`. Tunjukkan proses warm-up, iterasi run 1-5, dan perhitungan statistik. |
| **07:30 – 08:30** | Pemeriksaan Data CSV | Buka file `results.csv` di teks editor atau Excel, tunjukkan log lengkap per run. Jalankan `python benchmark.py --summary`. |
| **08:30 – 09:30** | Analisis Metrik | Jelaskan makna metrik $T_{\text{seq}}$, $T_{\text{dist}}$, $T_m$, $T_w$, $T_{\text{comm}}$, Speedup, dan Efficiency. |
| **09:30 – 10:00** | Kesimpulan | Penutup: pembuktian bahwa distributed memory TCP soket berhasil berjalan stabil dan akurat. |

---

## 8. Panduan Troubleshooting Koneksi LAN

| Pesan Error | Penyebab Utama | Solusi |
|---|---|---|
| `ERROR: Cannot connect to Worker.` | `worker.py` belum aktif di PC 2 atau salah port. | Pastikan `worker.py` sudah berjalan di PC 2 pada port 65432. |
| `ERROR: Worker address is unreachable.` | IP salah atau firewall memblokir paket inbound. | Periksa IP PC 2 dan pastikan firewall mengizinkan port 65432 (lihat Bagian 3C). |
| `VALIDATION: MISMATCH` | Perbedaan algoritma atau kesalahan partisi data. | Pastikan kedua PC menggunakan file `common.py` yang sama persis. |

---

## 9. Final Acceptance Checklist

- [x] **Arsitektur:** PC 1 (Master) dan PC 2 (Worker) berkomunikasi via soket TCP LAN tanpa simulasi lokal.
- [x] **Komputasi:** Algoritma trial division $6k \pm 1$ dan `math.isqrt()` murni tanpa library eksternal.
- [x] **Integritas Partisi:** Master dan Worker membagi rentang $[1, N]$ tanpa celah (*gap*) dan tanpa tumpang tindih (*overlap*).
- [x] **Komunikasi:** Protokol `TASK` $\to$ `RESULT` $\to$ `STOP` $\to$ `ACK` tertutup secara *graceful*.
- [x] **Benchmark:** Mendukung 3 ukuran data PRD ($2M, 6M, 12M$), 1 warm-up, 5 runs resmi, dan kalkulasi statistik.
- [x] **Penyimpanan:** Data otomatis tersimpan persisten ke `results.csv`.
- [x] **Dokumentasi:** README lengkap mencakup panduan demo, matriks uji, konfigurasi jaringan, dan troubleshooting.
