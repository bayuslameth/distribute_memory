# Distributed Prime Range Counter

Aplikasi komputasi paralel dan terdistribusi pencari serta penghitung bilangan prima berbasis **Distributed Memory** menggunakan 2 PC fisik (atau laptop) yang terhubung melalui jaringan **LAN / VLAN yang sama** menggunakan protokol **Python Socket TCP/IP** murni.

---

## 1. Arsitektur Distributed Memory

Sistem mengadopsi topologi **1 Master / Coordinator (PC 1) + 1 Worker / Processor (PC 2)**:

```text
                                Jaringan LAN / VLAN Fisik (Switch / Router / Wi-Fi)
+-------------------------------+                                                   +-------------------------------+
|         PC 1: MASTER          |                                                   |         PC 2: WORKER          |
|      (Coordinator / Client)   |                                                   |     (Processor / Server)      |
|    Contoh IP: 192.168.1.10    |                                                   |    Contoh IP: 192.168.1.11    |
+-------------------------------+                                                   +-------------------------------+
                |                                                                                   |
                |============== [TCP CONNECT: Port 65432] =========================================>| (Listen 0.0.0.0)
                |                                                                                   |
                |-------- TASK: range [worker_start .. N] (JSON Length-Prefix) -------------------->|
                |                                                                                   |
     [KOMPUTASI PC 1 (T_m)]                                                              [KOMPUTASI PC 2 (T_w)]
     count_primes(1 .. s)                                                                count_primes(s+1 .. N)
     (berjalan serentak)                                                                 (berjalan serentak)
                |                                                                                   |
                |<------- RESULT: prime_count & T_w (JSON Length-Prefix) ---------------------------|
                |                                                                                   |
         Total = C1 + C2                                                                            |
     T_dist = t_end - t_start                                                                       |
                |                                                                                   |
                |-------- STOP (JSON Length-Prefix) [setelah seluruh pengujian selesai] ----------->|
                |<------- ACK (JSON Length-Prefix) -------------------------------------------------|
                |                                                                                   |
            [CLOSE]                                                                              [CLOSE]
```

### Prinsip Utama Sistem:
1. **True Distributed Memory**: PC 1 dan PC 2 adalah dua komputer fisik terpisah dengan ruang alamat memori (RAM) dan CPU mandiri. PC 1 tidak dapat mengakses RAM PC 2 secara langsung, melainkan harus bertukar pesan (*message passing*) melalui paket data TCP.
2. **Zero External Dependencies**: $100\%$ dibangun menggunakan pustaka standar bawaan Python (`socket`, `struct`, `json`, `math`, `time`, `statistics`, `csv`, `argparse`). Tidak memerlukan MPI, Docker, threading, multiprocessing, Celery, atau library pihak ketiga.
3. **Length-Prefix Framing**: Header 4-byte unsigned integer big-endian (`!I`) menjamin integritas transmisi data terbingkai tanpa risiko terpengaruh fragmentasi paket TCP stream.
4. **Independent Execution**: PC 1 langsung mengeksekusi komputasi partisi lokal begitu paket `TASK` terkirim ke soket, tanpa menunggu PC 2 selesai terlebih dahulu.
5. **Cross-Platform**: Berjalan mulus di Windows, Linux, maupun macOS.

---

## 2. Struktur Proyek

```text
distributed_prime_counter/
├── common.py        # Protokol soket, length-prefix framing, is_prime(), count_primes(), split_range(), KNOWN_PRIMES
├── master.py        # Program Master: eksekusi manual single-run (mode seq & mode dist)
├── worker.py        # Program Worker: TCP Server mandiri bind 0.0.0.0:65432 (loop TASK & STOP)
├── benchmark.py     # Suite benchmark otomatis: 3 ukuran N, 5 runs, warm-up, Tabel A, B, C, results.csv
├── results.csv      # Log data mentah hasil pengujian benchmark (format CSV RFC 4180)
└── README.md        # Dokumentasi arsitektur, panduan konfigurasi LAN, eksekusi, dan analisis metrik
```

---

## 3. Strategi Pembagian Beban Kerja

Algoritma penentuan bilangan prima memiliki kompleksitas non-linear karena batas pengujian bilangan $n$ adalah $\lfloor\sqrt{n}\rfloor$. Bilangan yang lebih besar membutuhkan komputasi yang jauh lebih berat dibanding bilangan kecil.

Sistem mengimplementasikan dua strategi partisi data:

### A. Strategi Equal (50 : 50)
Rentang data dibagi rata secara jumlah elemen:
* Titik potong: $s = \lfloor N / 2 \rfloor$
* PC 1 (Master): Rentang $[1, s]$
* PC 2 (Worker): Rentang $[s + 1, N]$

| N | Beban PC 1 (Master) | Beban PC 2 (Worker) | Karakteristik Beban |
|---:|---:|---:|:---|
| **2.000.000** | 1.000.000 | 1.000.000 | Beban tidak seimbang (*load imbalance*): PC 1 selesai jauh lebih cepat karena menguji angka 1–1.000.000, sedangkan PC 2 menguji 1.000.001–2.000.000 dengan akar $\sqrt{n}$ yang lebih tinggi. |
| **6.000.000** | 3.000.000 | 3.000.000 | PC 1 idle menunggu Worker menyelesaikan separuh akhir data. |
| **12.000.000** | 6.000.000 | 6.000.000 | Efisiensi terdistribusi turun karena terjadi *bottleneck* komputasi di PC 2. |

### B. Strategi Weighted (1,7 : 1)
Menggunakan formula proporsi beban komputasi $s = \text{round}\left(\frac{N}{2^{2/3}}\right) \approx 63\% \text{ data untuk PC 1}$ dan $37\% \text{ data untuk PC 2}$:
* PC 1 (Master): Rentang $[1, s]$ (jumlah bilangan lebih banyak, namun angka-angka kecil lebih ringan per bilangan).
* PC 2 (Worker): Rentang $[s + 1, N]$ (jumlah bilangan lebih sedikit, namun angka-angka besar lebih berat per bilangan).

| N | Beban PC 1 (Master) | Beban PC 2 (Worker) | Rasio Target Waktu Komputasi ($T_m : T_w$) |
|---:|---:|---:|:---:|
| **2.000.000** | 1.259.921 | 740.079 | $\approx 1,00 : 1,00$ (Ideal Load Balance) |
| **6.000.000** | 3.779.763 | 2.220.237 | $\approx 1,00 : 1,00$ (Ideal Load Balance) |
| **12.000.000** | 7.559.526 | 4.440.474 | $\approx 1,00 : 1,00$ (Ideal Load Balance) |

---

## 4. Konfigurasi Jaringan LAN / VLAN & Windows Firewall

### A. Mengetahui IP PC 2 (Worker)
Jalankan perintah ini di **PC 2**:
* **Windows (Command Prompt / PowerShell):**
  ```cmd
  ipconfig
  ```
  Catat **IPv4 Address** antarmuka jaringan yang terhubung ke LAN/VLAN (misalnya `192.168.1.11`).
* **Linux:**
  ```bash
  hostname -I
  # atau
  ip addr
  ```

> [!WARNING]
> Jangan gunakan IP `127.0.0.1` (loopback) atau IP adapter virtual (WSL, VirtualBox, VMware, Docker) karena tidak dapat diakses dari PC fisik lain di LAN.

### B. Membuka Port 65432 di Windows Firewall (PC 2)
Agar PC 1 dapat menghubungi PC 2, port 65432 TCP harus diizinkan masuk (*inbound*):

1. **Menggunakan PowerShell Administrator (Rekomendasi Cepat):**
   Klik kanan Start Menu $\to$ pilih **Terminal (Admin)** atau **Windows PowerShell (Admin)**, lalu jalankan:
   ```powershell
   New-NetFirewallRule -DisplayName "DistributedPrimeWorker" -Direction Inbound -LocalPort 65432 -Protocol TCP -Action Allow
   ```
2. **Menggunakan Windows Defender Firewall GUI:**
   - Buka **Control Panel** $\to$ **System and Security** $\to$ **Windows Defender Firewall**.
   - Pilih **Advanced settings** di menu sebelah kiri.
   - Klik **Inbound Rules** $\to$ klik **New Rule...** di panel kanan.
   - Pilih tipe aturan: **Port** $\to$ Next.
   - Pilih **TCP** dan isi **Specific local ports**: `65432` $\to$ Next.
   - Pilih **Allow the connection** $\to$ Next.
   - Centang opsi **Domain**, **Private**, dan **Public** $\to$ Next.
   - Beri nama: `DistributedPrimeWorker` $\to$ klik **Finish**.

### C. Uji Konektivitas Jaringan dari PC 1 (Master)
Sebelum menjalankan program, verifikasi keterjangkauan PC 2 dari terminal **PC 1**:
1. Uji Ping:
   ```bash
   ping 192.168.1.11
   ```
2. Uji Port Soket TCP:
   - **Windows PowerShell:** `Test-NetConnection 192.168.1.11 -Port 65432`
   - **Linux:** `nc -zv 192.168.1.11 65432`

---

## 5. Panduan Menjalankan Program

### Langkah 1: Jalankan Worker di PC 2
Jalankan terlebih dahulu di terminal **PC 2**:
```bash
python worker.py --bind 0.0.0.0 --port 65432
```
Worker akan mencetak:
```text
============================================================
WORKER NODE (Distributed Memory Processor)
============================================================
Host IP : 0.0.0.0
Port    : 65432
Status  : LISTENING
Waiting for Master connection from LAN...
```

### Langkah 2: Menjalankan Pengujian Manual di PC 1 (Master)
Untuk demonstrasi single-run:
* **Mode Sequential Baseline (Lokal 1 PC):**
  ```bash
  python master.py --mode seq --n 2000000
  ```
* **Mode Distributed (2 PC via LAN, strategi Weighted):**
  ```bash
  python master.py --mode dist --host 192.168.1.11 --port 65432 --n 2000000
  ```
* **Mode Distributed (2 PC via LAN, strategi Equal):**
  ```bash
  python master.py --mode dist --host 192.168.1.11 --port 65432 --n 2000000 --split equal
  ```

### Langkah 3: Menjalankan Seluruh Pengujian Otomatis (Benchmark Suite)
Jalankan perintah ini di **PC 1** untuk mengeksekusi pengujian Sequential, Distributed-Weighted, dan Distributed-Equal untuk ketiga ukuran data ($N = 2.000.000, 6.000.000, 12.000.000$) masing-masing sebanyak 5 run (+ 1 warm-up):
```bash
python benchmark.py --host 192.168.1.11 --port 65432
```

Program akan menjalankan urutan berikut secara otomatis:
1. Warm-up 1x (tidak dicatat).
2. 5x Sequential run.
3. 5x Distributed Weighted run.
4. 5x Distributed Equal run.
5. Menghitung statistik dan rasio waktu.
6. Menyimpan seluruh run ke `results.csv`.
7. Menampilkan **Tabel A**, **Tabel B**, dan **Tabel C** secara langsung.

### Langkah 4: Menampilkan Ringkasan Tabel dari CSV Kapan Saja
Jika pengujian benchmark sudah selesai dan ingin mencetak kembali Tabel A, B, dan C tanpa menjalankan komputasi ulang:
```bash
python benchmark.py --summary
```

---

## 6. Format Tabel Output Hasil Pengujian

Output benchmark disajikan dalam format Markdown yang siap disalin langsung ke laporan tugas:

### TABEL A: HASIL PENGUJIAN PERFORMA SETIAP SKENARIO
Menyajikan perbandingan performa antara Sequential dan Distributed (Weighted):

| Skenario (N) | T_seq (s) | T_m (s) | T_dist (s) | Speedup | Eff (%) |
|-------------:|----------:|--------:|-----------:|--------:|--------:|
|    2.000.000 |  *(T_seq)*|  *(T_m)*|  *(T_dist)*| *(Sp)*  |  *(Eff)*|
|    6.000.000 |  *(T_seq)*|  *(T_m)*|  *(T_dist)*| *(Sp)*  |  *(Eff)*|
|   12.000.000 |  *(T_seq)*|  *(T_m)*|  *(T_dist)*| *(Sp)*  |  *(Eff)*|

### TABEL B: DISTRIBUSI BEBAN KERJA (EQUAL VS WEIGHTED)
Menyajikan perbandingan beban partisi data serta rasio waktu komputasi lokal PC 1 terhadap PC 2 ($T_m : T_w$):

|          N | Strategi | Beban PC 1 | Beban PC 2 |   T_m (s) |   T_w (s) | Rasio T_m : T_w |
|-----------:|:---------|-----------:|-----------:|----------:|----------:|:----------------|
|  2.000.000 | Equal    |  1.000.000 |  1.000.000 |  *(T_m)*  |  *(T_w)*  | 0,74 : 1,00     |
|  2.000.000 | Weighted |  1.259.921 |    740.079 |  *(T_m)*  |  *(T_w)*  | 0,99 : 1,00     |
|  6.000.000 | Equal    |  3.000.000 |  3.000.000 |  *(T_m)*  |  *(T_w)*  | 0,74 : 1,00     |
|  6.000.000 | Weighted |  3.779.763 |  2.220.237 |  *(T_m)*  |  *(T_w)*  | 0,99 : 1,00     |
| 12.000.000 | Equal    |  6.000.000 |  6.000.000 |  *(T_m)*  |  *(T_w)*  | 0,74 : 1,00     |
| 12.000.000 | Weighted |  7.559.526 |  4.440.474 |  *(T_m)*  |  *(T_w)*  | 1,00 : 1,00     |

### TABEL C: STATISTIK WAKTU EKSEKUSI ANTAR-RUN
Menyajikan data mentah dari Run 1 hingga Run 5 serta parameter statistiknya:

|          N | Mode          | Run 1 (s) | Run 2 (s) | Run 3 (s) | Run 4 (s) | Run 5 (s) | Rata-rata (s) | Median (s) | Std. Dev (s) |
|-----------:|:--------------|----------:|----------:|----------:|----------:|----------:|--------------:|-----------:|-------------:|
|  2.000.000 | seq           |    ...    |    ...    |    ...    |    ...    |    ...    |      ...      |    ...     |     ...      |
|  2.000.000 | dist-weighted |    ...    |    ...    |    ...    |    ...    |    ...    |      ...      |    ...     |     ...      |
|  2.000.000 | dist-equal    |    ...    |    ...    |    ...    |    ...    |    ...    |      ...      |    ...     |     ...      |
|  6.000.000 | seq           |    ...    |    ...    |    ...    |    ...    |    ...    |      ...      |    ...     |     ...      |
|  6.000.000 | dist-weighted |    ...    |    ...    |    ...    |    ...    |    ...    |      ...      |    ...     |     ...      |
|  6.000.000 | dist-equal    |    ...    |    ...    |    ...    |    ...    |    ...    |      ...      |    ...     |     ...      |
| 12.000.000 | seq           |    ...    |    ...    |    ...    |    ...    |    ...    |      ...      |    ...     |     ...      |
| 12.000.000 | dist-weighted |    ...    |    ...    |    ...    |    ...    |    ...    |      ...      |    ...     |     ...      |
| 12.000.000 | dist-equal    |    ...    |    ...    |    ...    |    ...    |    ...    |      ...      |    ...     |     ...      |

---

## 7. Rumus Metrik & Analisis Komputasi Terdistribusi

### A. Speedup
Mengukur seberapa kali lebih cepat sistem terdistribusi dibandingkan sistem sekuensial tunggal:
$$\text{Speedup} = \frac{T_{\text{seq}}}{T_{\text{dist}}}$$
* Jika $\text{Speedup} > 1$: Terjadi percepatan (*speedup* positif).
* Nilai teoritis maksimum dengan 2 PC adalah $2,00\times$ (Amdahl's Law).

### B. Efficiency
Mengukur efektivitas pemanfaatan sumber daya komputasi dari kedua node PC:
$$\text{Efficiency} = \left(\frac{\text{Speedup}}{P}\right) \times 100\% = \left(\frac{\text{Speedup}}{2}\right) \times 100\%$$
* Efisiensi ideal adalah $100\%$ (Speedup $= 2,00\times$).
* Pada sistem nyata di jaringan LAN, efisiensi berkisar antara $75\% - 95\%$ akibat adanya latensi jaringan dan serialisasi data.

### C. Waktu Komputasi vs Waktu Total Distributed
* **$T_m$ (Waktu Komputasi PC 1):** Durasi komputasi murni Master saat mengeksekusi `count_primes(master_range)`.
* **$T_w$ (Waktu Komputasi PC 2):** Durasi komputasi murni Worker saat mengeksekusi `count_primes(worker_range)` di PC 2.
* **$T_{\text{comm}}$ (Network & Serialization Overhead):**
  $$T_{\text{comm}} = T_{\text{dist}} - \max(T_m, T_w)$$
  Mencakup durasi transmisi TCP, latensi kabel/Wi-Fi switch, serialisasi JSON, dan pembuatan header.
* **$T_{\text{dist}}$ (Wall-clock Total):** Waktu nyata dari saat Master mulai mengirimkan `TASK` hingga seluruh hasil `RESULT` diterima dan diagregasikan.

### D. Mengapa Distributed Processing Belum Tentu Lebih Cepat?
1. **Network Communication Overhead ($T_{\text{comm}}$):** Jika ukuran data $N$ terlalu kecil (misalnya $N < 100.000$), waktu komputasi hanya beberapa milidetik. Biaya latensi bolak-balik (RTT) soket TCP LAN lebih besar daripada waktu komputasi, sehingga $T_{\text{dist}} > T_{\text{seq}}$ ($\text{Speedup} < 1$).
2. **Ketidakseimbangan Beban (*Load Imbalance*):** Jika menggunakan pembagian Equal (50:50), PC 1 akan selesai lebih cepat dan harus menganggur menunggu PC 2. Total waktu distributed dibatasi oleh node yang paling lambat: $T_{\text{dist}} \approx \max(T_m, T_w) + T_{\text{comm}}$.
3. **Serial Fraction (Hukum Amdahl):** Bagian kode yang tidak dapat diparalelkan (inisiasi soket, partisi range, agregasi hasil penjumlahan) membatasi batas atas percepatan yang dapat dicapai.

---

## 8. Ground Truth & Validasi Kebenaran Hasil

Jumlah bilangan prima yang dihitung harus selalu identik antara mode Sequential dan Distributed, serta cocok dengan referensi matematis PRD:
* $N = 2.000.000 \to 148.933$ bilangan prima
* $N = 6.000.000 \to 412.849$ bilangan prima
* $N = 12.000.000 \to 788.060$ bilangan prima
