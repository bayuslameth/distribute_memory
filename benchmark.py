"""
benchmark.py - Automated Benchmark Suite untuk Distributed Prime Range Counter.

Menghasilkan:
- TABEL A: Hasil Pengujian Performa Setiap Skenario (N, T_seq, T_m, T_dist, Speedup, Eff (%))
- TABEL B: Distribusi Beban Kerja Equal vs Weighted (N, Strategi, Beban PC 1, Beban PC 2, T_m, T_w, Rasio T_m : T_w)
- TABEL C: Statistik Waktu Eksekusi Antar-Run (N, Mode, Run 1-5, Rata-rata, Median, Std. Dev)

Fitur:
1. Otomatisasi pengujian untuk 3 ukuran data:
   - S1: N = 2.000.000  (Reference prime count: 148.933)
   - S2: N = 6.000.000  (Reference prime count: 412.849)
   - S3: N = 12.000.000 (Reference prime count: 788.060)
2. 1 Warm-up run per skenario (tidak dimasukkan ke CSV atau statistik).
3. 5 kali pengulangan (runs = 5) untuk masing-masing mode:
   - Sequential baseline (T_seq)
   - Distributed Weighted (T_dist, T_m, T_w, T_comm)
   - Distributed Equal (T_dist, T_m, T_w, T_comm)
4. Perhitungan statistik sesuai PRD:
   - Rata-rata = sum(x) / 5
   - Median = nilai tengah dari 5 run
   - Standard Deviation = statistics.pstdev() untuk populasi 5 run
5. Rasio beban kerja T_m : T_w ditulis dalam format "X,XX : 1,00".
6. Penyimpanan otomatis log eksekusi ke results.csv (mode append).
7. Flag --summary untuk menampilkan Tabel A, B, dan C langsung dari results.csv.
"""

import argparse
import csv
import datetime
import os
import socket
import statistics
import sys
import time
from typing import Any, Optional
from common import (
    KNOWN_PRIMES,
    count_primes,
    recv_msg,
    send_msg,
    split_range,
)

CSV_FIELDNAMES = [
    "timestamp",
    "scenario",
    "run",
    "n",
    "mode",
    "split",
    "master_start",
    "master_end",
    "worker_start",
    "worker_end",
    "master_range_size",
    "worker_range_size",
    "master_prime_count",
    "worker_prime_count",
    "final_prime_count",
    "t_seq",
    "t_master",
    "t_worker",
    "t_dist",
    "t_comm",
    "speedup",
    "efficiency",
    "validation",
]


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Automated Benchmark Suite untuk Distributed Prime Range Counter"
    )
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Alamat IP LAN Worker node di PC 2 (wajib kecuali jika menggunakan opsi --summary)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=65432,
        help="Port TCP Worker node (default: 65432)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Jumlah pengulangan resmi per skenario (default: 5)",
    )
    parser.add_argument(
        "--split",
        type=str,
        choices=["both", "weighted", "equal"],
        default="both",
        help="Strategi partisi yang diuji: 'both' (default, menguji weighted & equal), 'weighted', atau 'equal'",
    )
    parser.add_argument(
        "--sizes",
        nargs="+",
        type=int,
        default=[2000000, 6000000, 12000000],
        help="Daftar ukuran data N yang diuji (default: 2000000 6000000 12000000)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=180.0,
        help="Batas waktu soket dalam detik (default: 180.0)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="results.csv",
        help="File CSV tujuan penyimpanan data benchmark (default: results.csv)",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Tampilkan Tabel A, B, dan C dari file results.csv tanpa menjalankan benchmark ulang",
    )
    return parser.parse_args()


# =====================================================================
# 1. STATISTICAL & FORMATTING HELPERS
# =====================================================================


def calculate_stats(data: list[float]) -> dict[str, float]:
    """
    Menghitung mean, median, pstdev (population standard deviation), min, max.
    Sesuai instruksi PRD: Standard deviation menggunakan statistics.pstdev()
    untuk 5 run yang dilaporkan.
    """
    if not data:
        return {"mean": 0.0, "median": 0.0, "pstdev": 0.0, "min": 0.0, "max": 0.0}
    mean_val = statistics.mean(data)
    median_val = statistics.median(data)
    pstdev_val = statistics.pstdev(data) if len(data) >= 1 else 0.0
    return {
        "mean": mean_val,
        "median": median_val,
        "pstdev": pstdev_val,
        "min": min(data),
        "max": max(data),
    }


def format_id_num(val: int) -> str:
    """Format angka integer dengan pemisah titik gaya Indonesia (e.g. 2.000.000)."""
    return f"{val:,}".replace(",", ".")


def format_ratio_tm_tw(t_m: float, t_w: float) -> str:
    """
    Format rasio waktu T_m : T_w dalam bentuk 'X,XX : 1,00'.
    Contoh: 1,12 : 1,00 atau 0,98 : 1,00.
    """
    if t_w <= 0.0:
        return "N/A"
    ratio = t_m / t_w
    return f"{ratio:.2f}".replace(".", ",") + " : 1,00"


# =====================================================================
# 2. CSV LOGGING
# =====================================================================


def append_csv_row(csv_path: str, row_dict: dict[str, Any]) -> None:
    """Menuliskan baris data ke results.csv secara aman (append)."""
    file_exists = os.path.isfile(csv_path)
    with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row_dict)


# =====================================================================
# 3. BENCHMARK EXECUTION HELPERS
# =====================================================================


def execute_sequential_run(n: int) -> tuple[int, float]:
    """Mengeksekusi perhitungan sequential [1, N] dan mengukur T_seq murni."""
    t0 = time.perf_counter()
    count = count_primes(1, n)
    t1 = time.perf_counter()
    return count, (t1 - t0)


def execute_distributed_run(
    sock: socket.socket,
    n: int,
    split_mode: str,
    run_id: str,
) -> dict[str, Any]:
    """
    Mengeksekusi perhitungan distributed [1, N] pada Worker fisik & Master lokal.
    Mengukur T_dist, T_m, T_w, dan T_comm.
    """
    m_start, m_end, w_start, w_end = split_range(n, split_mode)
    task_payload = {
        "type": "TASK",
        "task_id": run_id,
        "n_total": n,
        "start": w_start,
        "end": w_end,
        "split_mode": split_mode,
    }

    t_dist_start = time.perf_counter()
    send_msg(sock, task_payload)

    # Master komputasi lokal murni (T_m)
    tm0 = time.perf_counter()
    c1 = count_primes(m_start, m_end)
    tm1 = time.perf_counter()
    t_m = tm1 - tm0

    # Terima respons dari Worker fisik (T_w)
    worker_resp = recv_msg(sock)
    if worker_resp is None:
        raise ConnectionResetError("Worker menutup koneksi sebelum mengirimkan RESULT.")
    if worker_resp.get("type") == "ERROR":
        raise RuntimeError(f"Worker melaporkan error: {worker_resp.get('message')}")
    if worker_resp.get("type") != "RESULT":
        raise RuntimeError(f"Respons tidak dikenal dari Worker: {worker_resp}")

    c2 = worker_resp["prime_count"]
    t_w = worker_resp["compute_time_s"]
    total_primes = c1 + c2

    t_dist_end = time.perf_counter()
    t_dist = t_dist_end - t_dist_start
    t_comm = max(0.0, t_dist - max(t_m, t_w))

    return {
        "master_start": m_start,
        "master_end": m_end,
        "worker_start": w_start,
        "worker_end": w_end,
        "master_range_size": m_end - m_start + 1,
        "worker_range_size": w_end - w_start + 1,
        "c1": c1,
        "c2": c2,
        "total_primes": total_primes,
        "t_m": t_m,
        "t_w": t_w,
        "t_dist": t_dist,
        "t_comm": t_comm,
    }


# =====================================================================
# 4. TABEL RENDERING (TABEL A, TABEL B, TABEL C)
# =====================================================================


def print_table_a(scenario_data: list[dict[str, Any]]) -> None:
    """
    TABEL A: HASIL PENGUJIAN PERFORMA SETIAP SKENARIO
    Kolom: Skenario (N) | T_seq (s) | T_m (s) | T_dist (s) | Speedup | Eff (%)
    """
    print("\n### TABEL A: HASIL PENGUJIAN PERFORMA SETIAP SKENARIO\n")
    print("| Skenario (N) | T_seq (s) | T_m (s) | T_dist (s) | Speedup | Eff (%) |")
    print("|-------------:|----------:|--------:|-----------:|--------:|--------:|")

    for item in scenario_data:
        n_str = format_id_num(item["n"])
        t_seq = f"{item['t_seq_mean']:.6f}"
        t_m = f"{item['t_m_mean']:.6f}"
        t_dist = f"{item['t_dist_mean']:.6f}"
        speedup = f"{item['speedup_mean']:.2f}x"
        eff = f"{item['eff_mean']:.2f}%"
        print(f"| {n_str:>12} | {t_seq:>9} | {t_m:>7} | {t_dist:>10} | {speedup:>7} | {eff:>7} |")
    print()


def print_table_b(distribution_data: list[dict[str, Any]]) -> None:
    """
    TABEL B: DISTRIBUSI BEBAN KERJA (EQUAL VS WEIGHTED)
    Kolom: N | Strategi | Beban PC 1 | Beban PC 2 | T_m (s) | T_w (s) | Rasio T_m : T_w
    """
    print("\n### TABEL B: DISTRIBUSI BEBAN KERJA (EQUAL VS WEIGHTED)\n")
    print("|          N | Strategi | Beban PC 1 | Beban PC 2 |   T_m (s) |   T_w (s) | Rasio T_m : T_w |")
    print("|-----------:|:---------|-----------:|-----------:|----------:|----------:|:----------------|")

    for row in distribution_data:
        n_str = format_id_num(row["n"])
        strat = row["strategy"]
        load1 = format_id_num(row["load_pc1"])
        load2 = format_id_num(row["load_pc2"])
        t_m = f"{row['t_m_mean']:.6f}"
        t_w = f"{row['t_w_mean']:.6f}"
        ratio = row["ratio_str"]
        print(f"| {n_str:>10} | {strat:<8} | {load1:>10} | {load2:>10} | {t_m:>9} | {t_w:>9} | {ratio:<15} |")
    print()


def print_table_c(runs_data: list[dict[str, Any]]) -> None:
    """
    TABEL C: STATISTIK WAKTU EKSEKUSI ANTAR-RUN
    Kolom: N | Mode | Run 1 (s) | Run 2 (s) | Run 3 (s) | Run 4 (s) | Run 5 (s) | Rata-rata (s) | Median (s) | Std. Dev (s)
    """
    print("\n### TABEL C: STATISTIK WAKTU EKSEKUSI ANTAR-RUN\n")
    print("|          N | Mode          | Run 1 (s) | Run 2 (s) | Run 3 (s) | Run 4 (s) | Run 5 (s) | Rata-rata (s) | Median (s) | Std. Dev (s) |")
    print("|-----------:|:--------------|----------:|----------:|----------:|----------:|----------:|--------------:|-----------:|-------------:|")

    for row in runs_data:
        n_str = format_id_num(row["n"])
        mode_str = row["mode"]
        runs = row["runs"]
        r_strs = [f"{r:.6f}" for r in runs[:5]]
        while len(r_strs) < 5:
            r_strs.append("-")

        mean_str = f"{row['mean']:.6f}"
        med_str = f"{row['median']:.6f}"
        std_str = f"{row['pstdev']:.6f}"

        print(
            f"| {n_str:>10} | {mode_str:<13} | "
            f"{r_strs[0]:>9} | {r_strs[1]:>9} | {r_strs[2]:>9} | {r_strs[3]:>9} | {r_strs[4]:>9} | "
            f"{mean_str:>13} | {med_str:>10} | {std_str:>12} |"
        )
    print()


# =====================================================================
# 5. SUMMARY DARI CSV FILE (--summary)
# =====================================================================


def print_report_summary_from_csv(csv_path: str) -> None:
    """
    Membaca data dari results.csv dan menyusun Tabel A, B, dan C secara otomatis.
    """
    if not os.path.isfile(csv_path):
        print(f"[ERROR] File '{csv_path}' belum ditemukan.")
        print("Silakan jalankan pengujian benchmark terlebih dahulu.")
        return

    # Struktur penyimpanan: grouped[n][mode_key] = list of rows
    # mode_key: 'seq', 'dist_weighted', 'dist_equal'
    records: list[dict[str, str]] = []
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            records.append(r)

    if not records:
        print(f"[INFO] File '{csv_path}' kosong.")
        return

    # Ambil nilai N unik
    unique_ns = sorted(list({int(r["n"]) for r in records if r.get("n")}))

    scenario_a: list[dict[str, Any]] = []
    table_b: list[dict[str, Any]] = []
    table_c: list[dict[str, Any]] = []

    for n_val in unique_ns:
        # Filter rows untuk n_val
        n_rows = [r for r in records if int(r.get("n", 0)) == n_val]

        seq_rows = [r for r in n_rows if r.get("mode") == "seq"][-5:]
        dist_w_rows = [r for r in n_rows if r.get("mode") == "dist" and r.get("split") == "weighted"][-5:]
        dist_e_rows = [r for r in n_rows if r.get("mode") == "dist" and r.get("split") == "equal"][-5:]

        seq_times = [float(r["t_seq"]) for r in seq_rows if r.get("t_seq")]
        dw_dist_times = [float(r["t_dist"]) for r in dist_w_rows if r.get("t_dist")]
        dw_tm_times = [float(r["t_master"]) for r in dist_w_rows if r.get("t_master")]
        dw_tw_times = [float(r["t_worker"]) for r in dist_w_rows if r.get("t_worker")]
        dw_speedups = [float(r["speedup"]) for r in dist_w_rows if r.get("speedup")]
        dw_effs = [float(r["efficiency"]) for r in dist_w_rows if r.get("efficiency")]

        de_dist_times = [float(r["t_dist"]) for r in dist_e_rows if r.get("t_dist")]
        de_tm_times = [float(r["t_master"]) for r in dist_e_rows if r.get("t_master")]
        de_tw_times = [float(r["t_worker"]) for r in dist_e_rows if r.get("t_worker")]

        # 1. Tabel A (menggunakan weighted sebagai konfigurasi optimal distributed)
        if seq_times and dw_dist_times:
            mean_seq = statistics.mean(seq_times)
            mean_dist = statistics.mean(dw_dist_times)
            mean_tm = statistics.mean(dw_tm_times) if dw_tm_times else 0.0
            mean_sp = statistics.mean(dw_speedups) if dw_speedups else (mean_seq / mean_dist if mean_dist > 0 else 0.0)
            mean_ef = statistics.mean(dw_effs) if dw_effs else ((mean_sp / 2.0) * 100.0)

            scenario_a.append({
                "n": n_val,
                "t_seq_mean": mean_seq,
                "t_m_mean": mean_tm,
                "t_dist_mean": mean_dist,
                "speedup_mean": mean_sp,
                "eff_mean": mean_ef,
            })

        # 2. Tabel B (Equal & Weighted)
        m_s_eq, m_e_eq, w_s_eq, w_e_eq = split_range(n_val, "equal")
        load_pc1_eq = m_e_eq - m_s_eq + 1
        load_pc2_eq = w_e_eq - w_s_eq + 1

        m_s_wt, m_e_wt, w_s_wt, w_e_wt = split_range(n_val, "weighted")
        load_pc1_wt = m_e_wt - m_s_wt + 1
        load_pc2_wt = w_e_wt - w_s_wt + 1

        if de_tm_times and de_tw_times:
            tm_eq = statistics.mean(de_tm_times)
            tw_eq = statistics.mean(de_tw_times)
            table_b.append({
                "n": n_val,
                "strategy": "Equal",
                "load_pc1": load_pc1_eq,
                "load_pc2": load_pc2_eq,
                "t_m_mean": tm_eq,
                "t_w_mean": tw_eq,
                "ratio_str": format_ratio_tm_tw(tm_eq, tw_eq),
            })

        if dw_tm_times and dw_tw_times:
            tm_wt = statistics.mean(dw_tm_times)
            tw_wt = statistics.mean(dw_tw_times)
            table_b.append({
                "n": n_val,
                "strategy": "Weighted",
                "load_pc1": load_pc1_wt,
                "load_pc2": load_pc2_wt,
                "t_m_mean": tm_wt,
                "t_w_mean": tw_wt,
                "ratio_str": format_ratio_tm_tw(tm_wt, tw_wt),
            })

        # 3. Tabel C (Runs statistics)
        if seq_times:
            st = calculate_stats(seq_times)
            table_c.append({
                "n": n_val,
                "mode": "seq",
                "runs": seq_times,
                "mean": st["mean"],
                "median": st["median"],
                "pstdev": st["pstdev"],
            })

        if dw_dist_times:
            st = calculate_stats(dw_dist_times)
            table_c.append({
                "n": n_val,
                "mode": "dist-weighted",
                "runs": dw_dist_times,
                "mean": st["mean"],
                "median": st["median"],
                "pstdev": st["pstdev"],
            })

        if de_dist_times:
            st = calculate_stats(de_dist_times)
            table_c.append({
                "n": n_val,
                "mode": "dist-equal",
                "runs": de_dist_times,
                "mean": st["mean"],
                "median": st["median"],
                "pstdev": st["pstdev"],
            })

    print("==========================================================================================")
    print("DISTRIBUTED PRIME RANGE COUNTER — BENCHMARK REPORT SUMMARY")
    print(f"Data source: {csv_path}")
    print("==========================================================================================")

    if scenario_a:
        print_table_a(scenario_a)
    if table_b:
        print_table_b(table_b)
    if table_c:
        print_table_c(table_c)


# =====================================================================
# 6. MAIN BENCHMARK EXECUTION
# =====================================================================


def run_benchmark(
    host: str,
    port: int,
    runs: int,
    split_option: str,
    sizes: list[int],
    timeout: float,
    output_csv: str,
) -> None:
    if runs < 1:
        print("ERROR: Jumlah runs harus >= 1.")
        sys.exit(1)

    splits_to_test = ["weighted", "equal"] if split_option == "both" else [split_option]

    print("==========================================================================================")
    print("DISTRIBUTED PRIME RANGE COUNTER — AUTOMATED BENCHMARK SUITE")
    print("==========================================================================================")
    print(f"Target Worker Node  : {host}:{port}")
    print(f"Split Strategies    : {', '.join(splits_to_test)}")
    print(f"Runs per Mode       : {runs}x (+ 1 warm-up)")
    print(f"Data Sizes (N)      : {', '.join(format_id_num(sz) for sz in sizes)}")
    print(f"Output File         : {output_csv}")
    print("==========================================================================================\n")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)

    try:
        print(f"[CONNECT] Menghubungkan ke Worker node di {host}:{port}...")
        sock.connect((host, port))
        print("[CONNECTED] Koneksi TCP ke Worker berhasil diverifikasi.\n")
    except ConnectionRefusedError:
        print("ERROR: Cannot connect to Worker.")
        print(f"Make sure PC2 is running worker.py on port {port}.")
        sys.exit(1)
    except (socket.timeout, TimeoutError):
        print("ERROR: Worker address is unreachable.")
        print("Check the IP address, port configuration, and LAN connection.")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Cannot connect to port {port}. Check Worker configuration and firewall ({e}).")
        sys.exit(1)

    # Penampung data untuk pencetakan tabel
    scenario_a_list: list[dict[str, Any]] = []
    table_b_list: list[dict[str, Any]] = []
    table_c_list: list[dict[str, Any]] = []

    try:
        for idx, n in enumerate(sizes, start=1):
            sc_name = f"S{idx}"
            print("==========================================================================================")
            print(f"SKENARIO {sc_name}: N = {format_id_num(n)}")
            print("==========================================================================================")

            # -------------------------------------------------------------
            # A. WARM-UP RUN (1x per skenario, tidak dihitung ke hasil/CSV)
            # -------------------------------------------------------------
            print("[WARM-UP] Menjalankan warm-up (tidak dicatat ke statistik/CSV)...")
            execute_sequential_run(n)
            for sm in splits_to_test:
                execute_distributed_run(
                    sock, n, sm, f"warmup-{sc_name}-{sm}-{int(time.time())}"
                )
            print("[WARM-UP] Selesai.\n")

            # -------------------------------------------------------------
            # B. SEQUENTIAL RUNS (5x)
            # -------------------------------------------------------------
            seq_times: list[float] = []
            seq_counts: list[int] = []

            print(f"[1/3] Menjalankan {runs}x pengujian SEQUENTIAL...")
            for r in range(1, runs + 1):
                count, t_seq = execute_sequential_run(n)
                seq_times.append(t_seq)
                seq_counts.append(count)
                print(f"  Run {r}/{runs} (Seq) : {count:,} prima | T_seq = {t_seq:.6f} s")

                append_csv_row(
                    output_csv,
                    {
                        "timestamp": datetime.datetime.now().isoformat(),
                        "scenario": sc_name,
                        "run": r,
                        "n": n,
                        "mode": "seq",
                        "split": "",
                        "master_start": 1,
                        "master_end": n,
                        "worker_start": "",
                        "worker_end": "",
                        "master_range_size": n,
                        "worker_range_size": "",
                        "master_prime_count": count,
                        "worker_prime_count": "",
                        "final_prime_count": count,
                        "t_seq": f"{t_seq:.6f}",
                        "t_master": "",
                        "t_worker": "",
                        "t_dist": "",
                        "t_comm": "",
                        "speedup": "",
                        "efficiency": "",
                        "validation": "",
                    },
                )

            seq_st = calculate_stats(seq_times)
            table_c_list.append({
                "n": n,
                "mode": "seq",
                "runs": seq_times,
                "mean": seq_st["mean"],
                "median": seq_st["median"],
                "pstdev": seq_st["pstdev"],
            })

            # -------------------------------------------------------------
            # C. DISTRIBUTED RUNS (WEIGHTED & EQUAL)
            # -------------------------------------------------------------
            dist_results_by_split: dict[str, dict[str, Any]] = {}

            step_counter = 2
            for s_mode in splits_to_test:
                print(f"\n[{step_counter}/3] Menjalankan {runs}x pengujian DISTRIBUTED ({s_mode.upper()})...")
                step_counter += 1

                d_times: list[float] = []
                tm_times: list[float] = []
                tw_times: list[float] = []
                tcomm_times: list[float] = []
                speedups: list[float] = []
                efficiencies: list[float] = []
                dist_counts: list[int] = []

                for r in range(1, runs + 1):
                    task_id = f"bench-{sc_name}-{s_mode}-run{r}-{int(time.time())}"
                    d_res = execute_distributed_run(sock, n, s_mode, task_id)

                    t_seq_paired = seq_times[r - 1]
                    t_dist = d_res["t_dist"]
                    speedup = (t_seq_paired / t_dist) if t_dist > 0 else 0.0
                    efficiency = (speedup / 2.0) * 100.0

                    is_match = d_res["total_primes"] == seq_counts[r - 1]
                    val_status = "MATCH" if is_match else "MISMATCH"

                    d_times.append(t_dist)
                    tm_times.append(d_res["t_m"])
                    tw_times.append(d_res["t_w"])
                    tcomm_times.append(d_res["t_comm"])
                    speedups.append(speedup)
                    efficiencies.append(efficiency)
                    dist_counts.append(d_res["total_primes"])

                    print(
                        f"  Run {r}/{runs} ({s_mode}) : {d_res['total_primes']:,} prima "
                        f"| T_dist = {t_dist:.6f} s | T_m = {d_res['t_m']:.6f} s | T_w = {d_res['t_w']:.6f} s "
                        f"| Speedup = {speedup:.2f}x | Eff = {efficiency:.2f}% | Val = {val_status}"
                    )

                    append_csv_row(
                        output_csv,
                        {
                            "timestamp": datetime.datetime.now().isoformat(),
                            "scenario": sc_name,
                            "run": r,
                            "n": n,
                            "mode": "dist",
                            "split": s_mode,
                            "master_start": d_res["master_start"],
                            "master_end": d_res["master_end"],
                            "worker_start": d_res["worker_start"],
                            "worker_end": d_res["worker_end"],
                            "master_range_size": d_res["master_range_size"],
                            "worker_range_size": d_res["worker_range_size"],
                            "master_prime_count": d_res["c1"],
                            "worker_prime_count": d_res["c2"],
                            "final_prime_count": d_res["total_primes"],
                            "t_seq": f"{t_seq_paired:.6f}",
                            "t_master": f"{d_res['t_m']:.6f}",
                            "t_worker": f"{d_res['t_w']:.6f}",
                            "t_dist": f"{t_dist:.6f}",
                            "t_comm": f"{d_res['t_comm']:.6f}",
                            "speedup": f"{speedup:.4f}",
                            "efficiency": f"{efficiency:.2f}",
                            "validation": val_status,
                        },
                    )

                dist_results_by_split[s_mode] = {
                    "d_times": d_times,
                    "tm_times": tm_times,
                    "tw_times": tw_times,
                    "speedups": speedups,
                    "efficiencies": efficiencies,
                    "last_res": d_res,
                }

                # Catat ke Tabel C
                dst = calculate_stats(d_times)
                table_c_list.append({
                    "n": n,
                    "mode": f"dist-{s_mode}",
                    "runs": d_times,
                    "mean": dst["mean"],
                    "median": dst["median"],
                    "pstdev": dst["pstdev"],
                })

                # Catat ke Tabel B
                strat_label = "Weighted" if s_mode == "weighted" else "Equal"
                tm_m = statistics.mean(tm_times)
                tw_m = statistics.mean(tw_times)
                table_b_list.append({
                    "n": n,
                    "strategy": strat_label,
                    "load_pc1": d_res["master_range_size"],
                    "load_pc2": d_res["worker_range_size"],
                    "t_m_mean": tm_m,
                    "t_w_mean": tw_m,
                    "ratio_str": format_ratio_tm_tw(tm_m, tw_m),
                })

            # Data untuk Tabel A: Menggunakan weighted jika ada, atau mode yang dipilih
            primary_dist = "weighted" if "weighted" in dist_results_by_split else splits_to_test[0]
            p_res = dist_results_by_split[primary_dist]
            scenario_a_list.append({
                "n": n,
                "t_seq_mean": seq_st["mean"],
                "t_m_mean": statistics.mean(p_res["tm_times"]),
                "t_dist_mean": statistics.mean(p_res["d_times"]),
                "speedup_mean": statistics.mean(p_res["speedups"]),
                "eff_mean": statistics.mean(p_res["efficiencies"]),
            })

            print()

        # D. GRACEFUL SHUTDOWN WORKER
        print("[CLEANUP] Mengirimkan sinyal STOP ke Worker di PC 2...")
        send_msg(sock, {"type": "STOP"})
        ack = recv_msg(sock)
        if ack and (ack.get("type") == "ACK" or ack.get("action") == "ACK"):
            print("[CLEANUP] Worker shutdown terkonfirmasi secara graceful.")

    except ConnectionResetError:
        print("[ERROR] Koneksi diputus tiba-tiba oleh Worker saat benchmark berlangsung.")
    except socket.timeout:
        print(f"[ERROR] Socket timeout ({timeout}s) tercapai saat menunggu respon Worker.")
    except Exception as e:
        print(f"[ERROR] Terjadi kesalahan dalam eksekusi benchmark: {e}")
    finally:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()
        print("[CLEANUP] Koneksi soket ditutup.\n")

    # =====================================================================
    # 7. CETAK TABEL HASIL BENCHMARK
    # =====================================================================
    print("==========================================================================================")
    print("HASIL LENGKAP BENCHMARK DISTRIBUTED PRIME RANGE COUNTER")
    print("==========================================================================================")

    if scenario_a_list:
        print_table_a(scenario_a_list)
    if table_b_list:
        print_table_b(table_b_list)
    if table_c_list:
        print_table_c(table_c_list)

    print("==========================================================================================")
    print(f"Seluruh log hasil tersimpan permanen di: {output_csv}")
    print("Gunakan 'python benchmark.py --summary' untuk melihat kembali tabel ringkasan kapan saja.")
    print("==========================================================================================")


def main() -> None:
    args = parse_arguments()
    if args.summary:
        print_report_summary_from_csv(args.output)
        return

    if not args.host:
        print("ERROR: Worker address is unreachable.")
        print("Argumen --host diperlukan untuk menjalankan benchmark.")
        print("Contoh: python benchmark.py --host 192.168.1.11 --port 65432")
        print("Atau gunakan --summary untuk membaca data CSV yang ada: python benchmark.py --summary")
        sys.exit(1)

    run_benchmark(
        host=args.host,
        port=args.port,
        runs=args.runs,
        split_option=args.split,
        sizes=args.sizes,
        timeout=args.timeout,
        output_csv=args.output,
    )


if __name__ == "__main__":
    main()
