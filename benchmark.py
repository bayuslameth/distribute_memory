"""
benchmark.py - Automated Benchmark Suite untuk Distributed Prime Range Counter (Phase 5 - Final Polish).

Melakukan pengujian komparatif performa:
1. Mode Sequential (T_seq) vs Distributed (T_dist).
2. Tiga skenario ukuran data:
   - S1: N = 2,000,000  (Reference prime count: 148,933)
   - S2: N = 6,000,000  (Reference prime count: 412,849)
   - S3: N = 12,000,000 (Reference prime count: 788,060)
3. 1 warm-up run per skenario (tidak dimasukkan ke statistik/CSV).
4. Minimal 5 kali pengulangan (runs = 5).
5. Perhitungan statistik lengkap: mean, median, standard deviation, min, max.
6. Perhitungan Speedup (T_seq / T_dist) dan Efficiency ((Speedup / 2) * 100%).
7. Validasi silang: Distributed vs Sequential (MATCH/MISMATCH) & Reference Ground Truth.
8. Penyimpanan otomatis log eksekusi ke results.csv (mode append).
9. Opsi --summary untuk menampilkan tabel ringkasan laporan langsung dari results.csv.
"""

import argparse
import csv
import datetime
import os
import socket
import statistics
import sys
import time
from typing import Any
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
        description="Automated Benchmark Suite untuk Distributed Prime Range Counter (Phase 5)"
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
        choices=["weighted", "equal"],
        default="weighted",
        help="Strategi partisi data distributed: 'weighted' (default) atau 'equal'",
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
        default=300.0,
        help="Timeout soket TCP dalam detik (default: 300.0)",
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
        help="Tampilkan tabel ringkasan laporan dari file results.csv tanpa menjalankan benchmark",
    )
    return parser.parse_args()


# =====================================================================
# 1. STATISTICAL HELPER
# =====================================================================


def calculate_stats(data: list[float]) -> dict[str, float]:
    """
    Menghitung mean, median, stdev, min, dan max dari kumpulan data float.
    Menghindari error StatisticsError jika jumlah data < 2.
    """
    if not data:
        return {"mean": 0.0, "median": 0.0, "stdev": 0.0, "min": 0.0, "max": 0.0}
    mean_val = statistics.mean(data)
    median_val = statistics.median(data)
    stdev_val = statistics.stdev(data) if len(data) >= 2 else 0.0
    return {
        "mean": mean_val,
        "median": median_val,
        "stdev": stdev_val,
        "min": min(data),
        "max": max(data),
    }


# =====================================================================
# 2. CSV LOGGING & REPORT SUMMARY
# =====================================================================


def append_csv_row(csv_path: str, row_dict: dict[str, Any]) -> None:
    """
    Menuliskan baris data ke results.csv secara aman (append).
    Otomatis membuat header jika file belum ada.
    """
    file_exists = os.path.isfile(csv_path)
    with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row_dict)


def print_report_summary(csv_path: str) -> None:
    """
    Membaca results.csv dan menyajikan tabel ringkasan formal untuk laporan tugas.
    """
    if not os.path.isfile(csv_path):
        print(f"[INFO] File '{csv_path}' belum ditemukan. Jalankan benchmark terlebih dahulu.")
        return

    scenarios_data: dict[int, dict[str, list[float]]] = {}
    validation_status: dict[int, str] = {}

    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("mode") != "dist":
                continue
            try:
                n = int(row["n"])
                t_seq = float(row["t_seq"])
                t_dist = float(row["t_dist"])
                speedup = float(row["speedup"])
                eff = float(row["efficiency"])
                val = row["validation"]
            except (ValueError, KeyError):
                continue

            if n not in scenarios_data:
                scenarios_data[n] = {
                    "t_seq": [],
                    "t_dist": [],
                    "speedup": [],
                    "eff": [],
                }
                validation_status[n] = "MATCH"

            scenarios_data[n]["t_seq"].append(t_seq)
            scenarios_data[n]["t_dist"].append(t_dist)
            scenarios_data[n]["speedup"].append(speedup)
            scenarios_data[n]["eff"].append(eff)
            if val != "MATCH":
                validation_status[n] = "MISMATCH"

    if not scenarios_data:
        print(f"[INFO] Belum ada data distributed run di '{csv_path}'.")
        return

    print("========================================================================================")
    print("DISTRIBUTED PRIME RANGE COUNTER — BENCHMARK REPORT TABLE")
    print("========================================================================================")
    print(f"{'N':>12} | {'Mean T_seq':>12} | {'Mean T_dist':>12} | {'Speedup':>9} | {'Efficiency':>10} | {'Validation':<10}")
    print("-" * 88)

    for n in sorted(scenarios_data.keys()):
        data = scenarios_data[n]
        mean_seq = statistics.mean(data["t_seq"]) if data["t_seq"] else 0.0
        mean_dist = statistics.mean(data["t_dist"]) if data["t_dist"] else 0.0
        mean_sp = statistics.mean(data["speedup"]) if data["speedup"] else 0.0
        mean_ef = statistics.mean(data["eff"]) if data["eff"] else 0.0
        val = validation_status.get(n, "MATCH")

        print(
            f"{n:>12,} | {mean_seq:>10.6f} s | {mean_dist:>10.6f} s | {mean_sp:>7.2f}x | {mean_ef:>8.2f} % | {val:<10}"
        )

    print("========================================================================================")


# =====================================================================
# 3. BENCHMARK EXECUTION HELPERS
# =====================================================================


def execute_sequential_run(n: int) -> tuple[int, float]:
    """
    Mengeksekusi perhitungan sequential [1, N] dan mengukur T_seq murni.
    """
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

    # Terima respons dari Worker fisik
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
# 4. MAIN BENCHMARK ORCHESTRATION
# =====================================================================


def run_benchmark(
    host: str,
    port: int,
    runs: int,
    split_mode: str,
    sizes: list[int],
    timeout: float,
    output_csv: str,
) -> None:
    if runs < 1:
        print("ERROR: Jumlah runs harus >= 1.")
        sys.exit(1)

    print("============================================================")
    print("DISTRIBUTED PRIME RANGE COUNTER — BENCHMARK SUITE")
    print("============================================================")
    print(f"Target Worker       : {host}:{port}")
    print(f"Split Strategy      : {split_mode}")
    print(f"Runs per Scenario   : {runs}x")
    print(f"Scenarios (N)       : {', '.join(f'{sz:,}' for sz in sizes)}")
    print(f"Output CSV          : {output_csv}")
    print("============================================================\n")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)

    try:
        print(f"[CONNECT] Menghubungkan ke Worker di {host}:{port}...")
        sock.connect((host, port))
        print("[CONNECTED] Koneksi ke Worker berhasil diverifikasi.\n")
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

    scenario_summaries: list[dict[str, Any]] = []

    try:
        for idx, n in enumerate(sizes, start=1):
            sc_name = f"S{idx}"
            print("------------------------------------------------------------")
            print(f"SCENARIO {sc_name}: N = {n:,}")
            print("------------------------------------------------------------")

            # A. WARM-UP RUN (1x per skenario, tidak dihitung ke hasil/CSV)
            print("[WARM-UP] Menjalankan 1 warm-up run (tidak masuk statistik)...")
            execute_sequential_run(n)
            execute_distributed_run(
                sock, n, split_mode, f"warmup-{sc_name}-{int(time.time())}"
            )
            print("[WARM-UP] Selesai.\n")

            # B. STEP A: SEQUENTIAL RUNS
            seq_times: list[float] = []
            seq_counts: list[int] = []

            print(f"[BENCHMARK] Menjalankan {runs}x pengujian Sequential...")
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

            # C. STEP B: DISTRIBUTED RUNS
            dist_times: list[float] = []
            tm_times: list[float] = []
            tw_times: list[float] = []
            tcomm_times: list[float] = []
            speedups: list[float] = []
            efficiencies: list[float] = []
            dist_counts: list[int] = []
            scenario_matches = True

            print(f"\n[BENCHMARK] Menjalankan {runs}x pengujian Distributed ({split_mode})...")
            for r in range(1, runs + 1):
                task_id = f"bench-{sc_name}-run{r}-{int(time.time())}"
                d_res = execute_distributed_run(sock, n, split_mode, task_id)

                t_seq_paired = seq_times[r - 1]
                t_dist = d_res["t_dist"]
                speedup = (t_seq_paired / t_dist) if t_dist > 0 else 0.0
                efficiency = (speedup / 2.0) * 100.0

                is_match = d_res["total_primes"] == seq_counts[r - 1]
                if not is_match:
                    scenario_matches = False
                val_status = "MATCH" if is_match else "MISMATCH"

                dist_times.append(t_dist)
                tm_times.append(d_res["t_m"])
                tw_times.append(d_res["t_w"])
                tcomm_times.append(d_res["t_comm"])
                speedups.append(speedup)
                efficiencies.append(efficiency)
                dist_counts.append(d_res["total_primes"])

                print(
                    f"  Run {r}/{runs} (Dist): {d_res['total_primes']:,} prima "
                    f"| T_dist = {t_dist:.6f} s | Speedup = {speedup:.2f}x | Eff = {efficiency:.2f}% "
                    f"| Validasi = {val_status}"
                )

                append_csv_row(
                    output_csv,
                    {
                        "timestamp": datetime.datetime.now().isoformat(),
                        "scenario": sc_name,
                        "run": r,
                        "n": n,
                        "mode": "dist",
                        "split": split_mode,
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

            summary = {
                "name": sc_name,
                "n": n,
                "seq_stats": calculate_stats(seq_times),
                "dist_stats": calculate_stats(dist_times),
                "tm_stats": calculate_stats(tm_times),
                "tw_stats": calculate_stats(tw_times),
                "tcomm_stats": calculate_stats(tcomm_times),
                "speedup_stats": calculate_stats(speedups),
                "eff_stats": calculate_stats(efficiencies),
                "seq_count": seq_counts[-1],
                "dist_count": dist_counts[-1],
                "match": scenario_matches,
                "expected": KNOWN_PRIMES.get(n),
            }
            scenario_summaries.append(summary)
            print()

        # D. GRACEFUL SHUTDOWN WORKER
        print("[CLEANUP] Mengirimkan sinyal STOP ke Worker...")
        send_msg(sock, {"type": "STOP"})
        ack = recv_msg(sock)
        if ack and (ack.get("type") == "ACK" or ack.get("action") == "ACK"):
            print("[CLEANUP] Worker shutdown graceful terkonfirmasi.")

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
    # 5. CETAK LAPORAN SUMMARY BENCHMARK RESMI
    # =====================================================================
    print("============================================================")
    print("DISTRIBUTED PRIME RANGE COUNTER — BENCHMARK SUMMARY")
    print("============================================================")
    print(f"Split : {split_mode}")
    print(f"Runs  : {runs}")

    for s in scenario_summaries:
        n_val = s["n"]
        print("\n------------------------------------------------------------")
        print(f"N = {n_val:,} ({s['name']})")
        print("------------------------------------------------------------")

        print("Sequential (T_seq)")
        print(f"Mean   : {s['seq_stats']['mean']:.6f} s")
        print(f"Median : {s['seq_stats']['median']:.6f} s")
        print(f"Std    : {s['seq_stats']['stdev']:.6f} s")

        print("\nDistributed (T_dist)")
        print(f"Mean   : {s['dist_stats']['mean']:.6f} s")
        print(f"Median : {s['dist_stats']['median']:.6f} s")
        print(f"Std    : {s['dist_stats']['stdev']:.6f} s")

        print("\nSpeedup")
        print(f"Mean   : {s['speedup_stats']['mean']:.2f} x")
        print(f"Median : {s['speedup_stats']['median']:.2f} x")
        print(f"Std    : {s['speedup_stats']['stdev']:.4f}")

        print("\nEfficiency")
        print(f"Mean   : {s['eff_stats']['mean']:.2f} %")
        print(f"Median : {s['eff_stats']['median']:.2f} %")
        print(f"Std    : {s['eff_stats']['stdev']:.2f} %")

        print("\nValidation")
        val_text = "MATCH" if s["match"] else "MISMATCH (ERROR)"
        print(f"Sequential vs Distributed : {val_text}")

        if s["expected"] is not None:
            exp_match = s["dist_count"] == s["expected"]
            ref_text = "MATCH" if exp_match else "REFERENCE MISMATCH (WARNING)"
            print(
                f"Reference Ground Truth    : {ref_text} "
                f"(Expected: {s['expected']:,} | Actual: {s['dist_count']:,})"
            )

    print("\n============================================================")
    print(f"RESULTS SAVED TO {output_csv}")
    print("============================================================")


def main() -> None:
    args = parse_arguments()
    if args.summary:
        print_report_summary(args.output)
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
        split_mode=args.split,
        sizes=args.sizes,
        timeout=args.timeout,
        output_csv=args.output,
    )


if __name__ == "__main__":
    main()
