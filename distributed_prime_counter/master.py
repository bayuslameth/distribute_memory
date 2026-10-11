"""
master.py - Node Master untuk Distributed Prime Range Counter (Phase 5 - Final Polish).

Mendukung 2 mode eksekusi:
1. SEQUENTIAL (--mode seq):
   - Menghitung seluruh rentang [1, N] secara lokal di PC 1 sebagai baseline performa (T_seq).
2. DISTRIBUTED (--mode dist):
   - Membagi rentang [1, N] menggunakan strategi partisi 'weighted' (default) atau 'equal'.
   - Mengirim partisi Worker ke PC 2 via TCP socket LAN.
   - Menghitung partisi Master secara serentak di PC 1.
   - Menerima hasil komputasi Worker (C2 & T_w).
   - Menghitung metrik performa: T_dist, T_comm, Speedup, dan Efficiency.
   - Melakukan validasi hasil Distributed == Sequential.
"""

import argparse
import socket
import sys
import time
from common import count_primes, recv_msg, send_msg, split_range


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Master node untuk Distributed Prime Range Counter (Phase 5)"
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["seq", "dist"],
        default="dist",
        help="Mode eksekusi: 'seq' (Sequential Baseline) atau 'dist' (Distributed Memory, default: dist)",
    )
    parser.add_argument(
        "--n",
        type=int,
        default=2000000,
        help="Batas atas pencarian bilangan prima N (default: 2000000, minimum: 2)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Alamat IP LAN dari Worker node di PC 2 (wajib untuk mode dist)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=65432,
        help="Port TCP Worker node (default: 65432)",
    )
    parser.add_argument(
        "--split",
        type=str,
        choices=["weighted", "equal"],
        default="weighted",
        help="Strategi partisi data: 'weighted' (default) atau 'equal'",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="Batas waktu soket dalam detik (default: 120.0)",
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Lewati verifikasi sequential internal pada mode distributed",
    )
    return parser.parse_args()


# =====================================================================
# 1. SEQUENTIAL EXECUTION (BASELINE)
# =====================================================================


def run_sequential(n: int) -> None:
    if n < 2:
        print(f"ERROR: Nilai N harus integer >= 2 (diterima: {n}).")
        sys.exit(1)

    print("============================================================")
    print("DISTRIBUTED PRIME RANGE COUNTER")
    print("============================================================")
    print("Role        : MASTER")
    print("Mode        : SEQUENTIAL")
    print(f"N           : {n:,}")
    print(f"Range       : [1 .. {n:,}]")
    print()
    print("Computing...")

    # Pengukuran T_seq murni (hanya membungkus count_primes)
    t0 = time.perf_counter()
    prime_count = count_primes(1, n)
    t1 = time.perf_counter()
    t_seq = t1 - t0

    print()
    print("RESULT")
    print(f"Prime count : {prime_count:,}")
    print(f"Compute time: {t_seq:.6f} s")
    print()
    print("STATUS")
    print("Sequential computation : SUCCESS")
    print("============================================================")


# =====================================================================
# 2. DISTRIBUTED EXECUTION
# =====================================================================


def run_distributed(
    host: str, port: int, n: int, split_mode: str, timeout: float, skip_verify: bool
) -> None:
    if n < 2:
        print(f"ERROR: Nilai N harus integer >= 2 (diterima: {n}).")
        sys.exit(1)

    if not host:
        print("ERROR: Worker address is unreachable.")
        print("Argumen --host diperlukan untuk mode distributed.")
        print(f"Contoh: python master.py --mode dist --host 192.168.1.11 --port {port} --n {n}")
        sys.exit(1)

    # 1. Partisi data
    m_start, m_end, w_start, w_end = split_range(n, split_mode)
    m_workload = m_end - m_start + 1
    w_workload = w_end - w_start + 1

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)

    try:
        t_dist_start = time.perf_counter()
        sock.connect((host, port))

        # Kirim TASK ke Worker
        task_id = f"task-n{n}-{split_mode}-{int(time.time())}"
        task_payload = {
            "type": "TASK",
            "task_id": task_id,
            "n_total": n,
            "start": w_start,
            "end": w_end,
            "split_mode": split_mode,
        }
        send_msg(sock, task_payload)

        # Komputasi mandiri Master (T_m) berjalan serentak bersama Worker
        tm0 = time.perf_counter()
        c1 = count_primes(m_start, m_end)
        tm1 = time.perf_counter()
        t_m = tm1 - tm0

        # Terima RESULT dari Worker
        worker_resp = recv_msg(sock)
        if worker_resp is None:
            raise ConnectionResetError("Worker menutup koneksi sebelum mengirimkan RESULT.")

        if worker_resp.get("type") == "ERROR":
            print(f"ERROR: Invalid worker result: {worker_resp.get('message')}")
            sys.exit(1)

        if worker_resp.get("type") != "RESULT":
            print("ERROR: Invalid worker result.")
            sys.exit(1)

        c2 = worker_resp["prime_count"]
        t_w = worker_resp["compute_time_s"]
        final_primes = c1 + c2

        t_dist_end = time.perf_counter()
        t_dist = t_dist_end - t_dist_start

        # Graceful shutdown Worker
        send_msg(sock, {"type": "STOP"})
        recv_msg(sock)

    except ConnectionRefusedError:
        print("ERROR: Cannot connect to Worker.")
        print("Make sure PC2 is running worker.py.")
        sys.exit(1)
    except (socket.timeout, TimeoutError):
        print("ERROR: Worker address is unreachable.")
        print("Check the IP address, port configuration, and LAN connection.")
        sys.exit(1)
    except ConnectionResetError:
        print("ERROR: Connection reset by Worker.")
        sys.exit(1)
    except OSError as e:
        print(f"ERROR: Cannot connect to port {port}. Check Worker configuration and firewall ({e}).")
        sys.exit(1)
    finally:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()

    t_comm = max(0.0, t_dist - max(t_m, t_w))

    # -------------------------------------------------------------
    # 3. VERIFIKASI SEQUENTIAL (DIPISAHKAN DARI T_dist)
    # -------------------------------------------------------------
    t_seq_val = None
    seq_result_str = "N/A"
    val_status_str = "NOT VERIFIED"

    if not skip_verify:
        t_seq0 = time.perf_counter()
        seq_primes = count_primes(1, n)
        t_seq1 = time.perf_counter()
        t_seq_val = t_seq1 - t_seq0

        seq_result_str = f"{seq_primes:,}"
        if seq_primes == final_primes:
            val_status_str = "MATCH"
        else:
            val_status_str = "MISMATCH"

    speedup_str = "N/A"
    efficiency_str = "N/A"
    t_seq_display = "N/A"

    if t_seq_val is not None and t_dist > 0:
        speedup = t_seq_val / t_dist
        efficiency = (speedup / 2.0) * 100.0
        speedup_str = f"{speedup:.2f} x"
        efficiency_str = f"{efficiency:.2f} %"
        t_seq_display = f"{t_seq_val:.6f} s"

    # =============================================================
    # 4. CETAK OUTPUT DEMO RESMI
    # =============================================================
    print("============================================================")
    print("DISTRIBUTED PRIME RANGE COUNTER")
    print("============================================================")
    print()
    print("Role        : MASTER")
    print(f"Worker      : {host}:{port}")
    print(f"N           : {n:,}")
    print(f"Split       : {split_mode}")
    print()
    print("------------------------------------------------------------")
    print("WORKLOAD")
    print("------------------------------------------------------------")
    print()
    print(f"Master : [{m_start:,}, {m_end:,}]")
    print(f"Worker : [{w_start:,}, {w_end:,}]")
    print()
    print(f"Master workload : {m_workload:,}")
    print(f"Worker workload : {w_workload:,}")
    print()
    print("------------------------------------------------------------")
    print("RESULT")
    print("------------------------------------------------------------")
    print()
    print(f"Master prime count : {c1:,}")
    print(f"Worker prime count : {c2:,}")
    print()
    print(f"Final prime count   : {final_primes:,}")
    print()
    print(f"Sequential result   : {seq_result_str}")
    print(f"Validation           : {val_status_str}")
    print()
    print("------------------------------------------------------------")
    print("TIMING")
    print("------------------------------------------------------------")
    print()
    print(f"Master compute : {t_m:.6f} s")
    print(f"Worker compute : {t_w:.6f} s")
    print(f"Communication  : {t_comm:.6f} s")
    print(f"Distributed    : {t_dist:.6f} s")
    print(f"Sequential     : {t_seq_display}")
    print()
    print(f"Speedup        : {speedup_str}")
    print(f"Efficiency     : {efficiency_str}")
    print()
    print("------------------------------------------------------------")
    print("STATUS")
    print("------------------------------------------------------------")
    print()
    print("Distributed computation : SUCCESS")
    print(f"Validation              : {val_status_str}")
    print()
    print("============================================================")


def main() -> None:
    args = parse_arguments()
    if args.mode == "seq":
        run_sequential(args.n)
    else:
        run_distributed(
            host=args.host,
            port=args.port,
            n=args.n,
            split_mode=args.split,
            timeout=args.timeout,
            skip_verify=args.skip_verify,
        )


if __name__ == "__main__":
    main()
