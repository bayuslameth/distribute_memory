"""
worker.py - Node Worker untuk Distributed Prime Range Counter (Phase 5 - Final Polish).

Berperan sebagai TCP Server mandiri yang:
- Bind ke 0.0.0.0:65432 untuk menerima koneksi dari Master di jaringan LAN.
- Menerima TASK range [worker_start, worker_end] dari Master.
- Melakukan komputasi bilangan prima mandiri menggunakan count_primes().
- Mengirim RESULT (prime_count dan compute_time_s) kembali ke Master.
- Menutup sesi secara graceful saat menerima perintah STOP.
"""

import argparse
import socket
import sys
import time
from typing import Any
from common import count_primes, recv_msg, send_msg


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Worker node TCP Server untuk Distributed Prime Range Counter"
    )
    parser.add_argument(
        "--bind",
        type=str,
        default="0.0.0.0",
        help="IP address untuk listen socket (default: 0.0.0.0 untuk semua antarmuka LAN)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=65432,
        help="Port TCP untuk listen (default: 65432)",
    )
    return parser.parse_args()


def validate_task_payload(msg: dict[str, Any]) -> tuple[bool, str]:
    """
    Memvalidasi integritas payload TASK yang diterima dari Master.
    """
    if msg.get("type") != "TASK":
        return False, "Tipe pesan bukan 'TASK'."

    task_id = msg.get("task_id")
    if not task_id or not isinstance(task_id, str):
        return False, "Field 'task_id' tidak valid atau kosong."

    n_total = msg.get("n_total")
    start = msg.get("start")
    end = msg.get("end")

    if not isinstance(n_total, int) or not isinstance(start, int) or not isinstance(end, int):
        return False, "Field 'n_total', 'start', dan 'end' harus bertipe integer."

    if not (1 <= start <= end):
        return False, f"Range tidak valid: harus 1 <= start <= end (start={start}, end={end})."

    if end > n_total:
        return False, f"Range melebihi total data: end ({end}) > n_total ({n_total})."

    return True, ""


def handle_client(client_sock: socket.socket, client_addr: tuple[str, int]) -> bool:
    """
    Menangani sesi komunikasi dengan Master.

    Returns:
        bool: True jika menerima perintah STOP (server berhenti), False jika lanjut listen.
    """
    print(f"\n[ACCEPT] Connection from {client_addr[0]}:{client_addr[1]}")
    should_stop = False

    try:
        while True:
            msg = recv_msg(client_sock)
            if msg is None:
                # Master menutup koneksi
                break

            msg_type = msg.get("type") or msg.get("action") or msg.get("command", "")

            # -------------------------------------------------------------
            # A. HANDSHAKE PING (Kompatibilitas Phase 1)
            # -------------------------------------------------------------
            if msg_type == "PING":
                print("[RECV] PING")
                send_msg(
                    client_sock,
                    {
                        "type": "PONG",
                        "action": "PONG",
                        "status": "OK",
                        "message": "PONG from Worker",
                    },
                )
                print("[SEND] PONG")

            # -------------------------------------------------------------
            # B. TASK COMPUTATION (Phase 2 - 5)
            # -------------------------------------------------------------
            elif msg_type == "TASK":
                is_valid, err_msg = validate_task_payload(msg)
                task_id = msg.get("task_id", "unknown")

                if not is_valid:
                    print(f"\nTASK INVALID: {err_msg}")
                    error_resp = {
                        "type": "ERROR",
                        "task_id": task_id,
                        "status": "ERROR",
                        "message": err_msg,
                    }
                    send_msg(client_sock, error_resp)
                    continue

                start = msg["start"]
                end = msg["end"]
                n_total = msg["n_total"]
                data_count = end - start + 1

                print("\nTASK RECEIVED")
                print(f"Task ID : {task_id}")
                print(f"N       : {n_total:,}")
                print(f"Range   : [{start:,} .. {end:,}]")
                print(f"Workload: {data_count:,}")
                print()
                print("Computing...")

                # Komputasi mandiri pada node Worker dengan time.perf_counter()
                t0 = time.perf_counter()
                c2 = count_primes(start, end)
                elapsed = time.perf_counter() - t0

                print()
                print("RESULT")
                print(f"Prime count : {c2:,}")
                print(f"Compute time: {elapsed:.6f} s")
                print()
                print("Waiting for next command...")

                # Kirim RESULT ke Master
                result_payload = {
                    "type": "RESULT",
                    "task_id": task_id,
                    "status": "OK",
                    "node": "worker",
                    "start": start,
                    "end": end,
                    "range_size": data_count,
                    "prime_count": c2,
                    "compute_time_s": elapsed,
                }
                send_msg(client_sock, result_payload)

            # -------------------------------------------------------------
            # C. STOP / SHUTDOWN
            # -------------------------------------------------------------
            elif msg_type == "STOP":
                print("\nSTOP RECEIVED")
                print("Sending ACK...")
                ack_resp = {
                    "type": "ACK",
                    "action": "ACK",
                    "status": "OK",
                    "message": "worker shutdown",
                }
                send_msg(client_sock, ack_resp)
                print("Worker shutdown complete.")
                should_stop = True
                break

            # -------------------------------------------------------------
            # D. UNKNOWN COMMAND
            # -------------------------------------------------------------
            else:
                print(f"\n[WARN] Command tidak dikenal: '{msg_type}'")
                send_msg(
                    client_sock,
                    {
                        "type": "ERROR",
                        "status": "UNKNOWN_COMMAND",
                        "message": f"Command '{msg_type}' tidak dikenali.",
                    },
                )

    except ConnectionResetError:
        print(f"[WARN] Koneksi diputus tiba-tiba oleh Master ({client_addr[0]}).")
    except socket.timeout:
        print(f"[WARN] Socket timeout dari client ({client_addr[0]}).")
    except Exception as e:
        print(f"[ERROR] Terjadi kesalahan komunikasi: {e}")
    finally:
        try:
            client_sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        client_sock.close()
        print("[CLOSE] Connection closed.")

    return should_stop


def run_worker(bind_host: str, port: int) -> None:
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_sock.bind((bind_host, port))
    except OSError as e:
        print(f"[ERROR] Gagal melakukan bind ke {bind_host}:{port} -> {e}")
        print(f"[HINT] Pastikan port {port} belum digunakan proses lain.")
        sys.exit(1)

    server_sock.listen(1)

    print("============================================================")
    print("DISTRIBUTED PRIME RANGE COUNTER — WORKER")
    print("============================================================")
    print(f"Listening on {bind_host}:{port}")
    print("[WAIT] Menunggu koneksi Master...")

    try:
        while True:
            client_sock, client_addr = server_sock.accept()
            should_stop = handle_client(client_sock, client_addr)
            if should_stop:
                break
    except KeyboardInterrupt:
        print("\n[INFO] Server dihentikan manual oleh pengguna (Ctrl+C).")
    finally:
        server_sock.close()


def main() -> None:
    args = parse_arguments()
    run_worker(args.bind, args.port)


if __name__ == "__main__":
    main()
