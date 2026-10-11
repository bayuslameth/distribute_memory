"""
common.py - Modul Komunikasi Bersama & Algoritma Prime untuk Distributed Prime Range Counter.

Mengimplementasikan:
1. Protokol Komunikasi Jaringan (Phase 1):
   - Length-Prefix Framing: Header 4-byte unsigned integer big-endian (!I)
   - Payload: JSON UTF-8
2. Algoritma Prime & Partisi (Phase 2 & Phase 3):
   - is_prime(n): Trial division dengan optimasi 6k ± 1 dan math.isqrt()
   - count_primes(start, end): Menghitung bilangan prima pada rentang inklusif [start, end]
   - split_range(N, mode): Membagi rentang [1, N] dengan strategi 'equal' atau 'weighted'
"""

import json
import math
import socket
import struct
from typing import Any, Optional

# Ukuran header: 4 byte untuk unsigned integer 32-bit (0 sampai ~4 GB)
HEADER_SIZE = 4
HEADER_FORMAT = "!I"  # '!' = network byte order (big-endian), 'I' = unsigned int 32-bit

# Known results untuk validasi kebenaran perhitungan (Ground Truth & PRD Reference)
KNOWN_PRIMES = {
    10: 4,
    100: 25,
    1000: 168,
    10000: 1229,
    100000: 9592,
    2000000: 148933,
    6000000: 412849,
    12000000: 788060,
}


# =====================================================================
# 1. NETWORKING FUNCTIONS (Phase 1)
# =====================================================================


def recv_exact(sock: socket.socket, num_bytes: int) -> Optional[bytes]:
    """
    Membaca tepat num_bytes dari socket TCP.

    TCP bersifat stream-oriented dan tidak menjamin satu panggilan recv()
    mengembalikan seluruh data yang dikirim oleh pengirim. Fungsi ini
    melakukan looping recv() sampai seluruh byte terkumpul.

    Args:
        sock: Socket TCP yang sedang aktif.
        num_bytes: Jumlah byte yang harus dibaca secara presisi.

    Returns:
        bytes: Buffer data tepat berukuran num_bytes.
        None: Jika socket ditutup oleh remote peer sebelum membaca byte pertama.

    Raises:
        ConnectionResetError: Jika koneksi terputus di tengah-tengah transmisi.
    """
    buffer = bytearray()
    while len(buffer) < num_bytes:
        chunk = sock.recv(num_bytes - len(buffer))
        if not chunk:
            if len(buffer) == 0:
                return None
            raise ConnectionResetError(
                f"Koneksi terputus saat membaca data. "
                f"Hanya menerima {len(buffer)} dari {num_bytes} bytes."
            )
        buffer.extend(chunk)
    return bytes(buffer)


def send_msg(sock: socket.socket, payload: dict[str, Any]) -> None:
    """
    Mengirim dictionary payload sebagai pesan terbingkai (length-prefixed).

    Langkah pengiriman:
    1. Konversi dictionary ke JSON string lalu encode ke bytes UTF-8.
    2. Hitung panjang bytes payload.
    3. Kemas panjang tersebut menjadi 4 byte header big-endian (struct.pack).
    4. Kirim header + payload secara utuh menggunakan sock.sendall().

    Args:
        sock: Socket TCP tujuan.
        payload: Dictionary data yang akan dikirim.
    """
    payload_bytes = json.dumps(payload).encode("utf-8")
    header = struct.pack(HEADER_FORMAT, len(payload_bytes))
    sock.sendall(header + payload_bytes)


def recv_msg(sock: socket.socket) -> Optional[dict[str, Any]]:
    """
    Menerima satu pesan utuh yang dibungkus dengan length-prefix framing.

    Langkah penerimaan:
    1. Baca tepat 4 byte pertama untuk mengambil panjang payload.
    2. Unpack header untuk mengetahui berapa byte payload yang harus dibaca.
    3. Baca tepat N byte payload sesuai nilai dari header.
    4. Decode bytes UTF-8 menjadi string dan parse kembali ke dictionary Python.

    Args:
        sock: Socket TCP sumber data.

    Returns:
        dict: Payload data yang diterima.
        None: Jika koneksi ditutup secara normal oleh remote peer.

    Raises:
        ConnectionResetError: Jika koneksi terputus sebelum seluruh payload diterima.
        json.JSONDecodeError: Jika isi payload tidak berformat JSON yang valid.
    """
    header = recv_exact(sock, HEADER_SIZE)
    if header is None:
        return None

    payload_length = struct.unpack(HEADER_FORMAT, header)[0]
    payload_bytes = recv_exact(sock, payload_length)
    if payload_bytes is None:
        raise ConnectionResetError("Koneksi terputus sebelum seluruh payload diterima.")

    return json.loads(payload_bytes.decode("utf-8"))


# =====================================================================
# 2. ALGORITMA PRIME & RANGE PARTITION (Phase 2 & Phase 3)
# =====================================================================


def is_prime(n: int) -> bool:
    """
    Memeriksa apakah n adalah bilangan prima menggunakan trial division
    dengan optimasi 6k ± 1 dan math.isqrt().

    Aturan algoritma:
    - n < 2 -> False
    - n < 4 -> True (2 dan 3 adalah prima)
    - n habis dibagi 2 atau 3 -> False
    - Loop mulai dari i = 5, cek i dan i + 2, lalu i bertambah 6
    - Berhenti ketika i > math.isqrt(n)
    """
    if n < 2:
        return False
    if n < 4:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False

    limit = math.isqrt(n)
    i = 5
    while i <= limit:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6

    return True


def count_primes(start: int, end: int) -> int:
    """
    Menghitung jumlah bilangan prima pada rentang inklusif [start, end].

    Contoh:
        count_primes(1, 10) -> 4 (prima: 2, 3, 5, 7)
    """
    if start > end:
        return 0

    count = 0
    for num in range(start, end + 1):
        if is_prime(num):
            count += 1
    return count


def split_range(n_total: int, mode: str = "weighted") -> tuple[int, int, int, int]:
    """
    Membagi rentang [1, n_total] menjadi dua partisi untuk Master dan Worker.

    Strategi:
    1. 'equal':
       s = n_total // 2
       Master: [1, s]
       Worker: [s + 1, n_total]

    2. 'weighted':
       Formula PRD: s = round(n_total / 2^(2/3))
       Master: [1, s]      (~63% data, angka kecil lebih ringan per bilangan)
       Worker: [s + 1, n_total] (~37% data, angka besar lebih berat per bilangan)

    Memastikan:
    - Kedua range tidak overlap
    - Seluruh rentang [1, n_total] tercakup tanpa angka yang hilang
    - master_size + worker_size == n_total
    - 1 <= s < n_total (untuk n_total >= 2)

    Returns:
        tuple[int, int, int, int]: (master_start, master_end, worker_start, worker_end)

    Raises:
        ValueError: Jika n_total < 2 atau mode tidak dikenal
    """
    if n_total < 2:
        raise ValueError(f"Nilai N harus >= 2 untuk dapat dibagi ke 2 PC (diterima: {n_total})")

    mode_clean = mode.lower().strip()

    if mode_clean == "equal":
        s = n_total // 2
    elif mode_clean == "weighted":
        # s = round(N / 2^(2/3)) -> pembagian beban komputasi seimbang
        s = round(n_total / (2 ** (2 / 3)))
        # Pastikan batas valid
        if s < 1:
            s = 1
        elif s >= n_total:
            s = n_total - 1
    else:
        raise ValueError(
            f"Strategi partisi '{mode}' tidak dikenal. Pilih 'weighted' atau 'equal'."
        )

    master_start = 1
    master_end = s
    worker_start = s + 1
    worker_end = n_total

    # Validasi integritas partisi (mencegah gap, overlap, missing number, duplicated number)
    if not (master_start == 1 and master_end + 1 == worker_start and worker_end == n_total):
        raise ValueError(
            f"Partisi cacat: [{master_start}..{master_end}] dan [{worker_start}..{worker_end}] tidak menutup [1..{n_total}] secara sempurna."
        )
    if (master_end - master_start + 1) + (worker_end - worker_start + 1) != n_total:
        raise ValueError(
            f"Akumulasi ukuran partisi tidak sama dengan N: {(master_end - master_start + 1) + (worker_end - worker_start + 1)} != {n_total}"
        )

    return master_start, master_end, worker_start, worker_end
