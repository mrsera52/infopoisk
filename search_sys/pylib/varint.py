from typing import List, Tuple


def pack(number: int) -> bytes:
    if number < 0:
        raise ValueError("Only non-negative integers supported")
    if number == 0:
        return b'\x00'
    parts = bytearray()
    while number >= 128:
        parts.append((number & 0x7F) | 0x80)
        number >>= 7
    parts.append(number)
    return bytes(parts)


def unpack(data: bytes, offset: int = 0) -> Tuple[int, int]:
    val = 0
    shift = 0
    while True:
        if offset >= len(data):
            raise IndexError("Unexpected end of stream")
        b = data[offset]
        offset += 1
        val |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return val, offset


def delta_encode(numbers: List[int]) -> List[int]:
    if not numbers:
        return []
    deltas = [numbers[0]]
    for i in range(1, len(numbers)):
        deltas.append(numbers[i] - numbers[i - 1])
    return deltas


def delta_decode(deltas: List[int]) -> List[int]:
    if not deltas:
        return []
    result = [deltas[0]]
    cur = deltas[0]
    for d in deltas[1:]:
        cur += d
        result.append(cur)
    return result
