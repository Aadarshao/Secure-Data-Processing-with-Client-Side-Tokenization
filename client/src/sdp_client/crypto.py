import base64
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def generate_crypto_key_b64() -> str:
    """
    Generate a fresh 32-byte AES-GCM key and return it as URL-safe base64.

    Use this to create SDP_CRYPTO_KEY values for local dev or initial setup.
    """
    key = os.urandom(32)  # 256-bit
    return base64.urlsafe_b64encode(key).decode("ascii")


def _get_aesgcm_key() -> bytes:
    """
    Returns a 32-byte (256-bit) key from SDP_CRYPTO_KEY env var.
    The env var must be URL-safe base64-encoded.
    """
    key_b64 = os.getenv("SDP_CRYPTO_KEY")
    if not key_b64:
        raise RuntimeError("SDP_CRYPTO_KEY is not set")

    try:
        key = base64.urlsafe_b64decode(key_b64.encode("ascii"))
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Failed to decode SDP_CRYPTO_KEY") from exc

    if len(key) != 32:
        raise RuntimeError("SDP_CRYPTO_KEY must decode to 32 bytes (256 bits)")

    return key


def encrypt_value(plaintext: str) -> str:
    """
    Encrypts plaintext using AES-256-GCM.
    Returns URL-safe base64-encoded (nonce + ciphertext + tag).
    """
    key = _get_aesgcm_key()
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)  # 96-bit nonce

    ciphertext = aesgcm.encrypt(nonce, plaintext.encode("utf-8"), None)
    data = nonce + ciphertext  # nonce | ct+tag
    return base64.urlsafe_b64encode(data).decode("ascii")


def decrypt_value(ciphertext_b64: str) -> str:
    """
    Decrypts a value produced by encrypt_value.
    """
    key = _get_aesgcm_key()
    aesgcm = AESGCM(key)
    data = base64.urlsafe_b64decode(ciphertext_b64.encode("ascii"))
    nonce, ciphertext = data[:12], data[12:]
    plaintext = aesgcm.decrypt(nonce, ciphertext, None)
    return plaintext.decode("utf-8")
