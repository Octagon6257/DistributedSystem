import sys

sys.path.insert(0, "..")
from security.Encryption import MessageSecurity
from config.Settings import SecuritySettings


def run_encryption_test():
    print("=" * 60)
    print("ENCRYPTION TEST")
    print("=" * 60 + "\n")
    print("Signature Test (HMAC-SHA256)...")
    security = MessageSecurity(SecuritySettings.SECRET_KEY)
    message = {"type": "TEST", "payload": {"data": "hello"}}
    signature = security.sign_message(message)
    print(f"   Message: {message}")
    print(f"   Signature: {signature[:32]}...")
    is_valid = security.verify_signature(message, signature)
    assert is_valid, "Signature should be valid"
    print("   Valid Signature")
    modified_message = {"type": "TEST", "payload": {"data": "modified"}}
    is_valid = security.verify_signature(modified_message, signature)
    assert not is_valid, "Signature should be invalid"
    print("   Message tampering detected")
    print("\n   Test Fernet cipher...")
    encrypted, sig = security.encrypt_message(message)
    print(f"   Original message: {message}")
    print(f"   Encrypted ({len(encrypted)} bytes): {encrypted[:40].hex()}...")
    print(f"   Signature: {sig[:32]}...")
    decrypted = security.decrypt_message(encrypted, sig)
    assert decrypted == message, "Decrypted message does not match"
    print(f"Decrypted: {decrypted}")
    print("Encryption/decryption is functional")


if __name__ == "__main__":
    run_encryption_test()