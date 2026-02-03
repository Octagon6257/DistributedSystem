import hmac
import hashlib
import json
from base64 import urlsafe_b64encode
from cryptography.fernet import Fernet
from typing import Dict, Any, Tuple
from config.LoggingConfig import get_logger

logger = get_logger("MessageSecurity")


class MessageSecurity:

    def __init__(self, secret_key: str):
        self.secret_key = secret_key.encode('utf-8')
        key_material = hashlib.sha256(self.secret_key).digest()
        fernet_key = urlsafe_b64encode(key_material)
        self.cipher = Fernet(fernet_key)

    def sign_message(self, message_dict: dict) -> str:
        msg_str = json.dumps(message_dict, sort_keys=True)
        signature = hmac.new(self.secret_key,msg_str.encode('utf-8'),hashlib.sha256).hexdigest()
        logger.debug(f"Signed Message : {signature[:16]}...")
        return signature

    def verify_signature(self, message_dict: dict, signature: str) -> bool:
        expected = self.sign_message(message_dict)
        is_valid = hmac.compare_digest(expected, signature)
        if not is_valid:
            logger.warning("Signature Not Valid!")
        return is_valid

    def encrypt_message(self, message_dict: dict) -> Tuple[bytes, str]:
        msg_json = json.dumps(message_dict, sort_keys=True)
        encrypted = self.cipher.encrypt(msg_json.encode('utf-8'))
        signature = self.sign_message(message_dict)
        logger.debug(f"Encrypted message : {len(encrypted)} bytes")
        return encrypted, signature

    def decrypt_message(self, encrypted_data: bytes, signature: str) -> Dict[str, Any]:
        try:
            decrypted = self.cipher.decrypt(encrypted_data)
            message_dict = json.loads(decrypted.decode('utf-8'))
            if not self.verify_signature(message_dict, signature):
                raise ValueError("Signature Not Valid!")
            logger.debug("Message correctly decrypted and verified")
            return message_dict
        except Exception as e:
            logger.error(f"Error during decryption: {e}")
            raise ValueError(f"Unable to decrypt message: {e}")