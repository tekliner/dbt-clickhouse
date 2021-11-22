import string
import secrets


def generate_session_id(length: int = 10) -> str:
    alphabet = string.ascii_letters + string.digits

    return ''.join(
        secrets.choice(alphabet) for i in range(length)
    )
