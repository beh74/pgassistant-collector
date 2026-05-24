import re

PASSWORD_PATTERNS = [
    re.compile(r"(postgres(?:ql)?://[^:\s]+:)([^@\s]+)(@)", re.IGNORECASE),
    re.compile(r'("db_password"\s*:\s*")([^"]+)(")', re.IGNORECASE),
]


def mask_secret(value: str | None) -> str | None:
    if value is None:
        return None
    masked = value
    for pattern in PASSWORD_PATTERNS:
        masked = pattern.sub(r"\1***\3", masked)
    return masked
