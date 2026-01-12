import os
import snowflake.connector

def get_conn():
    account_url = os.environ["SNOWFLAKE_ACCOUNT_URL"]
    # Derive "account" by stripping protocol/port
    account = account_url.replace("https://", "").split(":")[0]

    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=account,
        role=os.environ.get("SNOWFLAKE_ROLE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA"),
        authenticator="SNOWFLAKE_JWT",
        private_key=_load_private_key_der(),
    )

def _load_private_key_der() -> bytes:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend

    key_body = os.environ["SNOWFLAKE_PRIVATE_KEY"].strip().replace("\n", "")
    pem = "-----BEGIN PRIVATE KEY-----\n"
    for i in range(0, len(key_body), 64):
        pem += key_body[i:i+64] + "\n"
    pem += "-----END PRIVATE KEY-----\n"

    private_key = serialization.load_pem_private_key(
        pem.encode("utf-8"),
        password=None,
        backend=default_backend(),
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
