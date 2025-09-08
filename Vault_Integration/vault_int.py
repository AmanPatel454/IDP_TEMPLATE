#!/usr/bin/env python3
"""
Production script to fetch a secret from Vault via CommonUtils.
Uses configuration file + Vault authentication.
Intended for Harness pipeline execution.
"""

import sys
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Add src to path so we can import our utils
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from utils.CommonUtils import (
    get_config,
    client_auth,
    read_secret,
)


def main():
    """
    Load config, authenticate to Vault, and fetch a secret.
    Only prints the secret value (to be captured in Harness).
    """
    try:
        # Load configuration
        config = get_config()
        environment = config.get("environment", os.getenv("ENVIRONMENT", "dev"))
        secret_path = config.get("vault_secret_path", os.getenv("VAULT_SECRET_PATH", "/1020_edb-genai/idmc"))

        logging.info(f"Fetching secret for env={environment}, path={secret_path}")

        # Authenticate to Vault
        client = client_auth()
        if not client:
            logging.error("Vault authentication failed")
            sys.exit(1)

        # Fetch secret
        secret_data = read_secret(secret_path, environment)
        if not secret_data:
            logging.error("No secret data returned from Vault")
            sys.exit(1)

        # Choose which key to return (e.g., password or token)
        secret_value = secret_data.get("password") or str(secret_data)

        # 🚨 Do NOT log secret_value here for security
        print(secret_value)  # Harness captures this

        return secret_value

    except Exception as e:
        logging.error(f"Error fetching secret: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
