import os
import sys

os.environ["BINANCE_USE_TESTNET"] = "false"

import uvicorn


if __name__ == "__main__":
    if "--check-credentials" in sys.argv:
        from main import run_credential_check

        run_credential_check()
    else:
        uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
