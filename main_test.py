"""Demo-mode launcher for Binance spot + futures arbitrage testing.

References:
- Spot Demo API docs: https://developers.binance.com/docs/binance-spot-api-docs/demo-mode/general-info
- Futures API docs: https://developers.binance.com/docs/derivatives/
- Futures demo base URL: https://demo-fapi.binance.com
"""

import os
import sys

# Force Binance demo endpoints for safe testing.
os.environ["BINANCE_USE_TESTNET"] = "true"
os.environ["BINANCE_SPOT_BASE_URL"] = "https://demo-api.binance.com"
os.environ["BINANCE_FUTURES_BASE_URL"] = "https://demo-fapi.binance.com"

import uvicorn


if __name__ == "__main__":
    if "--check-credentials" in sys.argv:
        from main import run_credential_check

        run_credential_check()
    else:
        uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
