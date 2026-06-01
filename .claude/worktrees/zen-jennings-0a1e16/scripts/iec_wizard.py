#!/usr/bin/env python3
"""IEC onboarding wizard — JSON-over-stdin/stdout protocol for web wizard."""
import asyncio
import json
import logging
import signal
import sys

# Suppress all library logging (would corrupt JSON protocol on stdout)
logging.disable(logging.CRITICAL)


def respond(data):
    sys.stdout.write(json.dumps(data, ensure_ascii=False) + '\n')
    sys.stdout.flush()


async def main():
    from iec_api.iec_client import IecClient

    loop = asyncio.get_event_loop()
    client = None
    bp_number = None
    id_number = None

    # Auto-kill after 12 minutes
    def timeout_handler(signum, frame):
        respond({"ok": False, "error": "session timeout"})
        sys.exit(1)
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(720)

    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:
            break
        line = line.strip()
        if not line:
            continue

        try:
            cmd = json.loads(line)
        except json.JSONDecodeError:
            respond({"ok": False, "error": "invalid JSON"})
            continue

        action = cmd.get("action")

        try:
            if action == "start":
                id_number = cmd.get("id_number", "")
                client = IecClient(id_number)
                await client.__aenter__()
                factor = await client.login_with_id()
                respond({"ok": True, "factor": str(factor or "SMS")})

            elif action == "verify":
                if not client:
                    respond({"ok": False, "error": "no active session"})
                    continue
                otp = cmd.get("otp", "")
                await client.verify_otp(otp)
                customer = await client.get_customer()
                bp_number = customer.bp_number
                raw_contracts = await client.get_contracts(bp_number)
                contracts_list = []
                for c in raw_contracts:
                    contracts_list.append({
                        "contract_id": str(c.contract_id),
                        "address": f"{c.address}, {c.city_name}" if c.city_name else str(c.address)
                    })
                respond({"ok": True, "contracts": contracts_list, "bp_number": str(bp_number)})

            elif action == "save":
                if not client:
                    respond({"ok": False, "error": "no active session"})
                    continue
                contract_id = cmd.get("contract_id", "")
                jwt_token = client.get_token()
                refresh_token = jwt_token.refresh_token if jwt_token else None
                if not refresh_token:
                    respond({"ok": False, "error": "no refresh token obtained"})
                    continue
                await client.__aexit__(None, None, None)
                client = None
                respond({
                    "ok": True,
                    "iec_user_id": id_number,
                    "iec_token": refresh_token,
                    "iec_bp_number": str(bp_number),
                    "iec_contract_id": contract_id
                })
                # Wipe PII from memory
                id_number = None
                break  # Done

            elif action == "ping":
                respond({"ok": True})

            else:
                respond({"ok": False, "error": f"unknown action: {action}"})

        except Exception as e:
            error_msg = str(e)
            # Sanitize — never expose ID number or OTP in errors
            if id_number and id_number in error_msg:
                error_msg = error_msg.replace(id_number, "***")
            respond({"ok": False, "error": error_msg})

    # Cleanup
    if client:
        try:
            await client.__aexit__(None, None, None)
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
