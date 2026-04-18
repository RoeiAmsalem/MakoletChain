#!/usr/bin/env python3
"""
IEC onboarding script — run once per branch to link IEC account.

Usage:
    python3 scripts/iec_onboard.py --branch-id 126

Flow:
    1. Prompt for ID number (תעודת זהות)
    2. Send OTP via IEC → SMS to phone on file
    3. Prompt for OTP code
    4. Fetch customer + contracts
    5. If multiple contracts, prompt which one
    6. Save to branches table
"""

import argparse
import asyncio
import json
import os
import sqlite3
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from iec_api.iec_client import IecClient

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'db', 'makolet_chain.db')


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def verify_branch_exists(branch_id: int):
    conn = get_db()
    row = conn.execute('SELECT id, name FROM branches WHERE id = ?', (branch_id,)).fetchone()
    conn.close()
    if not row:
        print(f"❌ Branch {branch_id} not found in database.")
        sys.exit(1)
    return dict(row)


async def onboard(branch_id: int):
    branch = verify_branch_exists(branch_id)
    print(f"Onboarding IEC for branch {branch_id} ({branch['name']})\n")

    # Step 1: Get ID number
    id_number = input("Enter the ID number (תעודת זהות) on the IEC account: ").strip()
    if not id_number:
        print("❌ ID number is required.")
        return

    # Step 2: Login — sends OTP
    print(f"\nSending OTP to the phone registered with IEC for ID {id_number}...")
    async with IecClient(id_number) as client:
        try:
            factor_type = await client.login_with_id()
            print(f"OTP sent via {factor_type or 'SMS'}.")
        except Exception as e:
            print(f"❌ Failed to initiate IEC login: {e}")
            return

        # Step 3: Get OTP code
        otp_code = input("\nEnter the OTP code received: ").strip()
        if not otp_code:
            print("❌ OTP code is required.")
            return

        try:
            await client.verify_otp(otp_code)
            print("✅ OTP verified successfully.\n")
        except Exception as e:
            print(f"❌ OTP verification failed: {e}")
            return

        # Step 4: Fetch customer + contracts
        try:
            customer = await client.get_customer()
            if not customer:
                print("❌ Could not fetch customer data from IEC.")
                return
            bp_number = customer.bp_number
            print(f"Customer: {customer.first_name} {customer.last_name} (BP: {bp_number})")
        except Exception as e:
            print(f"❌ Failed to fetch customer: {e}")
            return

        try:
            contracts = await client.get_contracts(bp_number)
            if not contracts:
                print("❌ No contracts found for this customer.")
                return
        except Exception as e:
            print(f"❌ Failed to fetch contracts: {e}")
            return

        # Step 5: Select contract
        if len(contracts) == 1:
            selected = contracts[0]
            print(f"Contract: {selected.contract_id} — {selected.address} ({selected.city_name})")
        else:
            print(f"\n{len(contracts)} contracts found:")
            for i, c in enumerate(contracts, 1):
                debt_str = f"₪{c.total_debt:.0f}" if c.total_debt else "₪0"
                print(f"  {i}. {c.contract_id} — {c.address}, {c.city_name} (debt: {debt_str})")
            while True:
                choice = input(f"\nWhich contract is for branch {branch_id}? Enter number (1-{len(contracts)}): ").strip()
                try:
                    idx = int(choice) - 1
                    if 0 <= idx < len(contracts):
                        selected = contracts[idx]
                        break
                except ValueError:
                    pass
                print("Invalid choice, try again.")

        # Step 6: Get the refresh token from the JWT
        jwt_token = client.get_token()
        refresh_token = jwt_token.refresh_token

        if not refresh_token:
            print("❌ No refresh token obtained — IEC login may not support long-lived tokens.")
            return

        # Save to DB
        try:
            conn = get_db()
            conn.execute('''
                UPDATE branches SET
                    iec_user_id = ?,
                    iec_token = ?,
                    iec_bp_number = ?,
                    iec_contract_id = ?,
                    iec_last_sync_at = NULL
                WHERE id = ?
            ''', (id_number, refresh_token, bp_number, selected.contract_id, branch_id))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"❌ Failed to save to database: {e}")
            return

        print(f"\n✅ Branch {branch_id} onboarded.")
        print(f"   Contract: {selected.contract_id}")
        print(f"   BP Number: {bp_number}")
        print(f"   Address: {selected.address}, {selected.city_name}")
        print(f"\nRun `bash scripts/run-agent.sh iec` to do first sync.")


def main():
    parser = argparse.ArgumentParser(description="Onboard a branch with IEC (Israel Electric Company)")
    parser.add_argument('--branch-id', type=int, required=True, help='Branch ID to onboard')
    args = parser.parse_args()
    asyncio.run(onboard(args.branch_id))


if __name__ == '__main__':
    main()
