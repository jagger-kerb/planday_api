import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests


TOKEN_URL = "https://id.planday.com/connect/token"
PAYROLL_URL = "https://openapi.planday.com/payroll/v1.0/payroll"
DEPARTMENT_URL = "https://openapi.planday.com/hr/v1.0/departments"

def refresh_access_token(token_url: str, client_id: str, refresh_token: str) -> str:
    payload = {
        "client_id": client_id,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    resp = requests.post(token_url, headers=headers, data=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "access_token" not in data:
        raise RuntimeError(f"Token response missing access_token: {data}")
    return data["access_token"]


def fetch_payroll(client_id: str, access_token: str, start_date: str, end_date: str, departments_csv: str):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-ClientId": client_id,
    }
    params = {
        "departmentIds": departments_csv,
        "from": start_date,                # yyyy-mm-dd
        "to": end_date,                    # yyyy-mm-dd
    }

    # small pause to be polite to API gateways
    time.sleep(0.2)

    resp = requests.get(PAYROLL_URL, headers=headers, params=params, timeout=30)
    resp.raise_for_status()

    body = resp.json()
    # According to your example, payroll is in shiftsPayroll
    if "shiftsPayroll" not in body:
        raise RuntimeError(f"Unexpected response shape (missing shiftsPayroll). Keys: {list(body.keys())}")

    return body["shiftsPayroll"]


def write_to_csv(records, output_path: str):
    if not records:
        raise ValueError("No records found for the requested day")

    df = pd.DataFrame.from_records(records)
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Exported {len(df)} rows to {output_path}")

def get_departments(client_id: str, access_token: str):

    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-ClientId": client_id,
    }

    resp = requests.get(DEPARTMENT_URL, headers=headers)
    resp.raise_for_status()

    body = resp.json()

    return body["data"]


def main():

    client_id = os.environ["PLANDAY_CLIENT_ID"]
    refresh_token = os.environ["PLANDAY_REFRESH_TOKEN"]
    departments_csv = ",".join([str(d["id"]) for d in get_departments(client_id, refresh_token)])

    tz_name = os.environ.get("PLANDAY_TZ", "UTC")
    if tz_name == "UTC":
        tz = timezone.utc
        now = datetime.now(tz)
    else:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(tz_name)
        now = datetime.now(tz)

    yesterday = (now.date() - timedelta(days=1))
    start = yesterday.strftime("%Y-%m-%d")
    end = yesterday.strftime("%Y-%m-%d")

    out_dir = os.environ.get("OUTPUT", "exports")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"payroll_{start}.csv")

    access_token = refresh_access_token(TOKEN_URL, client_id, refresh_token)
    records = fetch_payroll(client_id, access_token, start, end, departments_csv)
    write_to_csv(records, out_path)



if __name__ == "__main__":
    main()
