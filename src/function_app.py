import azure.functions as func
import logging
import json
import os
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import traceback
import math
import re
from datetime import datetime, timezone, timedelta
from azure.storage.filedatalake import DataLakeServiceClient
from azure.data.tables import TableClient, UpdateMode

app = func.FunctionApp()

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
CAPACITIES = {
    "PJM": 170000, "MISO": 130000, "ERCOT": 90000, "SPP": 58000,
    "CAISO": 56000, "NYISO": 32000, "ISONE": 30000,
    "SOCO": 48000, "TVA": 36000, "FPL": 33000, "DUK": 22000,
    "CPLE": 15000, "DEF": 11500, "BPAT": 12000, "PACE": 9200,
    "AZPS": 9500, "NEVP": 8400, "SRP": 8800, "PACW": 4400,
    "PNM": 2400, "SCEG": 6000, "SC": 6000
}

# ---------------------------------------------------------
# DATA INTELLIGENCE HELPERS
# ---------------------------------------------------------
def extract_val(obj):
    if isinstance(obj, (int, float)): return float(obj)
    if isinstance(obj, str):
        try: return float(obj.replace(',', ''))
        except: return None
    if isinstance(obj, list):
        if len(obj) > 0: return extract_val(obj[-1])
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            val = extract_val(v)
            if val is not None: return val
    return None

def parse_source_time(val, region="UTC"):
    if not val: return None
    try:
        ts = pd.to_datetime(val)
        if ts.tzinfo is not None:
            return ts.tz_convert(timezone.utc)
        tz_map = {
            "PJM": "US/Eastern", "NYISO": "US/Eastern", "ISONE": "US/Eastern",
            "MISO": "US/Eastern", "ERCOT": "US/Central", "SPP": "US/Central",
            "CAISO": "US/Pacific"
        }
        target_tz = tz_map.get(region, "UTC")
        return ts.tz_localize(target_tz).tz_convert(timezone.utc)
    except:
        return None

def calculate_status(load_mw, region):
    cap = CAPACITIES.get(region, 50000)
    util = (load_mw / cap) * 100
    status = "CRITICAL" if util > 85 else "ELEVATED" if util > 75 else "NORMAL"
    return util, status

def is_data_stale(timestamp_str, region):
    TIER_2_REGIONS = [
        "TVA", "SOCO", "DUK", "CPLE", "DEF", "FPL", "BPAT",
        "SRP", "AZPS", "PNM", "NEVP", "PACE", "PACW", "SCEG", "SC"
    ]
    limit_minutes = 720 if region in TIER_2_REGIONS else 20
    try:
        data_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        age = datetime.now(timezone.utc) - data_time
        return age > timedelta(minutes=limit_minutes), age.total_seconds() / 60
    except:
        return True, 9999

def save_to_lake(data_payload, folder_structure, file_name):
    try:
        conn_str = os.environ["AzureWebJobsStorage"]
        service_client = DataLakeServiceClient.from_connection_string(conn_str)
        fs_client = service_client.get_file_system_client("processed-silver")
        if not fs_client.exists(): fs_client.create_file_system()
        dir_client = fs_client.get_directory_client(folder_structure)
        if not dir_client.exists(): dir_client.create_directory()
        file_client = dir_client.get_file_client(file_name)
        file_client.upload_data(json.dumps(data_payload), overwrite=True)
    except Exception as e:
        logging.error(f"Lake Write Error ({data_payload.get('region')}): {str(e)}")

def save_to_table(data_payload):
    try:
        conn_str = os.environ["AzureWebJobsStorage"]
        latest_client = TableClient.from_connection_string(conn_str, table_name="GridStatusLatest")
        history_client = TableClient.from_connection_string(conn_str, table_name="GridStatusHistory")

        latest_entity = {
            "PartitionKey": "US_GRID",
            "RowKey": data_payload["region"],
            "Timestamp": datetime.now(timezone.utc),
            "LoadMW": data_payload["load_mw"],
            "Utilization": data_payload["utilization"],
            "Status": data_payload["status"],
            "Trend": data_payload.get("trend", "STABLE"),
            "LastUpdated": data_payload["timestamp"],
            "LMP": data_payload.get("lmp"),
            "DataLagMinutes": data_payload.get("lag_min", 0),
            "DataAgeSeconds": data_payload.get("lag_sec", 0)
        }
        latest_client.upsert_entity(mode=UpdateMode.REPLACE, entity=latest_entity)

        history_entity = {
            "PartitionKey": data_payload["region"],
            "RowKey": data_payload["timestamp"],
            "LoadMW": data_payload["load_mw"],
            "LMP": data_payload.get("lmp")
        }
        history_client.create_entity(entity=history_entity)
    except Exception as e:
        pass

# ---------------------------------------------------------
# TIER 1: ISOs (Real Time) - Production v2.4 (Patched)
# ---------------------------------------------------------
@app.schedule(schedule="0 */5 * * * *", arg_name="tier1Timer", run_on_startup=True, use_monitor=False)
def GridScraper_Tier1(tier1Timer: func.TimerRequest) -> None:
    logging.info("Starting Tier 1 (ISO) Scraper - Production Mode...")

    # 1. DEFINE HEADERS
    UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"

    regions = ["ERCOT", "MISO", "PJM", "NYISO", "ISONE", "SPP"]
    pjm_key = os.environ.get("PJM_API_KEY", "").strip()

    for region_code in regions:
        try:
            # --- INITIALIZATION (SCOPE SAFETY) ---
            load_mw = 0.0
            price_usd = 0.0

            # 'ts' is the execution time (fallback)
            ts = datetime.now(timezone.utc)

            # 'source_ts' is the data time. Initialize as None.
            # If a region doesn't find a time, default to 'ts' later.
            source_ts = None

            diagnostic_log = ""

            # =========================================================
            # 1. FETCH LOAD
            # =========================================================
            if region_code == "MISO":
                try:
                    url_snap = f"https://public-api.misoenergy.org/api/Snapshot?_={int(datetime.now().timestamp())}"
                    r_snap = requests.get(url_snap, headers={"User-Agent": UA}, timeout=10)
                    if r_snap.status_code == 200:
                        snap_list = r_snap.json()
                        if isinstance(snap_list, list):
                            load_item = next((x for x in snap_list if "Current Demand" in x.get("t", "")), None)
                            if load_item:
                                val = extract_val(load_item.get("v"))
                                if val and val > 20000:
                                    load_mw = val
                                    raw_ts = load_item.get("d")
                                    if raw_ts:
                                        try:
                                            clean_ts = raw_ts.replace(" EST", "").replace(" EDT", "").strip()
                                            dt_obj = datetime.strptime(clean_ts, "%m/%d/%Y %I:%M:%S %p")
                                            tz_est = timezone(timedelta(hours=-5))
                                            source_ts = dt_obj.replace(tzinfo=tz_est).astimezone(timezone.utc)
                                        except: diagnostic_log += f"[MISO_Time_Fail] "
                except Exception: diagnostic_log += f"[MISO_Snap_Err] "

            elif region_code == "ERCOT":
                try:
                    h_ercot_load = {
                        "User-Agent": UA, "Accept": "application/json",
                        "Referer": "https://www.ercot.com/gridmktinfo/dashboards/supplyanddemand"
                    }
                    url = "https://www.ercot.com/api/1/services/read/dashboards/supply-demand.json"
                    r = requests.get(url, headers=h_ercot_load, timeout=10)
                    if r.status_code == 200:
                        data = r.json()
                        target_list = data.get("Data") or data.get("data")
                        if target_list and isinstance(target_list, list):
                            valid_items = []
                            now_utc = datetime.now(timezone.utc)
                            for item in target_list:
                                try:
                                    raw_ts = item.get("timestamp")
                                    if raw_ts:
                                        ts_obj = pd.to_datetime(raw_ts).tz_convert(timezone.utc)
                                        if ts_obj <= now_utc + timedelta(minutes=5):
                                            valid_items.append((ts_obj, item))
                                except: pass
                            if valid_items:
                                valid_items.sort(key=lambda x: x[0], reverse=True)
                                best_ts, best_item = valid_items[0]
                                val = extract_val(best_item.get("demand") or best_item.get("value"))
                                if val and val > 1000:
                                    load_mw = val
                                    source_ts = best_ts
                except Exception: pass

            elif region_code == "PJM":
                try:
                    if not pjm_key: diagnostic_log += "[PJM_NO_KEY] "
                    else:
                        h_pjm = { "User-Agent": UA, "Ocp-Apim-Subscription-Key": pjm_key }
                        url_l = "https://api.pjm.com/api/v1/inst_load"
                        r_l = requests.get(url_l, headers=h_pjm, timeout=10)
                        if r_l.status_code == 200:
                            items = r_l.json().get('items', [])
                            if items:
                                best = max(items, key=lambda x: float(x.get('instantaneous_load', 0) or x.get('total_load', 0)))
                                load_mw = float(best.get('instantaneous_load', 0) or best.get('total_load', 0))
                                raw_ts = best.get('datetime_beginning_ept')
                                if raw_ts: source_ts = parse_source_time(raw_ts, "PJM")
                except: diagnostic_log += "[PJM_L_Ex] "

            # --- NYISO PATCH (PART 1: LOAD) ---
            elif region_code == "NYISO":
                try:
                    et_now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-5)))
                    date_str = et_now.strftime("%Y%m%d")
                    url = f"http://mis.nyiso.com/public/csv/pal/{date_str}pal.csv"
                    df = pd.read_csv(url)
                    if not df.empty and 'Load' in df.columns:
                        latest_time = pd.to_datetime(df['Time Stamp']).max()
                        source_ts = parse_source_time(latest_time, "NYISO") # Baseline Time

                        df_latest = df[pd.to_datetime(df['Time Stamp']) == latest_time]
                        if not df_latest.empty:
                            exclude = ['NYCA', 'N.Y.C.A.', 'TOTAL']
                            # Try to get NYCA specific, else sum zones
                            if 'Name' in df_latest.columns:
                                df_nyca = df_latest[df_latest['Name'].isin(['N.Y.C.A.', 'NYCA'])]
                                if not df_nyca.empty: load_mw = float(df_nyca.iloc[0]['Load'])
                                else: load_mw = float(df_latest[~df_latest['Name'].isin(exclude)]['Load'].sum())
                            elif 'Zone Name' in df_latest.columns:
                                df_nyca = df_latest[df_latest['Zone Name'].isin(['N.Y.C.A.', 'NYCA'])]
                                if not df_nyca.empty: load_mw = float(df_nyca.iloc[0]['Load'])
                                else: load_mw = float(df_latest[~df_latest['Zone Name'].isin(exclude)]['Load'].sum())
                except: diagnostic_log += "[NY_L_Ex] "

            elif region_code == "ISONE":
                try:
                    s = requests.Session()
                    s.headers.update({"User-Agent": UA, "Accept": "application/json"})
                    s.get("https://www.iso-ne.com/isoexpress/web/reports/load-and-demand/-/tree/dmnd-five-minute-sys", timeout=10)
                    target_tz = timezone(timedelta(hours=-5))
                    iso_now = datetime.now(timezone.utc).astimezone(target_tz)
                    date_str = iso_now.strftime("%Y%m%d")
                    csv_url = f"https://www.iso-ne.com/transform/csv/fiveminutesystemload?start={date_str}&end={date_str}"
                    r_l = s.get(csv_url, timeout=15)
                    if r_l.status_code == 200:
                        lines = r_l.text.splitlines()
                        for line in reversed(lines):
                            clean = line.replace('"', '').strip()
                            if clean.startswith('D,'):
                                parts = [p.strip() for p in clean.split(',')]
                                if len(parts) >= 3:
                                    try:
                                        val = float(parts[2])
                                        if val > 5000:
                                            load_mw = val
                                            source_ts = parse_source_time(parts[1], "ISONE")
                                            break
                                    except: pass
                except: diagnostic_log += "[ISONE_L_Ex] "

            elif region_code == "SPP":
                try:
                    h_spp = { "User-Agent": UA, "Referer": "https://portal.spp.org/", "Accept": "application/json" }
                    r_l = requests.get("https://marketplace.spp.org/chart-api/load-forecast/asChart", headers=h_spp, timeout=15)
                    if r_l.status_code == 200:
                        data = r_l.json()
                        resp_content = data.get("response", {})
                        datasets = resp_content.get("datasets", [])
                        labels = resp_content.get("labels", [])
                        for ds in datasets:
                            if ds.get("label") == "Actual Load":
                                values = ds.get("data", [])
                                valid_vals = [(i, v) for i, v in enumerate(values) if v is not None and v > 0]
                                if valid_vals:
                                    last_idx, last_val = valid_vals[-1]
                                    load_mw = float(last_val)
                                    if labels and last_idx < len(labels):
                                        source_ts = parse_source_time(labels[last_idx], "SPP")
                                break
                except: diagnostic_log += "[SPP_L_Ex] "


            # =========================================================
            # 2. FETCH PRICE
            # =========================================================
            if region_code == "MISO":
                try:
                    h_miso = {"User-Agent": UA, "Accept": "application/json", "Referer": "https://www.misoenergy.org/"}
                    r = requests.get("https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable", headers=h_miso, timeout=10)
                    if r.status_code == 200:
                        nodes = r.json().get("LMPData", {}).get("FiveMinLMP", {}).get("PricingNode", [])
                        for node in nodes:
                            if "MINN.HUB" in node.get("name", ""):
                                price_usd = float(node.get("LMP", 0))
                                break
                except: pass

            elif region_code == "ERCOT":
                try:
                    h_ercot_price = {"User-Agent": UA, "Accept": "application/json", "Referer": "https://www.ercot.com/gridmktinfo/dashboards/systemwideprices"}
                    r = requests.get("https://www.ercot.com/api/1/services/read/dashboards/system-wide-prices", headers=h_ercot_price, timeout=10)
                    if r.status_code == 200:
                        data = r.json()
                        target_list = data.get("rtSppData") or data.get("rtsppdata")
                        if target_list and isinstance(target_list, list):
                            for item in reversed(target_list):
                                raw_val = item.get("hbNorth") or item.get("hbnorth")
                                if raw_val is not None:
                                    price_usd = float(raw_val)
                                    break
                except: pass

            elif region_code == "PJM":
                try:
                    if pjm_key:
                        h_pjm = { "User-Agent": UA, "Ocp-Apim-Subscription-Key": pjm_key }
                        params = { "startRow": 1, "rowCount": 1, "name": "WESTERN HUB", "sort": "datetime_beginning_utc", "order": "Desc" }
                        r = requests.get("https://api.pjm.com/api/v1/unverified_five_min_lmps", headers=h_pjm, params=params, timeout=15)
                        if r.status_code == 200:
                            items = r.json().get('items', [])
                            if items:
                                best = items[0]
                                p = best.get('five_min_rtlmp') or best.get('totallmprt')
                                if p: price_usd = float(p)
                except: pass

            # --- NYISO PATCH v2.9 (Strict Time Slicing) ---
            elif region_code == "NYISO":
                try:
                    et_now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-5)))
                    date_str = et_now.strftime("%Y%m%d")
                    url = f"http://mis.nyiso.com/public/csv/realtime/{date_str}realtime_zone.csv"
                    df = pd.read_csv(url)

                    if not df.empty and 'Time Stamp' in df.columns:
                        df['dt'] = pd.to_datetime(df['Time Stamp'])

                        # STRICT CLAMP: Exactly Now. No future buffer allowed.
                        et_naive_now = et_now.replace(tzinfo=None)

                        if 'LBMP ($/MWHr)' in df.columns:
                            df = df.dropna(subset=['LBMP ($/MWHr)'])

                        # Filter out NYISO's future projections
                        df_valid = df[df['dt'] <= et_naive_now]

                        if not df_valid.empty:
                            latest_ts = df_valid['dt'].max()
                            price_time = parse_source_time(latest_ts, "NYISO")

                            # Arbitration
                            if source_ts is None:
                                source_ts = price_time
                            elif price_time and price_time > source_ts:
                                source_ts = price_time
                                diagnostic_log += "[TS_Arbitrated] "

                            df_latest = df_valid[df_valid['dt'] == latest_ts]
                            if not df_latest.empty:
                                if 'Name' in df_latest.columns:
                                    df_cen = df_latest[df_latest['Name'].astype(str).str.contains('CENTRL')]
                                    if not df_cen.empty: price_usd = float(df_cen.iloc[0]['LBMP ($/MWHr)'])
                                    else: price_usd = float(df_latest['LBMP ($/MWHr)'].mean())
                                else: price_usd = float(df_latest['LBMP ($/MWHr)'].mean())
                        else:
                            diagnostic_log += "[NY_No_Past_Data] "
                except Exception as e: diagnostic_log += f"[NY_P_Err] "


            # --- ISONE PATCH v2.11 (Session Fix & Parser) ---
            elif region_code == "ISONE":
                try:
                    import time
                    ts_buster = int(time.time())

                    # CRITICAL: MUST use a Session and hit the base page first
                    # to get the cookie, otherwise ISO-NE blocks the CSV download with a 403.
                    s = requests.Session()
                    s.headers.update({"User-Agent": UA, "Accept": "application/json"})
                    s.get("https://www.iso-ne.com/isoexpress/web/reports/pricing/-/tree/zone-info", timeout=10)

                    # Try Prelim first, fallback to Final if empty/0.0
                    for feed_type in ['prelim', 'final']:
                        url = f"https://www.iso-ne.com/transform/csv/fiveminlmp/current?type={feed_type}&_={ts_buster}"
                        r_p = s.get(url, timeout=15) # Use Session 's'

                        if r_p.status_code == 200:
                            lines = r_p.text.splitlines()
                            for line in lines:
                                # Clean line of quotes FIRST to ensure matching works regardless of formatting
                                clean_line = line.replace('"', '').strip()

                                # Target Hub explicitly by ISO-NE Location ID 4000
                                if clean_line.startswith('D,4000,'):
                                    parts = [p.strip() for p in clean_line.split(',')]
                                    if len(parts) >= 7:
                                        # Index 6 is LMP, Index 3 is Energy Component
                                        try:
                                            price_usd = float(parts[6])
                                        except ValueError: pass

                                        if price_usd == 0.0:
                                            try:
                                                price_usd = float(parts[3])
                                            except ValueError: pass
                                    break # Break inner loop once Hub is found
                        else:
                            diagnostic_log += f"[ISO_P_HTTP_{r_p.status_code}] "

                        # If successfully get a price, stop checking feeds
                        if price_usd > 0:
                            if feed_type == 'final':
                                diagnostic_log += "[ISO_Final_Fallback] "
                            break
                except Exception as e: diagnostic_log += f"[ISO_P_Err_{str(e)[:10]}] "

            elif region_code == "SPP":
                try:
                    h_spp = { "User-Agent": UA, "Referer": "https://portal.spp.org/", "Accept": "application/json" }
                    base_url = "https://pricecontourmap.spp.org/arcgis/rest/services/MarketMaps/RTBM_FeatureData/MapServer/0/query"
                    params = { "where": "1=1", "outFields": "*", "f": "json" }
                    r = requests.get(base_url, headers=h_spp, params=params, timeout=15)

                    if r.status_code == 200:
                        features = r.json().get("features", [])
                        if not features:
                            diagnostic_log += "[SPP_P_NoFeat] "

                        lmps = []
                        for f in features:
                            val = f.get("attributes", {}).get("LMP")
                            if val is not None:
                                lmps.append(float(val))

                        if lmps:
                            price_usd = sum(lmps) / len(lmps)
                        else:
                            diagnostic_log += "[SPP_P_NoLMP] "
                    else:
                        diagnostic_log += f"[SPP_P_HTTP_{r.status_code}] "
                except Exception as e:
                    diagnostic_log += f"[SPP_P_Ex_{str(e)[:10]}] "

            # =========================================================
            # 3. SAVE / AUDIT
            # =========================================================
            if load_mw <= 100:
                if diagnostic_log:
                    logging.warning(f"Discarding Zero/Low Load for {region_code}: {load_mw} MW. Diag: {diagnostic_log}")
                continue

            util, status = calculate_status(load_mw, region_code)

            # FINAL TIME CALCULATION (SCOPE SAFE)
            # Use 'source_ts' if found (from Load OR Price arbitration).
            # Fallback to execution time 'ts' if data was missing/unparsable.
            final_ts = source_ts if source_ts else ts

            # Lag Calculation
            lag_min = 0
            lag_sec = 0
            if source_ts:
                delta = datetime.now(timezone.utc) - source_ts
                lag_sec = int(delta.total_seconds())
                lag_min = int(lag_sec / 60)

                if abs(lag_sec) < 300: status = f"OK ({lag_sec}s)"
                elif abs(lag_sec) < 1200: status = f"OK ({lag_min}m)"
                else: status = f"LAG {lag_min}m"
            else:
                status = "NO_DATA"

            if diagnostic_log: status += f" {diagnostic_log}"

            payload = {
                "type": "grid",
                "region": region_code,
                # Ensure final_ts is defined.
                "timestamp": final_ts.isoformat(),
                "load_mw": int(load_mw),
                "utilization": int(util),
                "status": status,
                "trend": "STABLE",
                "lmp": round(price_usd, 2) if price_usd is not None else 0.0,
                "lag_min": lag_min,
                "lag_sec": lag_sec
            }

            save_to_table(payload)
            date_path = f"{region_code}/{final_ts.year}/{final_ts.month:02d}/{final_ts.day:02d}"
            file_name = f"{final_ts.strftime('%H-%M-%S')}.json"
            save_to_lake(payload, date_path, file_name)

        except Exception as e:
            logging.error(f"Tier 1 Global Fail {region_code}: {e}")

# ---------------------------------------------------------
# TIER 2: Regulated Markets (EIA API)
# ---------------------------------------------------------
@app.schedule(schedule="0 */15 * * * *", arg_name="tier2Timer", run_on_startup=True, use_monitor=False)
def GridScraper_Tier2(tier2Timer: func.TimerRequest) -> None:
    api_key = os.environ.get("EIA_API_KEY", "").strip()
    if not api_key: return

    eia_map = {
        "TVA": "TVA", "SOCO": "SOCO", "DUK": "DUK", "CPLE": "CPLE", "DEF": "FPC",
        "FPL": "FPL", "BPAT": "BPAT", "SRP": "SRP", "AZPS": "AZPS", "PNM": "PNM",
        "NEVP": "NEVP", "PACE": "PACE", "PACW": "PACW", "SCEG": "SCEG", "SC": "SC"
    }
    target_respondents = list(eia_map.values())

    try:
        url = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
        params = {
            "api_key": api_key, "frequency": "local-hourly", "data[0]": "value",
            "facets[respondent][]": target_respondents, "sort[0][column]": "period",
            "sort[0][direction]": "desc", "length": 2500, "offset": 0
        }
        resp = requests.get(url, params=params, timeout=20)
        if resp.status_code != 200: return

        data = resp.json().get("response", {}).get("data", [])
        df = pd.DataFrame(data)
        if df.empty: return

        df.columns = [c.lower() for c in df.columns]
        if 'type' in df.columns: df = df[df['type'] == 'D']

        for db_label, eia_code in eia_map.items():
            try:
                mask = df['respondent'] == eia_code
                df_ba = df[mask]
                if df_ba.empty: continue

                latest = df_ba.sort_values(by="period").iloc[-1]
                try: val = float(latest['value'])
                except: continue

                if val <= 10: continue

                ts = pd.to_datetime(latest['period']).replace(tzinfo=timezone.utc)
                util, status = calculate_status(val, db_label)

                payload = {
                    "type": "grid", "region": db_label, "timestamp": ts.isoformat(),
                    "load_mw": val, "utilization": round(util, 1), "status": status,
                    "lmp": None
                }
                save_to_table(payload)
                date_path = f"{db_label}/{ts.year}/{ts.month:02d}/{ts.day:02d}"
                file_name = f"{ts.strftime('%H-%M-%S')}.json"
                save_to_lake(payload, date_path, file_name)
            except: pass
    except: pass

# ---------------------------------------------------------
# HEARTBEAT (KEEP WARM) - Added v17.8
# ---------------------------------------------------------
@app.schedule(schedule="0 */9 * * * *", arg_name="heartbeatTimer", run_on_startup=True, use_monitor=False)
def KeepWarm(heartbeatTimer: func.TimerRequest) -> None:
    logging.info("Heartbeat: Keeping Function Host Warm.")

# ---------------------------------------------------------
# CAISO NATIVE SCRAPER (Full Tier 1) - v19.2 (Production Polish)
# Fix: Clamps DataAgeSeconds to 0 (prevents negative numbers in DB).
# ---------------------------------------------------------
@app.schedule(schedule="0 */5 * * * *", arg_name="caisoTimer", run_on_startup=True, use_monitor=False)
def GridScraper_CAISO(caisoTimer: func.TimerRequest) -> None:
    logging.info("Starting CAISO Tier 1 Scraper (OASIS API)...")

    # --- CONFIGURATION ---
    BASE_URL = "https://oasis.caiso.com/oasisapi/SingleZip"
    PRICE_NODE = "TH_NP15_GEN-APND"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "application/xml,application/zip,application/octet-stream"
    }

    # State Holders
    caiso_price = 0.0
    caiso_load = 0.0
    price_ts = None
    load_ts = None
    debug_status = "INIT"

    # Time Window
    now_utc = datetime.now(timezone.utc)
    start_time = (now_utc - timedelta(minutes=20)).strftime("%Y%m%dT%H:%M-0000")
    end_time = (now_utc + timedelta(minutes=5)).strftime("%Y%m%dT%H:%M-0000")

    import zipfile
    import io
    import xml.etree.ElementTree as ET
    import time

    # --- HELPER: OASIS CALLER ---
    def fetch_oasis(params, label):
        nonlocal debug_status
        try:
            r = requests.get(BASE_URL, params=params, headers=headers, timeout=20)
            if r.status_code != 200:
                debug_status = f"{label}_HTTP_{r.status_code}"
                return None
            if not r.content:
                debug_status = f"{label}_EMPTY"
                return None
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                if len(z.namelist()) == 0:
                    debug_status = f"{label}_BAD_ZIP"
                    return None
                filename = z.namelist()[0]
                with z.open(filename) as f:
                    return ET.fromstring(f.read())
        except Exception as e:
            debug_status = f"{label}_ERR_{str(e)[:10]}"
            return None

    # --- HELPER: PARSER (MAX VALUE HEURISTIC) ---
    def parse_oasis_xml(root, target_item_name, use_max_heuristic=False):
        best_val = None
        best_ts = None
        max_seen_value = -1.0

        for item in root.iter():
            if item.tag.endswith("REPORT_DATA"):
                data_map = {child.tag.split('}')[-1]: child.text for child in item}
                item_name = data_map.get("DATA_ITEM", "")

                if target_item_name in item_name:
                    try:
                        val = float(data_map.get("VALUE", 0))
                        t_str = data_map.get("INTERVAL_END_GMT")

                        if t_str:
                            ts = datetime.strptime(t_str, "%Y-%m-%dT%H:%M:%S-00:00").replace(tzinfo=timezone.utc)

                            if use_max_heuristic:
                                # For Load: Largest Value = System Total
                                if val > max_seen_value:
                                    max_seen_value = val
                                    best_val = val
                                    best_ts = ts
                            else:
                                # For Price: Latest Time = Real Time
                                if best_ts is None or ts > best_ts:
                                    best_ts = ts
                                    best_val = val
                    except: pass
        return best_val, best_ts

    # --- STEP 1: GET PRICE (RTM) ---
    try:
        price_params = {
            "queryname": "PRC_INTVL_LMP",
            "market_run_id": "RTM",
            "startdatetime": start_time,
            "enddatetime": end_time,
            "version": "1",
            "node": PRICE_NODE,
            "resultformat": "5"
        }
        root = fetch_oasis(price_params, "PRC")
        if root:
            val, ts = parse_oasis_xml(root, "LMP_PRC", use_max_heuristic=False)
            if ts:
                caiso_price = val
                price_ts = ts
            else:
                debug_status = "PRC_NO_DATA"
    except Exception as e:
        debug_status = f"PRC_FAIL_{str(e)[:10]}"

    time.sleep(1)

    # --- STEP 2: GET LOAD (RTM FORECAST) ---
    try:
        load_params = {
            "queryname": "SLD_FCST",
            "market_run_id": "RTM",
            "startdatetime": start_time,
            "enddatetime": end_time,
            "version": "1",
            "resultformat": "5"
        }
        root = fetch_oasis(load_params, "LOAD")
        if root:
            val, ts = parse_oasis_xml(root, "SYS_FCST_5MIN_MW", use_max_heuristic=True)
            if ts:
                caiso_load = val
                load_ts = ts
            else:
                if "PRC" not in debug_status: debug_status = "LOAD_NO_MATCH"
    except Exception as e:
         if "PRC" not in debug_status: debug_status = f"LOAD_FAIL_{str(e)[:10]}"

    # --- STEP 3: READ STATE (PERSISTENCE) ---
    try:
        conn_str = os.environ["AzureWebJobsStorage"]
        table_client = TableClient.from_connection_string(conn_str, table_name="GridStatusLatest")
        try:
            existing = table_client.get_entity(partition_key="US_GRID", row_key="CAISO")
            if caiso_load == 0 and existing.get("LoadMW", 0) > 0: caiso_load = existing["LoadMW"]
            if caiso_price == 0 and existing.get("LMP") is not None: caiso_price = existing["LMP"]
        except: pass
    except: pass

    # --- STEP 4: CONSOLIDATE ---
    master_ts = price_ts if price_ts else load_ts
    if load_ts and price_ts and load_ts > price_ts:
        master_ts = load_ts
    if not master_ts: master_ts = datetime.now(timezone.utc)

    # Lag Calculation
    delta = datetime.now(timezone.utc) - master_ts
    raw_sec = int(delta.total_seconds())

    # v19.2 Fix: Explicitly clamp 'lag_sec' to 0 if negative
    lag_sec = max(0, raw_sec)
    lag_min = int(lag_sec / 60)

    status_str = "UNKNOWN"
    if caiso_price != 0 or caiso_load != 0:
        status_str = f"OK ({lag_min}m)"
        if caiso_price == 0: status_str = "WARN: No Price"
        if caiso_load == 0: status_str = "WARN: No Load"
        if lag_min > 20: status_str = f"LAG {lag_min}m"
    else:
        status_str = f"ERR: {debug_status}"

    util_val = int(calculate_status(caiso_load, "CAISO")[0])

    payload = {
        "type": "grid", "region": "CAISO", "timestamp": master_ts.isoformat(),
        "load_mw": int(caiso_load),
        "utilization": util_val,
        "status": status_str, "trend": "STABLE",
        "lmp": round(caiso_price, 2),
        "lag_min": lag_min,
        "lag_sec": lag_sec # Now guaranteed >= 0
    }

    save_to_table(payload)

    if caiso_price != 0 or caiso_load != 0:
        date_path = f"CAISO/{master_ts.year}/{master_ts.month:02d}/{master_ts.day:02d}"
        file_name = f"{master_ts.strftime('%H-%M-%S')}.json"
        save_to_lake(payload, date_path, file_name)

    logging.info(f"CAISO Native: {status_str}")

# ---------------------------------------------------------
# API ENDPOINTS
# ---------------------------------------------------------
@app.route(route="status", auth_level=func.AuthLevel.FUNCTION)
def GetGridStatus(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn_str = os.environ["AzureWebJobsStorage"]
        table_client = TableClient.from_connection_string(conn_str, table_name="GridStatusLatest")
        entities = table_client.query_entities(query_filter="PartitionKey eq 'US_GRID'")
        final_data = {}
        for entity in entities:
            r_code = entity['RowKey']
            ts = entity['LastUpdated']
            stale, age_mins = is_data_stale(ts, r_code)
            status_val = "STALE_DATA" if stale else entity['Status']
            final_data[r_code.lower()] = {
                "latest": {
                    "load_mw": entity['LoadMW'], "utilization": entity['Utilization'],
                    "status": status_val, "timestamp": ts, "trend": entity.get('Trend', 'STABLE'),
                    "price_usd": entity.get('LMP'), "data_age_mins": round(age_mins, 1)
                }
            }
        return func.HttpResponse(json.dumps({"timestamp": datetime.now(timezone.utc).isoformat(), "status": "ONLINE", **final_data}), mimetype="application/json")
    except Exception as e: return func.HttpResponse(f"API Error: {str(e)}", status_code=500)

@app.route(route="curtailment", auth_level=func.AuthLevel.FUNCTION)
def GetCurtailmentSignal(req: func.HttpRequest) -> func.HttpResponse:
    try:
        region = req.params.get('region', '').upper()
        if not region: return func.HttpResponse("Missing region", status_code=400)
        user_price_cap = float(req.params.get('price_cap', 9999.0))
        user_stress_cap = float(req.params.get('stress_cap', 100.0))

        conn_str = os.environ["AzureWebJobsStorage"]
        table_client = TableClient.from_connection_string(conn_str, table_name="GridStatusLatest")
        try: entity = table_client.get_entity(partition_key="US_GRID", row_key=region)
        except: return func.HttpResponse("Region not found", status_code=404)

        timestamp = entity.get('LastUpdated', datetime.now(timezone.utc).isoformat())
        stale, age_mins = is_data_stale(timestamp, region)

        if stale:
            return func.HttpResponse(json.dumps({
                "region": region, "timestamp": timestamp, "curtail": False,
                "trigger_reason": f"DATA_STALE: Data is {int(age_mins)} mins old.",
                "metrics": { "data_age_mins": int(age_mins) }
            }), mimetype="application/json", status_code=422)

        curr_load = entity.get('LoadMW', 0)
        curr_price = entity.get('LMP')
        curr_stress = entity.get('Utilization', 0.0)
        should_curtail = False
        reasons = []
        if curr_price is not None and curr_price >= user_price_cap:
            should_curtail = True
            reasons.append(f"Price ${curr_price} > Limit ${user_price_cap}")
        if curr_stress >= user_stress_cap:
            should_curtail = True
            reasons.append(f"Grid Stress {curr_stress}% > Limit {user_stress_cap}%")

        return func.HttpResponse(json.dumps({
            "region": region, "timestamp": timestamp, "curtail": should_curtail,
            "trigger_reason": " AND ".join(reasons) if reasons else "Normal Operation",
            "metrics": { "utilization_pct": curr_stress, "price_usd": curr_price, "load_mw": curr_load }
        }), mimetype="application/json")
    except Exception as e: return func.HttpResponse(str(e), status_code=500)

@app.route(route="dispatch", auth_level=func.AuthLevel.FUNCTION)
def GetDispatchSignal(req: func.HttpRequest) -> func.HttpResponse:
    """
    Economic Dispatch Endpoint (The "All Cylinders" Signal).
    Triggers when prices DROP BELOW a user-defined floor (Default: $0.00).
    Used for: Opportunity Charging, Hydro/Battery Pumping, Mining Boost.
    """
    try:
        region = req.params.get('region', '').upper()
        if not region: return func.HttpResponse("Missing region", status_code=400)

        # Default: Trigger only on Negative Pricing ($0.00 or less)
        # Users can raise this (e.g., 5.0) to run when power is simply "cheap".
        price_floor = float(req.params.get('price_floor', 0.0))

        conn_str = os.environ["AzureWebJobsStorage"]
        table_client = TableClient.from_connection_string(conn_str, table_name="GridStatusLatest")
        try: entity = table_client.get_entity(partition_key="US_GRID", row_key=region)
        except: return func.HttpResponse("Region not found", status_code=404)

        timestamp = entity.get('LastUpdated', datetime.now(timezone.utc).isoformat())
        stale, age_mins = is_data_stale(timestamp, region)

        # Safety Check: Do not dispatch on stale data (Price might have spiked since then)
        if stale:
            return func.HttpResponse(json.dumps({
                "region": region, "timestamp": timestamp, "dispatch": False,
                "trigger_reason": f"DATA_STALE: Data is {int(age_mins)} mins old.",
                "metrics": { "data_age_mins": int(age_mins) }
            }), mimetype="application/json", status_code=422)

        curr_load = entity.get('LoadMW', 0)
        curr_price = entity.get('LMP')

        should_dispatch = False
        reasons = []

        # Logic: Inverted Curtailment (Low Price = GO)
        if curr_price is not None and curr_price <= price_floor:
            should_dispatch = True
            reasons.append(f"Price ${curr_price} <= Floor ${price_floor}")

        return func.HttpResponse(json.dumps({
            "region": region, "timestamp": timestamp, "dispatch": should_dispatch,
            "trigger_reason": " AND ".join(reasons) if reasons else "Price above dispatch floor",
            "metrics": { "price_usd": curr_price, "load_mw": curr_load, "dispatch_floor": price_floor }
        }), mimetype="application/json")
    except Exception as e: return func.HttpResponse(str(e), status_code=500)

@app.route(route="history", auth_level=func.AuthLevel.FUNCTION)
def GetRegionHistory(req: func.HttpRequest) -> func.HttpResponse:
    try:
        region = req.params.get('region')
        if not region: return func.HttpResponse("Missing region", status_code=400)
        conn_str = os.environ["AzureWebJobsStorage"]
        client = TableClient.from_connection_string(conn_str, table_name="GridStatusHistory")
        entities = client.query_entities(query_filter=f"PartitionKey eq '{region.upper()}'")
        history = [{"timestamp": e['RowKey'], "load_mw": e['LoadMW'], "price_usd": e.get("LMP")} for e in entities]
        history.sort(key=lambda x: x['timestamp'], reverse=True)
        return func.HttpResponse(json.dumps(history[:100]), mimetype="application/json")
    except Exception as e: return func.HttpResponse(str(e), status_code=500)

@app.route(route="dashboard/status", auth_level=func.AuthLevel.ANONYMOUS)
def DashboardStatusProxy(req: func.HttpRequest) -> func.HttpResponse:
    try:
        referer = req.headers.get("Referer", "")
        if not any(d in referer for d in ["gridwatch.live", "localhost"]): return func.HttpResponse("Unauthorized", status_code=403)
        key = os.environ.get('GRIDWATCH_INTERNAL_KEY')
        url = f"https://{os.environ.get('WEBSITE_HOSTNAME')}/api/status?code={key}"
        return func.HttpResponse(requests.get(url).text, mimetype="application/json")
    except: return func.HttpResponse("Proxy Error", status_code=500)

@app.route(route="dashboard/history", auth_level=func.AuthLevel.ANONYMOUS)
def DashboardHistoryProxy(req: func.HttpRequest) -> func.HttpResponse:
    try:
        referer = req.headers.get("Referer", "")
        if not any(d in referer for d in ["gridwatch.live", "localhost"]): return func.HttpResponse("Unauthorized", status_code=403)
        region = req.params.get('region')
        key = os.environ.get('GRIDWATCH_INTERNAL_KEY')
        url = f"https://{os.environ.get('WEBSITE_HOSTNAME')}/api/history?code={key}&region={region}"
        return func.HttpResponse(requests.get(url).text, mimetype="application/json")
    except: return func.HttpResponse("Proxy Error", status_code=500)
