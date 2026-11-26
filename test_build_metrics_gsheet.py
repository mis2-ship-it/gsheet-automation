"""The Google Sheet reader: the sheet's values, read the way it returns them, must build the same data as the workbook.
Run: python test_build_metrics_gsheet.py  (uses the workbook path below)"""
import datetime as dt, json, numbers, os, sys, types
import pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE) if os.path.basename(HERE) == 'tests' else HERE
sys.path.insert(0, ROOT)
import build_metrics as bm

XLSX = os.environ.get("METRICS_XLSX", "/mnt/user-data/uploads/Metrics_Data.xlsx")
PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    PASS, FAIL = (PASS + 1, FAIL) if cond else (PASS, FAIL + 1)
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))


NUM_COLS = {"menu_opens", "cart_built", "order_makes", "menu_sessions", "cart_session", "order_session",
            "clicks", "ad_orders", "ads_orders"}


def to_sheet(df):
    """The rows as a sheet returns them: a header row, dates as serial numbers, some numbers as text with commas,
    blanks as empty text."""
    head = [str(c) for c in df.columns]
    rows = [head]
    for rec in df.itertuples(index=False):
        out = []
        for c, v in zip(head, rec):
            if v is None or (not isinstance(v, str) and pd.isna(v)):
                out.append("")
            elif isinstance(v, pd.Timestamp):
                out.append(float((v - pd.Timestamp("1899-12-30")).days))
            elif isinstance(v, numbers.Number) and c in NUM_COLS:
                out.append(f"{v:,.0f}" if float(v).is_integer() else str(v))
            else:
                out.append(v.item() if hasattr(v, "item") else v)
        rows.append(out)
    return rows


# the workbook, as the reference
ref_frames = bm.read_xlsx(XLSX)
ref = bm.build(ref_frames)
TABS_SHEET = {name: to_sheet(ref_frames[name]) for name in bm.TABS}
LOG = {"id": None, "from_dict": 0, "from_file": 0}


class FakeWS:
    def __init__(self, title, values): self.title, self._v = title, values
    def get_all_values(self, value_render_option=None):
        assert value_render_option == "UNFORMATTED_VALUE", value_render_option
        return self._v


class FakeSS:
    def __init__(self, tabs): self._t = tabs
    def worksheets(self): return [FakeWS(t, v) for t, v in self._t.items()]
    def worksheet(self, name): return FakeWS(name, self._t[name])


class FakeGC:
    def __init__(self, tabs): self._tabs = tabs
    def open_by_key(self, key):
        LOG["id"] = key
        return FakeSS(self._tabs)


fake = types.ModuleType("gspread")
def _from_dict(d):
    LOG["from_dict"] += 1
    return FakeGC(TABS_SHEET)
def _from_file(filename):
    LOG["from_file"] += 1
    return FakeGC(TABS_SHEET)
fake.service_account_from_dict = _from_dict
fake.service_account = _from_file
sys.modules["gspread"] = fake

SHEET_ID = "1s9tNe9QnlOzbOgsrus-pvlZzulRDxGft"
# 1. the serial dates and comma numbers convert back to the same values
check("a serial date converts to its day", bm.iso_day(46326.0) == (dt.date(1899, 12, 30) + dt.timedelta(days=46326)).isoformat())
check("a blank date is dropped (None)", bm.iso_day("") is None and bm.iso_day(None) is None)
check("ISO text still works", bm.iso_day("2026-11-05") == "2026-11-05")
check("comma numbers read as numbers, blanks as 0", list(bm.num(pd.Series(["1,234", "", 56.0, "7"])).round(6)) == [1234.0, 0.0, 56.0, 7.0])
# 2. the sheet, read as the sheet returns it, builds the same data as the workbook
got_frames = bm.read_gsheet(SHEET_ID, '{"type": "service_account", "client_email": "x@y.iam.gserviceaccount.com"}')
got = bm.build(got_frames)
check("the key from text is used (not a file)", LOG["from_dict"] == 1 and LOG["from_file"] == 0)
check("the sheet id is opened", LOG["id"] == SHEET_ID)
check("the sheet builds the same rows as the workbook", got["rows"] == ref["rows"], f"{len(got['rows'])} vs {len(ref['rows'])}")
check("the sheet builds the same dates, sources, brands, regions and stores",
      all(got[k] == ref[k] for k in ["dateMin", "dateMax", "sources", "brands", "regions", "storeTypes", "stores"]),
      f"{got['dateMin']} to {got['dateMax']}")
bm.read_gsheet(SHEET_ID, "/path/to/key.json")
check("a key file path is still accepted", LOG["from_file"] == 1)
# 3. a missing tab stops with the tabs it found
short = {k: v for k, v in TABS_SHEET.items() if k != "S_Ads"}
TABS_SHEET_SAVE = TABS_SHEET
TABS_SHEET = short
try:
    bm.read_gsheet(SHEET_ID, "/k.json")
    msg = ""
except SystemExit as e:
    msg = str(e)
check("a missing tab stops with its name and the tabs found", "S_Ads" in msg and "Tabs found" in msg, msg[:90])
TABS_SHEET = TABS_SHEET_SAVE
print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
