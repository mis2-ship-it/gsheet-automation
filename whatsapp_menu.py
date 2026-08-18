from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional

from whatsapp_recipients import get_user_access


SESSION_TTL_SECONDS = 30 * 60
MAX_INTERACTIVE_ROWS = 10

MAIN_MENU = "main_menu"
ANALYSIS_MENU = "analysis_menu"
PERIOD_MENU = "period_menu"
BRAND_MENU = "brand_menu"
REGION_MENU = "region_menu"
STORE_MENU = "store_menu"
SOURCE_MENU = "source_menu"
RANKING_MENU = "ranking_menu"


@dataclass
class MenuSession:
    sender: str
    state: str = MAIN_MENU
    analysis: Optional[str] = None
    period: Optional[str] = None
    brand: Optional[str] = None
    region: Optional[str] = None
    store: Optional[str] = None
    source: Optional[str] = None
    ranking: Optional[str] = None
    updated_at: float = field(default_factory=time.time)


_SESSIONS: dict[str, MenuSession] = {}


def _normalize(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _clean_id(value: Any) -> str:
    return _normalize(value).replace(" ", "_").replace("-", "_")


def _role(user: dict) -> str:
    return _normalize(user.get("role", ""))


def _session_valid(session: MenuSession) -> bool:
    return time.time() - session.updated_at <= SESSION_TTL_SECONDS


def get_session(sender: str) -> MenuSession:
    sender = str(sender)
    session = _SESSIONS.get(sender)
    if session is None or not _session_valid(session):
        session = MenuSession(sender=sender)
        _SESSIONS[sender] = session
    session.updated_at = time.time()
    return session


def clear_session(sender: str) -> None:
    _SESSIONS.pop(str(sender), None)


def reset_session(sender: str) -> MenuSession:
    clear_session(sender)
    return get_session(sender)


def get_main_menu_options(sender: str) -> list[dict[str, str]]:
    user = get_user_access(sender)
    if not user:
        return []

    options = [
        {"id": "today_sales", "title": "Today Sales", "description": "Current FTD sales"},
        {"id": "last_week_sales", "title": "Last Week Sales", "description": "Monday to Sunday"},
        {"id": "last_month_sales", "title": "Last Month Sales", "description": "Previous calendar month"},
        {"id": "last_year_sales", "title": "Last Year Sales", "description": "Same-period comparison"},
        {"id": "historical", "title": "Historical / 6 Months", "description": "Monthly trend and growth"},
        {"id": "store_performance", "title": "Store Performance", "description": "Store-level analysis"},
        {"id": "brand_performance", "title": "Brand Performance", "description": "Brand-level analysis"},
        {"id": "region_performance", "title": "Region Performance", "description": "Region-level analysis"},
        {"id": "source_performance", "title": "Source Performance", "description": "Swiggy, Zomato, In Store"},
        {"id": "rankings", "title": "Top / Bottom", "description": "Top and bottom performers"},
    ]

    role = _role(user)

    if role == "area manager":
        options = [x for x in options if x["id"] not in {"region_performance", "source_performance"}]
    elif role == "region manager":
        options = [x for x in options if x["id"] != "region_performance"]

    return options


def get_analysis_options(sender: str) -> list[dict[str, str]]:
    user = get_user_access(sender)
    if not user:
        return []

    options = [
        {"id": "overall", "title": "Overall", "description": "All available sales"},
        {"id": "brand", "title": "Brand", "description": "Frozen Bottle, Madno"},
        {"id": "region", "title": "Region", "description": "KA, TN, MH, KL"},
        {"id": "store", "title": "Store", "description": "Select a store"},
        {"id": "source", "title": "Source", "description": "Swiggy, Zomato, In Store"},
    ]

    role = _role(user)
    if role == "area manager":
        return [x for x in options if x["id"] == "store"]
    if role == "region manager":
        return [x for x in options if x["id"] in {"brand", "store"}]
    return options


def get_period_options() -> list[dict[str, str]]:
    return [
        {"id": "today", "title": "Today", "description": "Current business day"},
        {"id": "last_week", "title": "Last Week", "description": "Monday to Sunday"},
        {"id": "last_month", "title": "Last Month", "description": "Previous calendar month"},
        {"id": "last_3_months", "title": "Last 3 Months", "description": "Monthly performance"},
        {"id": "last_6_months", "title": "Last 6 Months", "description": "Monthly trend"},
        {"id": "last_12_months", "title": "Last 12 Months", "description": "Annual trend"},
    ]


def get_brand_options(sender: str, available_brands: Optional[list[str]] = None) -> list[dict[str, str]]:
    if not get_user_access(sender):
        return []
    brands = available_brands or ["Frozen Bottle", "Madno", "Boba Bar", "Lubov"]
    return [{"id": f"brand:{_clean_id(x)}", "title": str(x)[:24], "description": "Select brand"} for x in brands]


def get_region_options(sender: str, available_regions: Optional[list[str]] = None) -> list[dict[str, str]]:
    user = get_user_access(sender)
    if not user:
        return []

    if _role(user) == "region manager":
        region = str(user.get("region", "")).strip()
        regions = [region] if region else []
    else:
        regions = available_regions or ["KA", "TN", "MH", "KL"]

    return [{"id": f"region:{_clean_id(x)}", "title": str(x)[:24], "description": "Select region"} for x in regions if str(x).strip()]


def get_store_options(
    sender: str,
    stores: Optional[dict] = None,
    region: Optional[str] = None,
) -> list[dict[str, str]]:
    user = get_user_access(sender)
    if not user:
        return []

    stores = stores or {}
    role = _role(user)

    if role == "area manager":
        allowed = user.get("stores", [])
        if not isinstance(allowed, list):
            allowed = [allowed]
        if any(_normalize(x) == "all" for x in allowed):
            names = list(stores.keys())
        else:
            names = [str(x).strip() for x in allowed if str(x).strip()]
    else:
        names = list(stores.keys())

    if region:
        region_norm = _normalize(region)
        names = [
            name for name in names
            if region_norm in _normalize(stores.get(name, {}).get("region", ""))
            or _normalize(stores.get(name, {}).get("region", "")) in region_norm
        ]

    names = sorted(set(names), key=str.lower)
    return [{"id": f"store:{_clean_id(x)}", "title": x[:24], "description": "Select store"} for x in names]


def get_source_options(sender: str, available_sources: Optional[list[str]] = None) -> list[dict[str, str]]:
    if not get_user_access(sender):
        return []
    sources = available_sources or ["Swiggy", "Zomato", "In Store", "Ownly", "Magicpin", "Website", "Others"]
    return [{"id": f"source:{_clean_id(x)}", "title": str(x)[:24], "description": "Select source"} for x in sources]


def get_ranking_options() -> list[dict[str, str]]:
    return [
        {"id": "top_stores", "title": "Top Stores", "description": "Highest sales"},
        {"id": "bottom_stores", "title": "Bottom Stores", "description": "Lowest sales"},
        {"id": "top_brands", "title": "Top Brands", "description": "Highest brand sales"},
        {"id": "bottom_brands", "title": "Bottom Brands", "description": "Lowest brand sales"},
        {"id": "top_regions", "title": "Top Regions", "description": "Highest region sales"},
        {"id": "bottom_regions", "title": "Bottom Regions", "description": "Lowest region sales"},
    ]


def build_list_message(
    recipient: str,
    body_text: str,
    options: list[dict[str, str]],
    button_text: str = "Select",
    section_title: str = "Options",
) -> dict[str, Any]:
    rows = [
        {
            "id": str(x["id"])[:200],
            "title": str(x.get("title", ""))[:24],
            "description": str(x.get("description", ""))[:72],
        }
        for x in options[:MAX_INTERACTIVE_ROWS]
    ]
    return {
        "messaging_product": "whatsapp",
        "to": str(recipient),
        "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {"text": body_text},
            "action": {
                "button": button_text[:20],
                "sections": [{"title": section_title[:24], "rows": rows}],
            },
        },
    }


def build_button_message(
    recipient: str,
    body_text: str,
    options: list[dict[str, str]],
) -> dict[str, Any]:
    buttons = [
        {
            "type": "reply",
            "reply": {
                "id": str(x["id"])[:200],
                "title": str(x.get("title", ""))[:20],
            },
        }
        for x in options[:3]
    ]
    return {
        "messaging_product": "whatsapp",
        "to": str(recipient),
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": body_text},
            "action": {"buttons": buttons},
        },
    }


def build_text_menu(title: str, options: list[dict[str, str]]) -> str:
    lines = ["🤖 *AI MIS*", "", title, ""]
    for i, option in enumerate(options, 1):
        lines.append(f"{i}. {option.get('title', '')}")
    lines.extend(["", "Reply with the number.", "Type *menu* to return."])
    return "\n".join(lines)


def start_menu(sender: str) -> dict[str, Any]:
    session = reset_session(sender)
    options = get_main_menu_options(sender)
    session.state = MAIN_MENU
    return {
        "state": MAIN_MENU,
        "options": options,
        "text": build_text_menu("What are you looking for today?", options),
    }


def _next_period(sender: str) -> dict[str, Any]:
    session = get_session(sender)
    options = get_period_options()
    session.state = PERIOD_MENU
    return {
        "state": PERIOD_MENU,
        "options": options,
        "text": build_text_menu("Select period.", options),
    }


def _next_brand(sender: str) -> dict[str, Any]:
    session = get_session(sender)
    options = get_brand_options(sender)
    session.state = BRAND_MENU
    return {
        "state": BRAND_MENU,
        "options": options,
        "text": build_text_menu("Select brand.", options),
    }


def _next_region(sender: str, regions: Optional[list[str]] = None) -> dict[str, Any]:
    session = get_session(sender)
    options = get_region_options(sender, regions)
    session.state = REGION_MENU
    return {
        "state": REGION_MENU,
        "options": options,
        "text": build_text_menu("Select region.", options),
    }


def _next_store(sender: str, live_snapshot: Optional[dict] = None, region: Optional[str] = None) -> dict[str, Any]:
    session = get_session(sender)
    options = get_store_options(sender, (live_snapshot or {}).get("stores", {}), region)
    session.state = STORE_MENU
    if not options:
        return {
            "state": STORE_MENU,
            "options": [],
            "text": "❌ No stores are available for your selection.\n\nType *menu* to start again.",
        }
    return {
        "state": STORE_MENU,
        "options": options,
        "text": build_text_menu("Select store.", options),
    }


def _next_source(sender: str, sources: Optional[list[str]] = None) -> dict[str, Any]:
    session = get_session(sender)
    options = get_source_options(sender, sources)
    session.state = SOURCE_MENU
    return {
        "state": SOURCE_MENU,
        "options": options,
        "text": build_text_menu("Select source.", options),
    }


def _next_analysis(sender: str) -> dict[str, Any]:
    session = get_session(sender)
    options = get_analysis_options(sender)
    session.state = ANALYSIS_MENU
    return {
        "state": ANALYSIS_MENU,
        "options": options,
        "text": build_text_menu("Select what you want to analyze.", options),
    }


def _next_ranking(sender: str) -> dict[str, Any]:
    session = get_session(sender)
    options = get_ranking_options()
    session.state = RANKING_MENU
    return {
        "state": RANKING_MENU,
        "options": options,
        "text": build_text_menu("Select ranking.", options),
    }


def handle_menu_selection(
    sender: str,
    selection: str,
    live_snapshot: Optional[dict] = None,
) -> dict[str, Any]:
    session = get_session(sender)
    value = _normalize(selection)

def handle_menu_selection(
    sender: str,
    selection: str,
    live_snapshot: Optional[dict] = None,
) -> dict[str, Any]:
    session = get_session(sender)
    value = _normalize(selection)

    # =====================================================
    # DIRECT MAIN MENU ACTIONS
    # =====================================================
    
    if value == "today_sales":
    
        session.period = "today"
    
        return {
            "handled": True,
            "action": "today_sales",
            "next_menu": None,
            "session": session,
        }
    
    if value == "last_week_sales":
    
        session.period = "last_week"
    
        return {
            "handled": True,
            "action": "last_week_sales",
            "next_menu": None,
            "session": session,
        }
    
    if value == "last_month_sales":
    
        session.period = "last_month"
    
        return {
            "handled": True,
            "action": "last_month_sales",
            "next_menu": None,
            "session": session,
        }
    
    if value == "last_year_sales":
    
        session.period = "last_year"
    
        return {
            "handled": True,
            "action": "last_year_sales",
            "next_menu": None,
            "session": session,
        }
    
    if value == "historical":
    
        session.period = "last_6_months"
    
        return {
            "handled": True,
            "action": "historical",
            "next_menu": None,
            "session": session,
        }

    if value in {"menu", "main menu", "home", "start"}:
        return {"handled": True, "action": "menu", "next_menu": start_menu(sender), "session": session}

    if value.startswith("brand:"):
        session.brand = selection.split(":", 1)[1].replace("_", " ").title()
        return {"handled": True, "action": "brand_selected", "next_menu": _next_period(sender), "session": session}

    if value.startswith("region:"):
        session.region = selection.split(":", 1)[1].replace("_", " ").upper()
        if session.analysis == "store":
            return {"handled": True, "action": "region_selected", "next_menu": _next_store(sender, live_snapshot, session.region), "session": session}
        return {"handled": True, "action": "region_selected", "next_menu": _next_period(sender), "session": session}

    if value.startswith("store:"):
        session.store = selection.split(":", 1)[1].replace("_", " ").title()
        return {"handled": True, "action": "store_selected", "next_menu": _next_period(sender), "session": session}

    if value.startswith("source:"):
        session.source = selection.split(":", 1)[1].replace("_", " ").title()
        return {"handled": True, "action": "source_selected", "next_menu": _next_period(sender), "session": session}

    direct_main = {
        "today sales": ("today_sales", "today"),
        "last week sales": ("last_week_sales", "last_week"),
        "last month sales": ("last_month_sales", "last_month"),
        "last year sales": ("last_year_sales", "last_year"),
        "historical": ("historical", "last_6_months"),
    }
    if value in direct_main:
        action, period = direct_main[value]
        session.period = period
        return {"handled": True, "action": action, "next_menu": _next_analysis(sender), "session": session}

    if value == "store performance":
        session.analysis = "store"
        return {"handled": True, "action": "store_performance", "next_menu": _next_region(sender, list((live_snapshot or {}).get("regions", {}).keys()) or None), "session": session}

    if value == "brand performance":
        session.analysis = "brand"
        return {"handled": True, "action": "brand_performance", "next_menu": _next_brand(sender), "session": session}

    if value == "region performance":
        session.analysis = "region"
        return {"handled": True, "action": "region_performance", "next_menu": _next_region(sender, list((live_snapshot or {}).get("regions", {}).keys()) or None), "session": session}

    if value == "source performance":
        session.analysis = "source"
        return {"handled": True, "action": "source_performance", "next_menu": _next_source(sender, list((live_snapshot or {}).get("sources", {}).keys()) or None), "session": session}

    if value == "rankings":
        session.analysis = "ranking"
        return {"handled": True, "action": "rankings", "next_menu": _next_ranking(sender), "session": session}

    if value in {"overall", "brand", "region", "store", "source"}:
        session.analysis = value
        if value == "brand":
            return {"handled": True, "action": "analysis_brand", "next_menu": _next_brand(sender), "session": session}
        if value == "region":
            return {"handled": True, "action": "analysis_region", "next_menu": _next_region(sender, list((live_snapshot or {}).get("regions", {}).keys()) or None), "session": session}
        if value == "store":
            return {"handled": True, "action": "analysis_store", "next_menu": _next_region(sender, list((live_snapshot or {}).get("regions", {}).keys()) or None), "session": session}
        if value == "source":
            return {"handled": True, "action": "analysis_source", "next_menu": _next_source(sender, list((live_snapshot or {}).get("sources", {}).keys()) or None), "session": session}
        return {"handled": True, "action": "analysis_overall", "next_menu": _next_period(sender), "session": session}

    if value in {"today", "last_week", "last_month", "last_3_months", "last_6_months", "last_12_months"}:
        session.period = value
        if session.analysis == "overall":
            return {"handled": True, "action": "generate_overall", "next_menu": None, "session": session}
        if session.analysis == "ranking":
            return {"handled": True, "action": "generate_ranking", "next_menu": None, "session": session}
        if session.analysis == "brand" and session.brand:
            return {"handled": True, "action": "generate_brand", "next_menu": None, "session": session}
        if session.analysis == "region" and session.region:
            return {"handled": True, "action": "generate_region", "next_menu": None, "session": session}
        if session.analysis == "store" and session.store:
            return {"handled": True, "action": "generate_store", "next_menu": None, "session": session}
        if session.analysis == "source" and session.source:
            return {"handled": True, "action": "generate_source", "next_menu": None, "session": session}
        return {"handled": True, "action": "period_selected", "next_menu": None, "session": session}

    if value in {"top_stores", "bottom_stores", "top_brands", "bottom_brands", "top_regions", "bottom_regions"}:
        session.ranking = value
        return {"handled": True, "action": "generate_ranking", "next_menu": None, "session": session}

    if value.isdigit():
        number = int(value)
        builders = {
            MAIN_MENU: lambda: get_main_menu_options(sender),
            ANALYSIS_MENU: lambda: get_analysis_options(sender),
            PERIOD_MENU: get_period_options,
            BRAND_MENU: lambda: get_brand_options(sender),
            REGION_MENU: lambda: get_region_options(sender),
            STORE_MENU: lambda: get_store_options(sender, (live_snapshot or {}).get("stores", {}), session.region),
            SOURCE_MENU: lambda: get_source_options(sender),
            RANKING_MENU: get_ranking_options,
        }
        options = builders.get(session.state, lambda: [])()
        if 1 <= number <= len(options):
            return handle_menu_selection(sender, options[number - 1]["id"], live_snapshot)

    return {"handled": False, "action": None, "next_menu": None, "session": session}


def get_session_payload(sender: str) -> dict[str, Any]:
    session = get_session(sender)
    return {
        "sender": session.sender,
        "state": session.state,
        "analysis": session.analysis,
        "period": session.period,
        "brand": session.brand,
        "region": session.region,
        "store": session.store,
        "source": session.source,
        "ranking": session.ranking,
        "updated_at": session.updated_at,
    }


def get_menu_help(sender: str) -> str:
    return build_text_menu(
        "What are you looking for today?",
        get_main_menu_options(sender)
    )


if __name__ == "__main__":
    print(get_menu_help("919750820509"))
