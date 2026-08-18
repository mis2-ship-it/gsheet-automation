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

    # Multiple selection support
    periods: list[str] = field(default_factory=list)
    brands: list[str] = field(default_factory=list)
    regions: list[str] = field(default_factory=list)
    stores: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)

    ranking: Optional[str] = None

    updated_at: float = field(
        default_factory=time.time
    )


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

    options = [
        {
            "id": "period:today",
            "title": "Today",
            "description": "Current business day",
        },
        {
            "id": "period:last_week",
            "title": "Last Week",
            "description": "Monday to Sunday",
        },
        {
            "id": "period:last_month",
            "title": "Last Month",
            "description": "Previous calendar month",
        },
        {
            "id": "period:last_year",
            "title": "Last Year",
            "description": "Same period last year",
        },
        {
            "id": "period:last_3_months",
            "title": "Last 3 Months",
            "description": "Monthly performance",
        },
        {
            "id": "period:last_6_months",
            "title": "Last 6 Months",
            "description": "Monthly trend",
        },
        {
            "id": "period:last_12_months",
            "title": "Last 12 Months",
            "description": "Annual trend",
        },
        {
            "id": "period:add_another",
            "title": "Add Another",
            "description": "Select another period",
        },
        {
            "id": "period:done",
            "title": "Done",
            "description": "Finish period selection",
        },
    ]

    return options


def get_brand_options(
    sender: str,
    available_brands: Optional[list[str]] = None,
) -> list[dict[str, str]]:

    if not get_user_access(
        sender
    ):
        return []

    brands = (
        available_brands
        or
        [
            "Frozen Bottle",
            "Madno",
            "Boba Bar",
            "Lubov",
        ]
    )

    options = [
        {
            "id": f"brand:{_clean_id(name)}",
            "title": str(name)[:24],
            "description": "Select brand",
        }
        for name in brands
    ]

    options.extend(
        [
            {
                "id": "brand:add_another",
                "title": "Add Another",
                "description": "Select another brand",
            },
            {
                "id": "brand:done",
                "title": "Done",
                "description": "Finish brand selection",
            },
        ]
    )

    return options


def get_region_options(
    sender: str,
    available_regions: Optional[list[str]] = None,
) -> list[dict[str, str]]:

    user = get_user_access(
        sender
    )

    if not user:
        return []

    role = _role(
        user
    )

    if role == "region manager":

        region = str(
            user.get(
                "region",
                ""
            )
        ).strip()

        regions = (
            [region]
            if region
            else []
        )

    else:

        regions = (
            available_regions
            or
            [
                "KA",
                "Kerala",
                "MH",
                "TN",
            ]
        )

    options = [
        {
            "id": f"region:{_clean_id(region)}",
            "title": str(region)[:24],
            "description": "Select region",
        }
        for region in regions
        if str(region).strip()
    ]

    options.extend(
        [
            {
                "id": "region:add_another",
                "title": "Add Another",
                "description": "Select another region",
            },
            {
                "id": "region:done",
                "title": "Done",
                "description": "Finish region selection",
            },
        ]
    )

    return options


def get_store_options(
    sender: str,
    stores: Optional[dict] = None,
    regions: Optional[list[str]] = None,
) -> list[dict[str, str]]:

    user = get_user_access(
        sender
    )

    if not user:
        return []

    stores = stores or {}

    role = _role(
        user
    )

    # -----------------------------------------------------
    # AREA MANAGER
    # -----------------------------------------------------

    if role == "area manager":

        allowed = user.get(
            "stores",
            []
        )

        if not isinstance(
            allowed,
            list
        ):
            allowed = [allowed]

        if any(
            _normalize(x) == "all"
            for x in allowed
        ):

            names = list(
                stores.keys()
            )

        else:

            names = [
                str(x).strip()
                for x in allowed
                if str(x).strip()
            ]

    else:

        names = list(
            stores.keys()
        )

    # -----------------------------------------------------
    # REGION FILTER
    #
    # IMPORTANT:
    # Some live store snapshots currently contain
    # Region = UNKNOWN.
    #
    # Therefore do NOT remove all stores when region
    # metadata is missing.
    # -----------------------------------------------------

    if regions:

        region_set = {
            _normalize(x)
            for x in regions
        }

        filtered = []

        for store_name in names:

            store_data = (
                stores.get(
                    store_name,
                    {}
                )
                or {}
            )

            store_region = _normalize(
                store_data.get(
                    "region",
                    ""
                )
            )

            if (
                store_region
                and
                store_region != "unknown"
            ):

                if store_region in region_set:
                    filtered.append(
                        store_name
                    )

            else:

                # Keep stores with missing region metadata.
                # Final report must apply region filtering
                # from the actual historical data.
                filtered.append(
                    store_name
                )

        names = filtered

    names = sorted(
        {
            str(x).strip()
            for x in names
            if str(x).strip()
        },
        key=lambda x: x.lower()
    )

    options = [
        {
            "id": f"store:{_clean_id(name)}",
            "title": name[:24],
            "description": "Select store",
        }
        for name in names
    ]

    # -----------------------------------------------------
    # MULTI SELECT CONTROLS
    # -----------------------------------------------------

    if options:

        options.append(
            {
                "id": "store:add_another",
                "title": "Add Another",
                "description": "Select another store",
            }
        )

    options.append(
        {
            "id": "store:done",
            "title": "Done",
            "description": "Finish store selection",
        }
    )

    return options


def get_source_options(
    sender: str,
    available_sources: Optional[list[str]] = None,
) -> list[dict[str, str]]:

    if not get_user_access(
        sender
    ):
        return []

    sources = (
        available_sources
        or
        [
            "Swiggy",
            "Zomato",
            "In Store",
            "Ownly",
            "Magicpin",
            "Website",
            "Others",
        ]
    )

    options = [
        {
            "id": f"source:{_clean_id(source)}",
            "title": str(source)[:24],
            "description": "Select source",
        }
        for source in sources
        if str(source).strip()
    ]

    options.extend(
        [
            {
                "id": "source:add_another",
                "title": "Add Another",
                "description": "Select another source",
            },
            {
                "id": "source:done",
                "title": "Done",
                "description": "Finish source selection",
            },
        ]
    )

    return options


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


def _next_store(
    sender,
    live_snapshot=None,
    regions=None
):

    session = get_session(
        sender
    )

    options = get_store_options(
        sender,
        (
            live_snapshot or {}
        ).get(
            "stores",
            {}
        ),
        regions
    )

    session.state = STORE_MENU

    if not options:

        return {
            "state": STORE_MENU,
            "options": [
                {
                    "id": "store:done",
                    "title": "Done",
                    "description": "Finish selection",
                }
            ],
            "text": (
                "🏪 *Select Store*\n\n"
                "No store list is currently available."
            ),
        }

    return {
        "state": STORE_MENU,
        "options": options,
        "text": build_text_menu(
            "Select store(s).",
            options
        ),
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


# =========================================================
# 📱 HANDLE GUIDED MENU SELECTION
# =========================================================

def handle_menu_selection(
    sender: str,
    selection: str,
    live_snapshot: Optional[dict] = None,
) -> dict[str, Any]:

    session = get_session(
        sender
    )

    value = _normalize(
        selection
    )

    print("=" * 60)
    print("📱 MENU SELECTION")
    print("Sender   :", sender)
    print("Selection:", selection)
    print("Value    :", value)
    print("State    :", session.state)
    print("=" * 60)

    # =====================================================
    # MAIN MENU
    # =====================================================

    if value in {
        "menu",
        "main menu",
        "home",
        "start",
    }:

        return {
            "handled": True,
            "action": "menu",
            "next_menu": start_menu(
                sender
            ),
            "session": session,
        }

    # =====================================================
    # TODAY SALES
    # =====================================================

    if value == "today_sales":

        session.periods = [
            "today"
        ]

        return {
            "handled": True,
            "action": "today_sales",
            "next_menu": None,
            "session": session,
        }

    # =====================================================
    # LAST WEEK SALES
    # =====================================================

    if value == "last_week_sales":

        session.periods = [
            "last_week"
        ]

        session.analysis = None

        session.brands = []
        session.regions = []
        session.stores = []
        session.sources = []

        return {
            "handled": True,
            "action": None,
            "next_menu": _next_brand(
                sender
            ),
            "session": session,
        }

    # =====================================================
    # LAST MONTH SALES
    # =====================================================

    if value == "last_month_sales":

        session.periods = [
            "last_month"
        ]

        session.analysis = None

        session.brands = []
        session.regions = []
        session.stores = []
        session.sources = []

        return {
            "handled": True,
            "action": None,
            "next_menu": _next_brand(
                sender
            ),
            "session": session,
        }

    # =====================================================
    # LAST YEAR SALES
    # =====================================================

    if value == "last_year_sales":

        session.periods = [
            "last_year"
        ]

        session.analysis = None

        session.brands = []
        session.regions = []
        session.stores = []
        session.sources = []

        return {
            "handled": True,
            "action": None,
            "next_menu": _next_brand(
                sender
            ),
            "session": session,
        }

    # =====================================================
    # HISTORICAL
    # =====================================================

    if value == "historical":

        session.periods = [
            "last_6_months"
        ]

        session.analysis = None

        session.brands = []
        session.regions = []
        session.stores = []
        session.sources = []

        return {
            "handled": True,
            "action": None,
            "next_menu": _next_brand(
                sender
            ),
            "session": session,
        }

    # =====================================================
    # PERIOD PREFIX
    # =====================================================

    if value.startswith(
        "period:"
    ):

        selected = (
            selection
            .split(
                ":",
                1
            )[1]
        )

        selected_norm = _normalize(
            selected
        )

        # ---------------------------------------------
        # ADD ANOTHER
        # ---------------------------------------------

        if selected_norm == "add another":

            return {
                "handled": True,
                "action": None,
                "next_menu": _next_period(
                    sender
                ),
                "session": session,
            }

        # ---------------------------------------------
        # DONE
        # ---------------------------------------------

        if selected_norm == "done":

            if not session.periods:

                return {
                    "handled": True,
                    "action": None,
                    "next_menu": _next_period(
                        sender
                    ),
                    "session": session,
                }

            return {
                "handled": True,
                "action": None,
                "next_menu": _next_brand(
                    sender
                ),
                "session": session,
            }

        # ---------------------------------------------
        # ADD PERIOD
        # ---------------------------------------------

        if selected not in session.periods:

            session.periods.append(
                selected
            )

        return {
            "handled": True,
            "action": None,
            "next_menu": _next_period(
                sender
            ),
            "session": session,
        }

    # =====================================================
    # BRAND PREFIX
    # =====================================================

    if value.startswith(
        "brand:"
    ):

        selected = (
            selection
            .split(
                ":",
                1
            )[1]
            .replace(
                "_",
                " "
            )
            .title()
        )

        selected_norm = _normalize(
            selected
        )

        # ---------------------------------------------
        # ADD ANOTHER
        # ---------------------------------------------

        if selected_norm == "add another":

            return {
                "handled": True,
                "action": None,
                "next_menu": _next_brand(
                    sender
                ),
                "session": session,
            }

        # ---------------------------------------------
        # DONE
        # ---------------------------------------------

        if selected_norm == "done":

            if not session.brands:

                return {
                    "handled": True,
                    "action": None,
                    "next_menu": _next_brand(
                        sender
                    ),
                    "session": session,
                }

            return {
                "handled": True,
                "action": None,
                "next_menu": _next_region(
                    sender,
                    list(
                        (
                            live_snapshot
                            or
                            {}
                        )
                        .get(
                            "regions",
                            {}
                        )
                        .keys()
                    )
                    or None
                ),
                "session": session,
            }

        # ---------------------------------------------
        # ADD BRAND
        # ---------------------------------------------

        if selected not in session.brands:

            session.brands.append(
                selected
            )

        return {
            "handled": True,
            "action": None,
            "next_menu": _next_brand(
                sender
            ),
            "session": session,
        }

    # =====================================================
    # REGION PREFIX
    # =====================================================

    if value.startswith(
        "region:"
    ):

        selected = (
            selection
            .split(
                ":",
                1
            )[1]
            .replace(
                "_",
                " "
            )
        )

        selected_norm = _normalize(
            selected
        )

        # ---------------------------------------------
        # ADD ANOTHER
        # ---------------------------------------------

        if selected_norm == "add another":

            return {
                "handled": True,
                "action": None,
                "next_menu": _next_region(
                    sender
                ),
                "session": session,
            }

        # ---------------------------------------------
        # DONE
        # ---------------------------------------------

        if selected_norm == "done":

            if not session.regions:

                return {
                    "handled": True,
                    "action": None,
                    "next_menu": _next_region(
                        sender
                    ),
                    "session": session,
                }

            return {
                "handled": True,
                "action": None,
                "next_menu": _next_store(
                    sender,
                    live_snapshot,
                    session.regions
                ),
                "session": session,
            }

        # ---------------------------------------------
        # REGION NAME
        # ---------------------------------------------

        if (
            selected_norm
            ==
            "kerela"
        ):

            selected = "Kerala"

        elif (
            selected_norm
            ==
            "kerala"
        ):

            selected = "Kerala"

        elif selected:

            selected = selected.upper()

        # ---------------------------------------------
        # ADD REGION
        # ---------------------------------------------

        if selected not in session.regions:

            session.regions.append(
                selected
            )

        return {
            "handled": True,
            "action": None,
            "next_menu": _next_region(
                sender
            ),
            "session": session,
        }

    # =====================================================
    # STORE PREFIX
    # =====================================================

    if value.startswith(
        "store:"
    ):

        selected = (
            selection
            .split(
                ":",
                1
            )[1]
            .replace(
                "_",
                " "
            )
            .title()
        )

        selected_norm = _normalize(
            selected
        )

        # ---------------------------------------------
        # ADD ANOTHER
        # ---------------------------------------------

        if selected_norm == "add another":

            return {
                "handled": True,
                "action": None,
                "next_menu": _next_store(
                    sender,
                    live_snapshot,
                    session.regions
                ),
                "session": session,
            }

        # ---------------------------------------------
        # DONE
        # ---------------------------------------------

        if selected_norm == "done":

            if not session.stores:

                return {
                    "handled": True,
                    "action": None,
                    "next_menu": _next_store(
                        sender,
                        live_snapshot,
                        session.regions
                    ),
                    "session": session,
                }

            return {
                "handled": True,
                "action": "generate_period_report",
                "next_menu": None,
                "session": session,
            }

        # ---------------------------------------------
        # ADD STORE
        # ---------------------------------------------

        if selected not in session.stores:

            session.stores.append(
                selected
            )

        return {
            "handled": True,
            "action": None,
            "next_menu": _next_store(
                sender,
                live_snapshot,
                session.regions
            ),
            "session": session,
        }

    # =====================================================
    # SOURCE PREFIX
    # =====================================================

    if value.startswith(
        "source:"
    ):

        selected = (
            selection
            .split(
                ":",
                1
            )[1]
            .replace(
                "_",
                " "
            )
            .title()
        )

        selected_norm = _normalize(
            selected
        )

        # ---------------------------------------------
        # ADD ANOTHER
        # ---------------------------------------------

        if selected_norm == "add another":

            return {
                "handled": True,
                "action": None,
                "next_menu": _next_source(
                    sender
                ),
                "session": session,
            }

        # ---------------------------------------------
        # DONE
        # ---------------------------------------------

        if selected_norm == "done":

            if not session.sources:

                return {
                    "handled": True,
                    "action": None,
                    "next_menu": _next_source(
                        sender
                    ),
                    "session": session,
                }

            return {
                "handled": True,
                "action": "sources_selected",
                "next_menu": _next_period(
                    sender
                ),
                "session": session,
            }

        # ---------------------------------------------
        # ADD SOURCE
        # ---------------------------------------------

        if selected not in session.sources:

            session.sources.append(
                selected
            )

        return {
            "handled": True,
            "action": None,
            "next_menu": _next_source(
                sender
            ),
            "session": session,
        }

    # =====================================================
    # STORE PERFORMANCE
    # =====================================================

    if value == "store performance":

        session.analysis = "store"

        # Start with regions
        return {
            "handled": True,
            "action": None,
            "next_menu": _next_region(
                sender,
                list(
                    (
                        live_snapshot
                        or
                        {}
                    )
                    .get(
                        "regions",
                        {}
                    )
                    .keys()
                )
                or None
            ),
            "session": session,
        }

    # =====================================================
    # BRAND PERFORMANCE
    # =====================================================

    if value == "brand performance":

        session.analysis = "brand"

        return {
            "handled": True,
            "action": None,
            "next_menu": _next_brand(
                sender
            ),
            "session": session,
        }

    # =====================================================
    # REGION PERFORMANCE
    # =====================================================

    if value == "region performance":

        session.analysis = "region"

        return {
            "handled": True,
            "action": None,
            "next_menu": _next_region(
                sender,
                list(
                    (
                        live_snapshot
                        or
                        {}
                    )
                    .get(
                        "regions",
                        {}
                    )
                    .keys()
                )
                or None
            ),
            "session": session,
        }

    # =====================================================
    # SOURCE PERFORMANCE
    # =====================================================

    if value == "source performance":

        session.analysis = "source"

        return {
            "handled": True,
            "action": None,
            "next_menu": _next_source(
                sender,
                list(
                    (
                        live_snapshot
                        or
                        {}
                    )
                    .get(
                        "sources",
                        {}
                    )
                    .keys()
                )
                or None
            ),
            "session": session,
        }

    # =====================================================
    # RANKINGS
    # =====================================================

    if value == "rankings":

        session.analysis = "ranking"

        return {
            "handled": True,
            "action": None,
            "next_menu": _next_ranking(
                sender
            ),
            "session": session,
        }

    # =====================================================
    # RANKING SELECTION
    # =====================================================

    if value in {
        "top_stores",
        "bottom_stores",
        "top_brands",
        "bottom_brands",
        "top_regions",
        "bottom_regions",
    }:

        session.ranking = value

        # Ranking still needs a period.
        return {
            "handled": True,
            "action": None,
            "next_menu": _next_period(
                sender
            ),
            "session": session,
        }

    # =====================================================
    # GENERIC ANALYSIS
    # =====================================================

    if value in {
        "overall",
        "brand",
        "region",
        "store",
        "source",
    }:

        session.analysis = value

        if value == "brand":

            return {
                "handled": True,
                "action": None,
                "next_menu": _next_brand(
                    sender
                ),
                "session": session,
            }

        if value == "region":

            return {
                "handled": True,
                "action": None,
                "next_menu": _next_region(
                    sender,
                    list(
                        (
                            live_snapshot
                            or
                            {}
                        )
                        .get(
                            "regions",
                            {}
                        )
                        .keys()
                    )
                    or None
                ),
                "session": session,
            }

        if value == "store":

            return {
                "handled": True,
                "action": None,
                "next_menu": _next_region(
                    sender,
                    list(
                        (
                            live_snapshot
                            or
                            {}
                        )
                        .get(
                            "regions",
                            {}
                        )
                        .keys()
                    )
                    or None
                ),
                "session": session,
            }

        if value == "source":

            return {
                "handled": True,
                "action": None,
                "next_menu": _next_source(
                    sender,
                    list(
                        (
                            live_snapshot
                            or
                            {}
                        )
                        .get(
                            "sources",
                            {}
                        )
                        .keys()
                    )
                    or None
                ),
                "session": session,
            }

        return {
            "handled": True,
            "action": None,
            "next_menu": _next_period(
                sender
            ),
            "session": session,
        }

    # =====================================================
    # LEGACY TEXT PERIOD VALUES
    # =====================================================

    if value in {
        "today",
        "last_week",
        "last_month",
        "last_year",
        "last_3_months",
        "last_6_months",
        "last_12_months",
    }:

        if value not in session.periods:

            session.periods.append(
                value
            )

        return {
            "handled": True,
            "action": None,
            "next_menu": _next_brand(
                sender
            ),
            "session": session,
        }

    # =====================================================
    # NUMERIC FALLBACK
    # =====================================================

    if value.isdigit():

        number = int(
            value
        )

        builders = {

            MAIN_MENU:
                lambda:
                get_main_menu_options(
                    sender
                ),

            ANALYSIS_MENU:
                lambda:
                get_analysis_options(
                    sender
                ),

            PERIOD_MENU:
                get_period_options,

            BRAND_MENU:
                lambda:
                get_brand_options(
                    sender
                ),

            REGION_MENU:
                lambda:
                get_region_options(
                    sender
                ),

            STORE_MENU:
                lambda:
                get_store_options(
                    sender,
                    (
                        live_snapshot
                        or
                        {}
                    ).get(
                        "stores",
                        {}
                    ),
                    session.regions
                ),

            SOURCE_MENU:
                lambda:
                get_source_options(
                    sender
                ),

            RANKING_MENU:
                get_ranking_options,
        }

        options = builders.get(
            session.state,
            lambda: []
        )()

        if (
            1
            <= number
            <= len(options)
        ):

            return handle_menu_selection(
                sender,
                options[
                    number - 1
                ][
                    "id"
                ],
                live_snapshot
            )

    # =====================================================
    # UNKNOWN
    # =====================================================

    return {
        "handled": False,
        "action": None,
        "next_menu": None,
        "session": session,
    }


def get_session_payload(sender: str) -> dict[str, Any]:
    session = get_session(sender)
    return {
        "sender": session.sender,
        "state": session.state,
        "analysis": session.analysis,
        "periods": session.periods,
        "brands": session.brands,
        "regions": session.regions,
        "stores": session.stores,
        "sources": session.sources,
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
