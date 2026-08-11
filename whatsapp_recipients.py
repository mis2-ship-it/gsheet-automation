# =========================================================
# 📱 WHATSAPP RECIPIENT CONFIGURATION
# =========================================================

WHATSAPP_USERS = {

    # -----------------------------------------------------
    # OPS LEADERS
    # -----------------------------------------------------

    "919535075140": {
        "role": "Ops Leader",
        "region": "Overall",
        "patch": "All",
        "stores": ["All"]
    },

    "919620952646": {
        "role": "Ops Leader",
        "region": "Overall",
        "patch": "All",
        "stores": ["All"]
    },

    "919750820509": {
        "role": "Ops Leader",
        "region": "Overall",
        "patch": "All",
        "stores": ["All"]
    },

    # -----------------------------------------------------
    # REGION MANAGERS
    # -----------------------------------------------------

    "9163668270315": {
        "role": "Region Manager",
        "region": "MH",
        "patch": "MH Patch",
        "stores": ["All"]
    },

    "919611576106": {
        "role": "Region Manager",
        "region": "KA",
        "patch": "KA Patch",
        "stores": ["All"]
    },

    "919962098786": {
        "role": "Region Manager",
        "region": "TN",
        "patch": "TN Patch",
        "stores": ["All"]
    },

    "918892390985": {
        "role": "Region Manager",
        "region": "Kerala",
        "patch": "Kerala Patch",
        "stores": ["All"]
    },


    # -----------------------------------------------------
    # AREA MANAGERS
    # -----------------------------------------------------

    "916363327619": {
        "role": "Area Manager",
        "region": "KA",
        "patch": "Dayal",
        "stores": [
            "Tata Sherwood",
            "Manipal",
            "Tumkur",
            "Kempfort",
            "Whitefield",
            "Miraya Rose",
            "ITPL",
            "Gunjur",
            "AECS Layout",
            "Shivamogga"
        ]
    },

    "919538249461": {
        "role": "Area Manager",
        "region": "KA",
        "patch": "Kamrul",
        "stores": [
            "HSR Layout",
            "Sarjapur Road",
            "BTM Layout",
            "Kadubisanahalli - CF CK",
            "Harlur Road",
            "Koramangala",
            "Banashankari",
            "JP Nagar",
            "Ananth Nagar",
            "Meenakshi Mall"
        ]
    },

    "919641503119": {
        "role": "Area Manager",
        "region": "KA",
        "patch": "Reazul",
        "stores": [
            "Indiranagar - CK",
            "Kammanhalli",
            "Basaveshwarnagar",
            "Bel Road",
            "Yelahanka",
            "Frazer Town",
            "Nagavara",
            "Kolar- Highway Star",
            "Kanakapura",
            "Channasandra"
        ]
    },

    "919901062323": {
        "role": "Area Manager",
        "region": "KA",
        "patch": "Vasanth",
        "stores": [
            "Lubov Store"
        ]
    },

    "918921196734": {
        "role": "Area Manager",
        "region": "Kerala",
        "patch": "Anirudh",
        "stores": [
            "Ravipuram",
            "Kakkanad",
            "Thiruvalla"
        ]
    },

    "918002028360": {
        "role": "Area Manager",
        "region": "MH",
        "patch": "Aditya",
        "stores": [
            "Koregaon Park - Pune",
            "Wagholi - CF CK",
            "Sinhagad",
            "Hinjewadi",
            "Baner Road - Pune",
            "Hinjewadi Phase 3"
        ]
    },

    "919137989719": {
        "role": "Area Manager",
        "region": "MH",
        "patch": "Anju Bharati",
        "stores": [
            "Byculla",
            "Khar",
            "Prabhadevi",
            "Thakur Village",
            "Lokhandwala",
            "Malad - CF - CK",
            "Kalyan",
            "Badlapur"
        ]
    },

    "918108421347": {
        "role": "Area Manager",
        "region": "MH",
        "patch": "Jitendra",
        "stores": [
            "Sher- E-Punjab",
            "Dahisar",
            "Virar",
            "Mira Road",
            "Marol - CF CK",
            "Mulund",
            "Manpada - CF CK",
            "Powai- CF - CK",
            "Kamothe",
            "SEAWOOD"
        ]
    },

    "919791052114": {
        "role": "Area Manager",
        "region": "TN",
        "patch": "Akash",
        "stores": [
            "Valsarvakkam",
            "Mogappair",
            "Vellore",
            "Race Course Road",
            "Besant Nagar",
            "Express Avenue Mall",
            "Nanganallur CK",
            "Annanagar"
        ]
    },

    "919710831016": {
        "role": "Area Manager",
        "region": "TN",
        "patch": "Kamlesh",
        "stores": [
            "Pallikaranai",
            "Mudichur",
            "OMR",
            "Thoraipakkam",
            "Velachery",
            "Guduvanchery",
            "Urapakkam CK"
        ]
    },

    "919176555420": {
        "role": "Area Manager",
        "region": "TN",
        "patch": "Krishna Kumar",
        "stores": [
            "Iyyappanthangal - CK",
            "Zamin Pallavaram",
            "Nungambakkam - CK",
            "Kolathur",
            "Perambur - CK",
            "Erode",
            "Alwarpet"
        ]
    }
}


# =========================================================
# 📱 WHATSAPP RECIPIENTS
# =========================================================

WHATSAPP_RECIPIENTS = list(
    WHATSAPP_USERS.keys()
)


# =========================================================
# 🔐 GET USER ACCESS
# =========================================================

def get_user_access(sender):

    sender = (
        str(sender)
        .replace("+", "")
        .replace(" ", "")
        .replace("-", "")
    )

    user = WHATSAPP_USERS.get(sender)

    print("=" * 60)
    print("🔐 WHATSAPP USER ACCESS")
    print("Sender :", sender)

    if not user:

        print("❌ User not mapped")
        print("=" * 60)

        return None

    print("Role   :", user["role"])
    print("Region :", user["region"])
    print("Patch  :", user["patch"])
    print("Stores :", user["stores"])

    print("=" * 60)

    return user


# =========================================================
# 🏪 CHECK STORE ACCESS
# =========================================================

def user_can_access_store(
    sender,
    store_name,
    region=None
):

    user = get_user_access(sender)

    if not user:
        return False

    # -----------------------------------------------------
    # OPS LEADER
    # -----------------------------------------------------

    if user["role"] == "Ops Leader":

        return True

    # -----------------------------------------------------
    # REGION MANAGER
    # -----------------------------------------------------

    if user["role"] == "Region Manager":

        if region is None:
            return False

        return (
            str(region).strip().lower()
            ==
            str(user["region"]).strip().lower()
        )

    # -----------------------------------------------------
    # AREA MANAGER
    # -----------------------------------------------------

    if user["role"] == "Area Manager":

        if region is not None:

            if (
                str(region).strip().lower()
                !=
                str(user["region"]).strip().lower()
            ):
                return False

        allowed_stores = {
            str(x).strip().lower()
            for x in user["stores"]
        }

        return (
            str(store_name).strip().lower()
            in allowed_stores
        )

    return False


