"""
FK-ODH hub Status / Region mapping for Debit Master (authoritative list from operations).
Keys: hub identifier as produced from Hub Name (uppercase, strip, leading quote removed).
"""


def normalize_hub_lookup_key(hub) -> str:
    return str(hub).strip().upper().strip("'\"")


# (Status, Region) per hub — must match pivot row labels after _normalize_hub_code on source data
_HUB_STATUS_REGION: dict[str, tuple[str, str]] = {
    "BAGALURUMDH_BAG": ("Active", "Bangalore"),
    "BIDARFORTHUB_BDR": ("Active", "ROK"),
    "CABTSRNAGARODH_HYD": ("Active", "Hyderabad"),
    "DOMMASANDRASPLITODH_DMN": ("Active", "Bangalore"),
    "ELASTICRUNBIDARODH_BDR": ("Active", "ROK"),
    "ELASTICRUNCHEVELLAODH_VKB": ("Closed", "ROK"),
    "HULIMAVUHUB_BLR": ("Active", "Bangalore"),
    "IDENTIFYPLUSNARASAPURAMDH_KLR": ("Closed", "ROK"),
    "KOORIEEATTAPURODH_HYD": ("Closed", "ROK"),
    "KOORIEEHAYATHNAGARODH_HYD": ("Active", "Bangalore"),
    "KOORIEESOUKYARDODH_BLR": ("Active", "Bangalore"),
    "KOORIEESOUKYARDTEMPODH_BLR": ("Active", "Bangalore"),
    "LARGELOGICCHINNAMANURODH_CNM": ("Active", "Tamil Nadu"),
    "LARGELOGICDHARAPURAMODH_DHP": ("Active", "Tamil Nadu"),
    "LARGELOGICKUNIYAMUTHURODH_CJB": ("Active", "Tamil Nadu"),
    "LARGELOGICRAMESWARAMODH_RMS": ("Active", "Tamil Nadu"),
    "MOINDABADMDH_MDN": ("Closed", "Hyderabad"),
    "NAUBADMDH_BDR": ("Active", "ROK"),
    "SAIDABADSPILTODH": ("Active", "Hyderabad"),
    "SAIDABADSPLITODH_HYD": ("Active", "Hyderabad"),
    "SITICSWADIODH_WDI": ("Active", "ROK"),
    "SULEBELEMDH_SUL": ("Active", "Bangalore"),
    "THAVAREKEREMDH_THK": ("Active", "Bangalore"),
    "TTSPLBATLAGUNDUODH_BGU": ("Active", "Tamil Nadu"),
    "TTSPLKODAIKANALODH_KDI": ("Active", "Tamil Nadu"),
    "VADIPATTIMDH_VDP": ("Active", "Tamil Nadu"),
}


def attachment_status_region(hub) -> tuple[str, str] | None:
    """Return (Status, Region) if hub is in the official map; else None."""
    k = normalize_hub_lookup_key(hub)
    return _HUB_STATUS_REGION.get(k)


# When hub is not in FK-ODH map (e.g. Meesho 3-letter codes), treat these as Active
_MEESHO_STYLE_ACTIVE = frozenset({"MQR", "MQE", "MHK", "YLZ", "YLG"})


def hub_is_active_for_report(hub) -> bool:
    """True if hub should appear on Active-only Debit Master / Recovery Pending outputs."""
    att = attachment_status_region(hub)
    if att:
        return att[0] == "Active"
    return str(hub).strip() in _MEESHO_STYLE_ACTIVE
