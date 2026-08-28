"""Physically impossible values, dropped before anything is bucketed.

Load-bearing, and more so under `last()` than under a mean. A mean dilutes
one wild reading across its bucket; `last()` can hand it the bucket outright,
so a single corrupt sentence becomes the recorded value for that second and
looks exactly like a real observation ever after.

Found 2026-08-14 in the pipeline this came from: a corrupted MWV sentence
reported an apparent wind speed of 1003.1 knots. Wind speed had no guard at
all, and that value silently corrupted its second.

A field with no entry here is not range-checked. That is deliberate — the
table names what is *physically* impossible for a sailing yacht's
instruments, not what is merely surprising, and inventing bounds for a field
nobody has looked at would drop real data to no purpose.
"""

# Compass bearings, 0-360.
BEARING_FIELDS: frozenset[str] = frozenset({
    "rmc_true_course", "vtg_true_track", "hdm_heading", "hdg_heading",
    "mwd_direction_magnetic",
    "n2k_cog", "n2k_heading_magnetic", "n2k_set",
    "n2k_windangle_magnetic_ground_referenced_to_magnetic_north",
})

# Relative angles, folded to (-180, 180] so the sign says which side.
#
# UNVERIFIED for the n2k entries: which convention the nmea2000 library
# returns for PGN 130306 has not been confirmed against real traffic. The
# wire format is 0..2pi, which would make them bearings instead. Check the
# first real day before trusting apparent/true wind angle where the two
# streams overlap.
SIGNED_FIELDS: frozenset[str] = frozenset({
    "mwv_wind_angle_r", "mwv_wind_angle_t",
    "n2k_windangle_apparent", "n2k_windangle_true_boat_referenced",
})

RANGES: dict[str, tuple[float, float]] = {
    **{f: (0.0, 360.0) for f in BEARING_FIELDS},
    **{f: (-180.0, 360.0) for f in SIGNED_FIELDS},
    "rmc_latitude": (-90.0, 90.0), "gll_latitude": (-90.0, 90.0),
    "n2k_latitude": (-90.0, 90.0),
    "rmc_longitude": (-180.0, 180.0), "gll_longitude": (-180.0, 180.0),
    "n2k_longitude": (-180.0, 180.0),
    "xdr_m5_heel": (-90.0, 90.0), "xdr_roll": (-90.0, 90.0), "n2k_roll": (-90.0, 90.0),
    "xdr_m5_pitch": (-90.0, 90.0), "xdr_pitch": (-90.0, 90.0), "n2k_pitch": (-90.0, 90.0),
    "mda_water_temp": (0.0, 40.0),
    "n2k_actualtemperature_0_sea_temperature": (0.0, 40.0),
    # 100 kn is well beyond any wind a sailboat's instruments report in one
    # piece — hurricane-force gusts sit around 150 kn and onboard gear fails
    # well before that — so this only ever catches corrupt data, never
    # real weather.
    "mwv_wind_speed_r": (0.0, 100.0), "mwv_wind_speed_t": (0.0, 100.0),
    "n2k_windspeed_apparent": (0.0, 100.0),
    "n2k_windspeed_true_boat_referenced": (0.0, 100.0),
    "xdr_raw_wind_s": (0.0, 100.0),
}


def in_range(field: str, value: float) -> bool:
    """True if the value is possible for this field, or if nothing is known
    about the field's range."""
    bounds = RANGES.get(field)
    return bounds is None or bounds[0] <= value <= bounds[1]
