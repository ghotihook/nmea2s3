-- metrics_1s — the queryable view over `observations`.
--
-- nmea2s3-update-pg writes one column per decoded field and takes last() per
-- bucket. It deliberately does not choose between instruments, and
-- deliberately does not average. Both of those are policy, both change, and
-- both are cheap here and irreversible if done at write time. This file is
-- where they belong.
--
-- ADAPT IT TO YOUR ARCHIVE. `observations` only has columns for fields that
-- have actually appeared in yours, so a chain naming a field no device of
-- yours reports fails the script with `column ... does not exist`. Delete
-- the entries you do not carry — the chains are laid out one per line for
-- exactly that reason, and `\d observations` shows what you have.
--
-- Re-run it after fitting an instrument, with that instrument's field added
-- to its chain. CREATE OR REPLACE keeps the view's identity and its grants.
--
--   psql "$DATABASE_URL" -f sql/metrics.sql
--
-- Two names, and the naming says which is which:
--   observations  the TABLE nmea2s3-update-pg writes. One column per decoded
--                 field, every instrument kept, nothing resolved — what was
--                 REPORTED.
--   metrics_1s    this view. Instruments resolved into named quantities —
--                 what you query, and what a dashboard should point at.
--
-- `1s` names the grain `observations` was written at, which is
-- nmea2s3-update-pg's default. Ingest at another --bucket and this wants
-- renaming to match, rather than claiming a grain it no longer has.
--
-- Resampling coarser than the source grain is NOT here, because it cannot
-- be done with avg() alone — see the note at the foot of this file.
--
-- A plain view, so it costs nothing to keep and is always current. If the
-- archive grows past what your queries tolerate, MATERIALIZED VIEW plus an
-- index on ts is the next step, at the price of having to REFRESH it.


-- ── metrics_1s: arbitration ─────────────────────────────────────────────
--
-- COALESCE, in preference order: the first instrument that reported this
-- bucket wins and contributes ITS OWN value. Never an average across a
-- chain — two instruments disagreeing by a known offset must not be blended
-- into a number neither of them measured.
--
-- No device is named anywhere, only fields. N2K source addresses are leased
-- by ISO address claiming and change when the bus is repowered, so a chain
-- keyed on an address silently repoints itself at a different sensor.
--
-- The orderings below carry real measurements behind them:
--   sog/cog   RMC before VTG — finer resolution on the same GPS fix
--             (a measured 0.14 kn / 2.4° disagreement between them)
--   heel      xdr_roll before xdr_m5_heel — they differ by ~4.5°, and the
--             calibration these feed was fitted on this ordering

CREATE OR REPLACE VIEW metrics_1s AS
SELECT
    ts,

    -- wind
    COALESCE(n2k_windangle_apparent, mwv_wind_angle_r)                    AS awa,
    COALESCE(n2k_windspeed_apparent, mwv_wind_speed_r)                    AS aws,
    COALESCE(n2k_windangle_true_boat_referenced, mwv_wind_angle_t)        AS twa,
    COALESCE(n2k_windspeed_true_boat_referenced, mwv_wind_speed_t)        AS tws,
    COALESCE(n2k_windangle_magnetic_ground_referenced_to_magnetic_north,
             mwd_direction_magnetic)                                      AS twd,

    -- motion
    COALESCE(n2k_speedwaterreferenced, vhw_water_speed_knots)             AS stw,
    COALESCE(n2k_sog, rmc_spd_over_grnd, vtg_spd_over_grnd_kts)           AS sog,
    COALESCE(n2k_cog, rmc_true_course, vtg_true_track)                    AS cog,
    COALESCE(n2k_heading_magnetic, hdg_heading, hdm_heading)              AS hdg,
    COALESCE(n2k_roll, xdr_roll, xdr_m5_heel)                             AS heel,
    COALESCE(n2k_pitch, xdr_pitch, xdr_m5_pitch)                          AS pitch,
    -- both sides are degrees per SECOND: the 0183 wire carries deg/minute
    -- and the decoder scales it, so this chain is unit-consistent
    COALESCE(n2k_rate, rot_rate_of_turn)                                  AS rot,
    COALESCE(n2k_position_0, rsa_rsa_starboard)                           AS rudder,

    -- position and environment
    COALESCE(n2k_latitude, rmc_latitude, gll_latitude)                    AS lat,
    COALESCE(n2k_longitude, rmc_longitude, gll_longitude)                 AS lon,
    COALESCE(n2k_depth, dbt_depth_meters)                                 AS depth,
    COALESCE(n2k_actualtemperature_0_sea_temperature, mda_water_temp)     AS temp_sea,
    COALESCE(n2k_voltage_0, xdr_battv)                                    AS batt_v,

    -- the wall clock measured against the GPS clock. Positive and small is
    -- normal: the sentence is stamped on receipt, one transmission time
    -- after the fix it reports. A step here is the system clock moving.
    rmc_clock_offset                                                      AS clock_offset,

    -- what the instrument itself computed, kept separate from anything
    -- derived downstream so the two can be compared rather than confused
    n2k_set                                                               AS bus_set,
    n2k_drift                                                             AS bus_drift,
    n2k_variation_magnetic                                                AS bus_variation,
    n2k_leewayangle                                                       AS bus_leeway,

    -- raw transducer channels, before any instrument processing
    xdr_raw_wind_s                                                        AS aws_raw,
    xdr_raw_wind_a                                                        AS awa_raw,
    xdr_raw_bsp                                                           AS stw_raw

FROM observations;

COMMENT ON VIEW metrics_1s IS
    'Instrument chains resolved by COALESCE, in preference order: the first '
    'instrument that reported a bucket wins and contributes its own value. '
    'Defined in sql/metrics.sql.';


-- ── read access ─────────────────────────────────────────────────────────
--
-- Granted on the VIEW only. A view runs its underlying query as its owner,
-- so ro_user can read metrics_1s with no privilege on `observations` at all
-- — the resolved layer is readable and the raw per-instrument table is not.
-- That is a reason to point a dashboard at the view beyond convenience.
--
-- The role has to exist first. This file does not create it: what that role
-- may do elsewhere in the cluster is not this file's business.
--
--     CREATE ROLE ro_user LOGIN PASSWORD '...';
--
-- CREATE OR REPLACE VIEW preserves grants, so re-running this file keeps
-- them. DROP VIEW followed by CREATE does not — if you ever change the
-- view's column set that way, run this GRANT again afterwards.

GRANT USAGE ON SCHEMA public TO ro_user;
GRANT SELECT ON metrics_1s TO ro_user;


-- ── if you resample, angles need a circular mean ────────────────────────
--
-- Not included as a view, because which coarser grain you want is your
-- question, not this file's. But it is the one part you cannot get right
-- with avg(), so the recipe is here rather than left as an exercise.
--
-- The arithmetic mean of 359° and 1° is 180° — a heading pointing exactly
-- backwards, produced silently from two readings two degrees apart. The
-- mean of an angle is the direction of the summed unit vectors.
--
-- Three classes of column, and putting one in the wrong class yields a
-- plausible number rather than an error:
--
--   BEARINGS      compass directions, [0, 360). Circular, then wrapped.
--                 cog, hdg, twd, bus_set
--   SIGNED ANGLES relative to the boat, (-180, 180]. Circular, left signed
--                 so port stays negative. awa, twa
--   EVERYTHING    speeds, depths, temperatures, and small angles that never
--   ELSE          approach the wrap (heel, pitch, rudder, leeway,
--                 variation). Plain avg().
--
-- lat/lon take a plain average, right everywhere except across the
-- antimeridian. If you sail through 180° longitude, average lon circularly.
--
-- CREATE VIEW metrics_1m AS
-- SELECT date_trunc('minute', ts) AS ts,
--        count(*) AS n,
--        -- bearing: circular, wrapped to [0, 360)
--        (SELECT CASE WHEN d < 0 THEN d + 360.0 ELSE d END
--         FROM (SELECT round(degrees(atan2(avg(sin(radians(cog))),
--                                          avg(cos(radians(cog)))))::numeric,
--                            6) AS d) x)                       AS cog,
--        -- relative angle: circular, left signed
--        round(degrees(atan2(avg(sin(radians(awa))),
--                            avg(cos(radians(awa)))))::numeric, 6) AS awa,
--        -- linear
--        avg(sog) AS sog, avg(stw) AS stw, avg(heel) AS heel
-- FROM metrics_1s
-- GROUP BY 1;
--
-- Two details that are easy to lose:
--
--   The round() is not cosmetic. Values that cancel exactly — 359 and 1,
--   averaging to due north — leave atan2 with a sine term of about -1e-17
--   rather than 0, so the result sits a hair BELOW zero and the wrap turns
--   it into 359.999999999999. Six decimals is ~4 micro-arcseconds, far
--   finer than any instrument on the bus.
--
--   A mean direction says nothing about scatter, and the circular mean of
--   evenly opposed readings is arbitrary. The concentration R is the length
--   of the resultant unit vector — 1.0 for perfect agreement, near 0 for
--   noise:
--
--     sqrt(avg(sin(radians(cog)))^2 + avg(cos(radians(cog)))^2) AS cog_r
--
--   Treat a mean bearing with R below ~0.7 as not meaning much.
