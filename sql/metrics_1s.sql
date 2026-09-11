-- metrics_1s — fr_observations resolved, calibrated and labelled.
--
-- STATIC. Written against the 95 columns fr_observations had on 2026-09-11 —
-- every column named below exists there. Runs with nothing but psql:
--
--     psql "$DSN" -f sql/metrics_1s.sql
--
-- If a column is missing the CREATE fails naming it ("column o.<name> does not
-- exist"). Delete it from its COALESCE and run again; `\d fr_observations`
-- shows what the table has. CREATE OR REPLACE keeps the view's grants.
--
-- A plain VIEW, deliberately. Nothing is stored, so nothing goes stale: a
-- relabelled leg, a back-dated calibration or a late spool object shows up in
-- the next query, and there is no rebuild step.
--
-- Three layers:
--
--   fr_observations   nmea2s3-update-pg --table fr_observations. 1 s buckets,
--                     one column per decoded field, every instrument kept.
--   metrics_1s        this. One column per quantity, man_bsp_adj as of ts,
--                     sessions and legs LEFT joined.
--   ra_*              race-annotate's labels and calibrations, read-only here.

-- Stop at the first error rather than carrying on to the COMMENT and GRANT
-- against a view that was never created.
\set ON_ERROR_STOP on

CREATE OR REPLACE VIEW public.metrics_1s AS
SELECT
    r.*,

    -- ── calibration ─────────────────────────────────────────────────────────
    -- The man_bsp_adj row in force at ts: the newest effective_from <= ts.
    -- man_bsp_adj is that lookup as recorded, NULL before the first row.
    -- adj_stw takes a missing factor as 1.0, so it is the raw log there
    -- rather than NULL.
    c.param_value              AS man_bsp_adj,
    c.effective_from           AS cal_effective_from,  -- group by this for an epoch
    c.cal_id,                                          -- back to ra_calibrations.notes
    a.adj_stw,

    -- ── labels ──────────────────────────────────────────────────────────────
    s.session_id,
    s.name                     AS session_name,
    s.type                     AS session_type,
    g.seg_id,
    g.no                       AS leg_no,
    g.leg_type,                -- the LABELLED value; null outside a leg
    g.mainsail,
    g.headsail,

    -- ── speed and leeway models ─────────────────────────────────────────────
    -- Appended, not placed beside adj_stw: CREATE OR REPLACE VIEW can only add
    -- columns at the end, and fails on an existing view otherwise.
    k.corrected_stw,
    -- heel in degrees, so leeway in degrees and carrying heel's sign. Speed
    -- floored at 1 kn and the result capped at 15 degrees, so a heeled boat
    -- at a standstill cannot produce a leeway of hundreds of degrees. The
    -- 0 * term is not a no-op: GREATEST skips NULLs, so without it a second
    -- with heel but no stw would get a leeway computed at the 1 kn floor.
    sign(r.heel) * LEAST(9.2 * abs(r.heel) / (GREATEST(k.corrected_stw, 1.0) ^ 2), 15)
        + 0 * k.corrected_stw AS leeway

FROM (
    SELECT
        o.ts,
        COALESCE(o.n2k_windangle_apparent, o.mwv_wind_angle_r) AS awa,
        COALESCE(o.n2k_windspeed_apparent, o.mwv_wind_speed_r) AS aws,
        COALESCE(o.n2k_windangle_true_boat_referenced, o.mwv_wind_angle_t) AS twa,
        COALESCE(o.n2k_windspeed_true_boat_referenced, o.mwv_wind_speed_t) AS tws,
        COALESCE(o.n2k_windangle_magnetic_ground_referenced_to_magnetic_north, o.mwd_direction_magnetic) AS twd,
        -- the speed that goes with twd: true wind over the GROUND, not the boat
        o.n2k_windspeed_magnetic_ground_referenced_to_magnetic_north AS tws_ground,
        COALESCE(o.n2k_speedwaterreferenced, o.vhw_water_speed_knots) AS stw,
        -- PGN 128275, METRES. Log distance against GPS distance over a leg is
        -- a calibration check that does not depend on 1 s speed noise.
        o.n2k_log AS log_total,
        o.n2k_triplog AS log_trip,
        COALESCE(o.n2k_sog, o.rmc_spd_over_grnd, o.vtg_spd_over_grnd_kts) AS sog,
        COALESCE(o.n2k_cog, o.rmc_true_course, o.vtg_true_track) AS cog,
        COALESCE(o.n2k_heading_magnetic, o.hdg_heading, o.hdm_heading) AS hdg,
        COALESCE(o.n2k_roll, o.xdr_roll, o.xdr_m5_heel) AS heel,
        COALESCE(o.n2k_pitch, o.xdr_pitch, o.xdr_m5_pitch) AS pitch,
        COALESCE(o.n2k_rate, o.rot_rate_of_turn) AS rot,
        COALESCE(o.n2k_position_0, o.rsa_rsa_starboard) AS rudder,
        COALESCE(o.n2k_latitude, o.rmc_latitude, o.gll_latitude) AS lat,
        COALESCE(o.n2k_longitude, o.rmc_longitude, o.gll_longitude) AS lon,
        COALESCE(o.n2k_depth, o.dbt_depth_meters) AS depth,
        -- 130312 on this bus; 130316 and 130310 would go first if they appear
        COALESCE(o.n2k_actualtemperature_0_sea_temperature, o.mda_water_temp) AS temp_sea,
        o.n2k_actualtemperature_0_outside_temperature AS temp_air,  -- Celsius
        o.n2k_pressure_0_atmospheric AS pressure,                   -- PASCALS
        COALESCE(o.n2k_voltage_0, o.xdr_battv) AS batt_v,
        COALESCE(o.n2k_gps_time, o.n2k_gps_time_gps, o.rmc_gps_time) AS gps_time,
        -- fix quality, for throwing out seconds whose sog/cog are not worth
        -- comparing a log against
        o.n2k_hdop AS gnss_hdop,
        o.n2k_numberofsvs AS gnss_sats,
        o.n2k_set AS bus_set,
        o.n2k_drift AS bus_drift,
        -- what the heading sensor applied (127250) first, then the model (127258)
        COALESCE(o.n2k_variation_magnetic, o.n2k_variation_wmm_2020) AS bus_variation,
        o.n2k_leewayangle AS bus_leeway,
        o.xdr_raw_wind_s AS aws_raw,
        o.xdr_raw_wind_a AS awa_raw,
        o.xdr_raw_bsp AS stw_raw,
        (o.n2k_method_code)::integer AS gnss_method_code,
        (o.n2k_integrity_code)::integer AS gnss_integrity_code,
        (o.n2k_gnsstype_code)::integer AS gnss_type_code
    FROM public.fr_observations o
) r

-- Each calibration row as a half-open span [effective_from, next). Ordered
-- by cal_id within an instant, so two rows sharing an effective_from (two
-- device_ids, say) cannot duplicate a second: the earlier-entered one gets an
-- empty span and the newer one wins. device_id is provenance and does not
-- partition the lookup.
LEFT JOIN (
    SELECT cal_id, effective_from, param_value,
           lead(effective_from) OVER (ORDER BY effective_from, cal_id) AS effective_to
      FROM public.ra_calibrations
     WHERE param_name = 'man_bsp_adj'
) c
       ON r.ts >= c.effective_from
      AND (c.effective_to IS NULL OR r.ts < c.effective_to)

-- adj_stw once, so corrected_stw below cannot drift from it.
CROSS JOIN LATERAL (
    SELECT r.stw * COALESCE(c.param_value, 1.0) AS adj_stw
) a

-- corrected_stw once, so leeway can divide by it without restating the
-- model. |adj_stw|, plus a low-speed boost that dies away by ~6 kn, minus a
-- heel term (heel capped at 30 degrees, none when heel is missing).
--
-- The LEAST(..., 700) inside each exp() changes no value. Postgres raises
-- "value out of range" rather than returning 0 or Infinity, so without it
-- one garbage stw above ~355 kn in the ts window would fail the whole query;
-- by that argument every exp() term has already stopped contributing.
CROSS JOIN LATERAL (
    SELECT x.v
         + 1.9366 * (1 - exp(-LEAST(x.v / 0.5, 700))) / (1 + exp(LEAST(x.v / 1.5709, 700)))
         - 0.1849 * (LEAST(abs(COALESCE(r.heel, 0)), 30) / 20.0) ^ 2
                  * (1 - exp(-LEAST(x.v / 1.0, 700)))
           AS corrected_stw
      FROM (SELECT abs(a.adj_stw) AS v) x
) k

-- LEFT, both of them. Telemetry inside a session but outside any leg — the
-- pre-start, the gaps between legs, the sail home — is kept with leg_no NULL.
LEFT JOIN public.ra_sessions s
       ON r.ts >= s.t_start
      AND r.ts <  COALESCE(s.t_end, now())
LEFT JOIN public.ra_segments g
       ON g.session_id = s.session_id
      AND r.ts >= g.t_start
      AND r.ts <  g.t_end;           -- half-open: adjacent legs share an instant

COMMENT ON VIEW public.metrics_1s IS
  'fr_observations with instrument chains resolved, man_bsp_adj as of ts, and '
  'session/leg labels LEFT joined. Defined in sql/metrics_1s.sql. '
  'ALWAYS constrain ts: unfiltered, the planner cannot estimate the range joins.';

-- EVERY query against this view must constrain ts. Measured on the previous
-- labelled view with the same joins: no ts predicate scanned all 3.6M rows in
-- 8.6 s and ~4.8 GB of buffers for one afternoon; a literal ts window became
-- an index condition and returned the same rows in 0.3 s. Grafana always
-- passes $__timeFilter, so the exposure is ad-hoc queries.

-- A view runs as its owner, so ro_user reads this with no privilege on
-- fr_observations. Creating the role is not this file's business, so the
-- grant is skipped rather than failing when it does not exist.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ro_user') THEN
    EXECUTE 'GRANT USAGE ON SCHEMA public TO ro_user';
    EXECUTE 'GRANT SELECT ON public.metrics_1s TO ro_user';
  ELSE
    RAISE NOTICE 'role ro_user does not exist; skipping the grant on public.metrics_1s';
  END IF;
END $$;
