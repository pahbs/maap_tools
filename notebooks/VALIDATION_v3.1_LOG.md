# v3.1 height & AGBD validation: method log

**Scope.** Consolidate the field-plot validation of the v3.1 multi-year vegetation height (`Ht_H30_YYYY_v3.1_multiyr`) and AGBD (`AGB_H30_YYYY_v3.1_multiyr`) maps (2016–2025) into one notebook. Use the 2020 stand-age map to decide which plots are still valid references for the time series, and add v3.1 validation against airborne lidar (LVIS, G-LiHT).

**Status (2026-10-09).** Both notebooks are written and tested end-to-end on *synthetic* map values attached to the real local reference tables (see §7). **They have not yet been run on the real MAAP extractions, so this log contains no accuracy numbers.** Run them on MAAP to produce results.

| Deliverable | Purpose |
|---|---|
| `validation_v3.1_height_agbd.ipynb` (R) | Field-plot validation, told from simple to complex: first look (no screening) → why it is not enough (time gap, disturbance, growth, post-2020 change, each shown in the data before a rule is added) → screened results → where the error lives → time-series consistency → uncertainty coverage → sensitivity |
| `validation_v3.1_lidar_LVIS_GLiHT.ipynb` (Python, pangeo) | LVIS 2017/2019 same-year and change validation; G-LiHT 1 m CHM aggregated on the map grid |
| `VALIDATION_v3.1_LOG.md` | This log |

---

## 1. Inputs

| Source | File (MAAP) | Reference used |
|---|---|---|
| Canadian NFI (987 plots, 1992–2007) | `/projects/my-private-bucket/reference/nfi_plus_20220603_20260513.geojson` (GPKG despite the extension) | height: site height (`top`) or Lorey's (`legacy`); AGBD: `plotbio_lgtr_live + plotbio_smtr_live` |
| NASA Eurasia (646 plots, 2002–2016) | `.../eurasia_forest_structure_plots_agbd_20260513.gpkg` + heights joined by `site` from `/projects/my-public-bucket/databank/extract_from_points/eurasia_forest_structure_plots_smrytrees_20240124_s3_20241003_wGlobalCHM_s3_20250922.gpkg` | height: `Ht_max` (`top`) or `Ht_med` (`legacy`); AGBD: `agbd_v2_mg_ha` |
| Miesner/AWI (226 plots, 2011–2021) | `.../Miesner_plots_20260513.gpkg` | height: 98th-percentile tree height; 0 m where `Trees [#] == 0`; no AGBD |
| Kolyma (405 plots, 2010–2019) | `.../KolymaRegion_all_20260513.gpkg` | AGBD: `larch_biomass × 0.01` (assumed g/m² → Mg/ha); no height |

Map columns (from `extract_rasters_to_val_plots.ipynb`, verified from its saved band descriptions): `value_Ht_H30_{YYYY}_v3.1_multiyr_{mean,std}_ht`, `value_AGB_H30_{YYYY}_v3.1_multiyr_{mean,std}_agbd`, `value_CACC_2020_v3.1_multiyr_age_{mean,std}`, `value_AGB_v3.1_multiyr_2020-2025_{trendslope,kendall_pvalue,...}`. The notebook also accepts the older `value_ht_H30_YYYY_v3.1_multiyr` naming.

`site` is unique in the smrytrees file (checked: 646 unique, no NA), so the NASA height join is one-to-one.

## 2. Issues found in the earlier notebooks (fixed in the new one)

| # | Where | Issue | Effect | Fix |
|---|---|---|---|---|
| 1 | `lm_eqn`, `lm_eqn_boreal_ht` (height notebooks, `above_lidar_check`, `examine_height_boreal_lvis`) | "bias" label = OLS **intercept**; "RMSE" = OLS **residual** SE | Reported numbers describe the fitted line, not map error. An unbiased map with slope ≠ 1 shows a large "bias". Residual SE < RMSD whenever the slope ≠ 1. | Bias = mean(map − ref), RMSD vs 1:1. Slope and intercept kept only to describe attenuation. |
| 2 | `examine_boreal_agbd_validation` cell 10 | `total_plotbio` sums every `plotbio*` column | Not used in the saved figures, but would be wrong if adopted: `plotbio_lgtr_live` already equals stem wood + bark + branches + foliage (difference ≤ 0.02 Mg/ha on all 987 plots), and the sum adds dead trees, debris, stumps, shrubs, bryophytes. Median **2.3×** live-tree AGBD. | Live trees only: large + small. |
| 3 | `examine_boreal_agbd_validation` | `plots_sf` loaded as Eurasia (cell 5), then overwritten with NFI (cell 8). Cell 11 needs `agbd_v2_mg_ha`. Cell 14's statistics exclude West Siberian Plains, but its plot includes them. | The saved outputs depend on out-of-order execution. The labels don't match the plotted points. | Single linear pipeline; labels computed from the plotted data. |
| 4 | height notebooks | `'Bahkta River'` misspelling in the region `case_match` | Bakhta River plots were labelled "Permafrost Boreal" instead of "Boreal". | Spelling fixed. |
| 5 | height notebooks | Taseevo, Mana River, Sisim River, South of Lake Baikal, Amur River not assigned | Fell through to "Permafrost Boreal". | Assigned to "Boreal" (southern taiga). **Confirm.** |
| 6 | `examine_boreal_height_validation` `DO_FILT_MELT` | `MAX_DIFF_YR` filter acts on `diff_*` columns, but none existed at that point | No-op: 1666 plots at both 40 and 20 years (saved output). | Explicit time gap per plot × map year. |
| 7 | all | Pooled metrics mixed height definitions (Lorey's, median, p98) | Systematic between-source bias was folded into the pooled error. | Metrics reported per source. `top` vs `legacy` definitions as a sensitivity. |

## 3. Method (field plots)

### 3.1 Exclusions decided a priori (the `EXCLUDE` table in the notebook)
* NASA Changbai Mountains: outside the boreal domain (both variables).
* NASA Northernmost (AWI): same plots as Miesner. Height is taken from Miesner only; their AGBD is kept, since Miesner has none.
* NASA Amur River (AGBD): volume only, no tree biomass.
* NASA Russian Far East (AGBD): *Alnus* likely recorded as *Betula*; *Alnus* has no allometry, so plot AGBD is low.
* Temperate NFI ecozones (Atlantic Maritime, Pacific Maritime, Mixedwood Plains) are kept but reported separately. They are excluded from the "boreal domain" headline.

West Siberian Plains and Tunguska River are **not** removed by hand (earlier notes suspected change from Google Earth review). The age screen decides, and `T10` reports its verdict per group so it can be compared against the manual review.

### 3.2 Matching plots to map years
* **Matched year** = the map year closest to the field year: `clamp(t₀, 2016, 2025)`. All NFI plots and most NASA plots predate 2016, so they match 2016 with gaps of 9–24 yr (NFI) and 0–14 yr (NASA).
* All 10 plot × map-year pairs are used only for the gap analyses (F05, F10) and the per-year analysis (F19).

### 3.3 Stand-age screening (the core of the request)
With *t₀* = field year, *A* = mapped 2020 stand age (mean ± sd), years since measurement *Δ* = 2020 − *t₀*:

| Class | Rule | Used? |
|---|---|---|
| disturbed after obs | *A* < *Δ* (the stand is younger than the time since measurement, so it was replaced after the plot was measured). Same rule as `Review_maps_v3.1_trends_VHR_CHM.ipynb`. **Also** *A* = 0 at a plot that measured forest (height ≥ 2 m or AGBD ≥ 5 Mg/ha): the age product codes treeless pixels, including recent clearcuts, as 0. Reported separately as "stand removed after obs (age 0)". | no |
| marginal | *A* ≥ *Δ* but *A* − sd < *Δ* | yes by default; no under `AGE_RULE="conservative"` |
| no mapped age | age missing (NA/nodata), or age 0 at a non-forest plot | only if the plot is also non-forest (height < 2 m, AGBD < 5 Mg/ha). A forested plot with a missing age is not trusted. |
| undisturbed | otherwise | yes |

Two more screens:
* **Growth across the gap.** Age at measurement *A₀* = *A* − *Δ*. Stands with *A₀* ≥ `MATURE_AGE` (60 yr) are allowed any gap. Younger stands only within ±`MAX_GAP_YOUNG` (5 yr). The tier with this screen applied is called **strict**.
* **Disturbance after 2020**, which the 2020 age map cannot see. A pair is dropped from the first map year in 2021–2025 where the mapped series falls > 5 m and > 50 % (height) or > 20 Mg/ha and > 50 % (AGBD) below its 2016–2020 median.

Tiers: **all** (no screening, comparable to earlier notebooks) → **age-screened** → **strict**.

### 3.4 Metrics and uncertainty
* n, bias, relative bias, RMSD, relative RMSD, MAE, R², OLS slope/intercept.
* 95 % CIs from a **cluster bootstrap** over plot groups (source × group × field year; 1000 resamples). Groups with < 5 clusters get no CI.
* Uncertainty calibration: % of references within map mean ± 1.96 sd (T9).

### 3.5 Outputs (`/projects/my-private-bucket/validation_v3.1/<date>/`)
Numbered in reading order. Tables: T0 run config · T1 inventory · T2 first-look metrics (no screening) · T3 screening flow · T4 metrics by tier (pooled / source / region) · T5 per plot group · T6 error-vs-gap trend by maturity · T7 per map year on a fixed plot set · T8 NFI field age vs map age · T9 uncertainty coverage · T10 manual-review groups · T11 sensitivity. Also: per-plot screened GPKG (reusable as the Review-map layer) and matched pairs CSV.

Figures: F01 plot map by source · F02–F03 first look (height, AGBD) · F04 field years · F05 error vs gap, unscreened · F06 stand-age rule · F07 first look coloured by age class · F08 NFI field vs mapped age · F09 plot map by age class · F10 error vs gap by maturity · F11–F12 retained vs excluded · F13–F14 metrics by tier · F15–F16 error by reference value · F17–F18 by plot group · F19 metrics by map year · F20 trajectories by age class.

## 4. Trade-offs and nuances (read before quoting numbers)

1. **Age 0 is ambiguous in the age product.** It is the fill value for non-forest, and a recent stand-replacing disturbance (clearcut harvest, severe fire) also yields 0. Examples are evident in the West Siberian Plains plots. Age 0 is therefore read through the plot: at forested plots it counts as disturbance, and at non-forest plots as consistent non-forest. A plot is treated as forested if *either* its height or its AGBD reference passes the threshold.
1. **The age map is a model.** Its error propagates into the screen. T8/F08 (NFI cored `site_age` projected to 2020 vs the mapped age) is the plot-scale check. If agreement is poor, treat the screen as a *disturbance* detector (large age deficits) rather than a precise age. `AGE_RULE="conservative"` uses the age sd.
2. **Possible circularity.** The CACC product combines age with v3.1 AGB, and age inputs (TerraPulse disturbance history, GAMI) may share inputs with the structure maps. If age is partly informed by structure, screening on age could favour plots where the map is already consistent. **Confirm how `age_mean` is derived.** If it is structure-independent, this concern disappears.
3. **The post-2020 drop flag screens the product with itself.** It could remove genuine map errors (spurious drops), which would be optimistic. T3 counts the pairs it removes, and T11 reports results without it.
4. **`MATURE_AGE = 60` is a judgement call.** Boreal height growth slows markedly after ~50–80 yr, but this varies by site and species. T11 tests 40/60/80/100. F10 (bias vs gap, by maturity) is the empirical check: bias should rise with gap in young stands and stay flat in mature ones.
5. **Pre-2016 plots.** All NFI pairs have gaps ≥ 9 yr, so their strict-tier membership depends entirely on the maturity rule. Even mature stands keep growing slowly, so a small positive map-minus-field bias is expected from growth alone.
6. **Reference definitions differ by source.** The map targets ATL08 canopy-top height (≈ RH98). Lorey's and median heights sit below top height, so `legacy` mode gives positive bias for NFI/NASA by construction. `top` (default) is the closest match. NASA `Ht_max` is a single tallest tree in a 314–707 m² plot and can exceed a 30 m RH98. Both modes are reported (T11).
7. **Footprint mismatch.** Plots of 314–707 m² are compared with 900 m² pixels by point extraction, with no geolocation buffer. That adds error unrelated to map quality, especially in heterogeneous or sparse stands.
8. **NFI coordinates.** Verify the `nfi_plus` locations are true plot coordinates and not the publicly perturbed ones. If perturbed, pixel-level comparison is not meaningful.
9. **Kolyma AGBD unit.** `larch_biomass` is assumed to be g/m² (median ≈ 600 → 6 Mg/ha). It is larch-only, so other species and shrubs are excluded. Confirm against the Alexander et al. / FLARE metadata before using Kolyma in headline numbers.
10. **NFI small trees.** `plotbio_smtr_live` is missing on 78 plots, which are treated as large trees only and labelled in `ref_agbd_def`.
11. **Sample imbalance.** Boreal pooled metrics are dominated by NFI (Boreal Shield alone is 376 plots). Read per-source and per-group results (T4, T5, F08/F12) alongside the pooled numbers.
12. **Non-forest plots.** Treeless Miesner plots (64 with 0 trees) anchor the low end. They are kept even without a mapped age because the reference is also non-forest. This assumes treeline and shrub change between field year and map year is small.

## 5. Lidar notebook

| Part | Design | Why it adds value |
|---|---|---|
| LVIS same-year | ORNL `ABoVE_LVIS_VegetationStructure` L3 `RH098_mean_30m` (2017, 2019). Up to 150 random pixels per tile on a 90 m lattice. v3.1 sampled for the same year via the footprint index (nearest pixel, or 3×3 mean with `MAP_WINDOW=3`). Stratified by LVIS height × canopy cover. | No temporal gap, no age screen needed. Same height definition (RH98). |
| LVIS change 2017→2019 | 2017 samples inside 2019 tiles are re-read from 2019. ΔLVIS vs Δmap; loss (< −5 m) recall and precision; reference noise on stable pixels. | The only *change* validation available for the time series. |
| Time-series noise | Inter-annual sd of the map at LVIS pixels with age > 10 yr. | Map noise floor, to compare against same-year RMSD. |
| G-LiHT | `earthaccess` search (2016–2025, ABoVE box). Each 1 m CHM is warped onto the map's 30 m grid (30 × 30 cells per pixel). p98 and cover ≥ 1.37 m per pixel, keeping pixels with ≥ 80 % valid cells. | Removes grid misregistration. Independent sensor. |

Not in the repo before: v3.1 lidar validation of any kind. `examine_height_boreal_lvis` used an older 2020 gridded ATL08 height, and `above_lidar_check` (LVIS vs G-LiHT) used local Windows files.

**Assumptions to verify on MAAP:**
* G-LiHT short name `GLCHMT` and the CHM file pattern (a discovery cell prints the candidates).
* LVIS cover file suffix `CC_gte_01p37m_30m` and its scale (the notebook divides by 100 if max > 100).
* ORNL credentials through `maap.aws.earthdata_s3_credentials` or `requests` + netrc.
* CACC age bands 9/10, which match the band list printed by the extraction notebook.

**Caveats:**
* LVIS point sampling carries up to ~21 m grid offset between the LVIS and map grids. Use `MAP_WINDOW=3` as a check: if RMSD drops a lot, misregistration matters.
* Lidar pixels are clustered along flight lines, so CIs use a bootstrap over tiles or granules.
* LVIS covers only the ABoVE domain (Alaska / NW Canada), so it cannot validate Eurasia.

## 6. Reproduce

1. On MAAP, confirm the four `*_20260513` files and the smrytrees height file exist (paths in §1, or set `VAL_REF_DIR`, `VAL_DATA_STAMP`, `VAL_EURASIA_HT_FN`).
2. Run `validation_v3.1_height_agbd.ipynb` top to bottom (R kernel; packages: sf, dplyr ≥ 1.1, tidyr, ggplot2 ≥ 3.5, **patchwork ≥ 1.2**, rnaturalearth optional). Outputs go to `VAL_OUT_DIR`.
3. Run `validation_v3.1_lidar_LVIS_GLiHT.ipynb` in the pangeo env with Earthdata login. Set `RUN_LVIS` / `RUN_GLIHT` as needed.
4. `T0_run_config.csv` records every threshold used.

## 7. What was tested, and how

* **R notebook.** Executed end-to-end through Jupyter (IR kernel, R 4.1.2, dplyr 1.1.4, ggplot2 3.5.1, sf 1.0.5, patchwork 1.2.0) on test files built from the *real* local reference tables (NFI CSV, Miesner, NASA smrytrees, Kolyma under `/explore/nobackup/people/pmontesa/userfs02/data/reference/extracted/`). The map, age and AGBD values were **simulated**, with planted pre- and post-2020 disturbances. This exercised every code path and figure. The screening caught the planted disturbances. The numbers carry no meaning.
* **Lidar notebook.** Core functions were tested on synthetic rasters in different CRSs (UTM CHM/LVIS vs Alaska Albers map):
  * G-LiHT grid aggregation recovers the planted per-pixel truth (r = 1.000);
  * LVIS sampling, footprint-index extraction (1×1 and 3×3), same-year matching and the cluster bootstrap all run correctly.
  * **Not tested:** S3/earthaccess access, the 2017↔2019 change section, and the real file formats.
* patchwork < 1.2 fails with ggplot2 3.5 when collecting legends. Make sure the MAAP R environment has ≥ 1.2.

## 8. Decisions for you

1. Region assignment of Taseevo, Mana, Sisim, South of Lake Baikal, Amur → "Boreal" (§2 #5).
2. Headline height definition: `top` (default) or `legacy` for continuity with earlier figures.
3. `MATURE_AGE` and `MAX_GAP_YOUNG` defaults, after looking at F10 and T11.
4. Whether Kolyma enters the headline AGBD numbers before its unit is confirmed.
5. Whether the post-2020 drop flag stays on in the headline tier (default on; T11 shows its effect).
