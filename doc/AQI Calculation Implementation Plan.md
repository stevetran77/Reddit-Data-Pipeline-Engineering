# AQI Calculation Implementation Plan

## Objective
Add EPA-standard Air Quality Index (AQI) calculation to the OpenAQ Vietnam data pipeline to convert raw pollutant measurements (PM2.5, PM10, O3, NO2, SO2, CO) into actionable AQI values, categories, and health guidance.

## Background

**Current State:**
- Pipeline collects 6 pollutant parameters from OpenAQ API v3
- Glue job transforms raw JSON → Parquet with columns: pm25, pm10, o3, no2, so2, co
- Data flows: Lambda → S3 raw → Glue → S3 marts → Athena → Looker
- **No AQI calculation currently exists**

**Target State:**
- Add 3 new columns to output: `aqi` (numeric 0-500+), `aqi_level` (Good/Moderate/Unhealthy/etc.), `dominant_pollutant` (pm25/pm10/o3/etc.)
- Calculations based on US EPA AQI standard (internationally recognized)
- Integrate seamlessly into existing Glue PySpark transformation

## Implementation Approach

### Phase 1: Create AQI Calculator Module

**File:** `glue_jobs/aqi_calculator.py` (NEW)

**Contents:**
- EPA breakpoint tables for all 6 pollutants with concentration ranges mapped to AQI values
- Helper functions:
  - `find_breakpoint(concentration, pollutant)` - Locate EPA breakpoint range
  - `calculate_single_aqi(concentration, pollutant)` - Calculate AQI for one pollutant using EPA formula
  - `get_aqi_level(aqi_value)` - Map AQI to category (Good/Moderate/etc.)
- Main calculation functions (for PySpark UDF):
  - `calculate_aqi_row(pm25, pm10, o3, no2, so2, co)` - Overall AQI = MAX of individual pollutant AQIs
  - `calculate_dominant_pollutant(...)` - Identify which pollutant has highest AQI
  - `calculate_aqi_level_from_value(aqi)` - Category wrapper for UDF

**EPA AQI Formula:**
```
AQI = ((AQI_high - AQI_low) / (Conc_high - Conc_low)) * (C - Conc_low) + AQI_low
```

**Breakpoint Example (PM2.5 in µg/m³):**
| Conc Range | AQI Range | Level |
|------------|-----------|-------|
| 0.0 - 12.0 | 0 - 50 | Good |
| 12.1 - 35.4 | 51 - 100 | Moderate |
| 35.5 - 55.4 | 101 - 150 | Unhealthy for Sensitive Groups |
| 55.5 - 150.4 | 151 - 200 | Unhealthy |
| 150.5 - 250.4 | 201 - 300 | Very Unhealthy |
| 250.5+ | 301+ | Hazardous |

### Phase 2: Integrate into Glue Job

**File:** `glue_jobs/process_openaq_raw.py` (MODIFY)

**Location:** After line 205 (after enrichment step, before validation)

**Changes:**
1. Import AQI calculator functions
2. Register PySpark UDFs (DoubleType for aqi, StringType for aqi_level/dominant_pollutant)
3. Apply UDFs to create 3 new columns:
   ```python
   df_with_aqi = df_enriched.withColumn("aqi", aqi_udf(F.col("pm25"), F.col("pm10"), ...))
   df_with_aqi = df_with_aqi.withColumn("dominant_pollutant", dominant_pollutant_udf(...))
   df_with_aqi = df_with_aqi.withColumn("aqi_level", aqi_level_udf(F.col("aqi")))
   ```
4. Update validation to check AQI columns exist (optional columns, won't fail if missing)
5. Add logging for AQI calculation success/failure

**Edge Case Handling:**
- Missing pollutants: Calculate AQI from available pollutants only
- All nulls: Return null AQI, "Unknown" level
- Negative values: Treat as invalid, skip in calculation
- Extreme values (>500 AQI): Use highest breakpoint range, don't cap

### Phase 3: Unit Testing

**File:** `tests/test_aqi_calculator.py` (NEW)

**Test Coverage (~30 tests):**
- Breakpoint range tests for each pollutant (PM2.5: Good/Moderate/Unhealthy/etc.)
- Edge cases: null, negative, zero, extreme values
- Boundary conditions (e.g., exactly 12.0 µg/m³ PM2.5)
- Overall AQI calculation (max of all pollutants)
- Dominant pollutant detection
- AQI level categorization

**Run Command:**
```bash
python -m pytest tests/test_aqi_calculator.py -v
```

### Phase 4: Integration Testing

**File:** `tests/test_glue_complete.py` (MODIFY)

**Add Method:** `test_aqi_calculation_integration()` to `TestGlueTransformLogic` class

**Test Scenario:**
- Create mock data with realistic pollutant values
- Run full transformation pipeline (parse → pivot → enrich → AQI)
- Assert aqi, aqi_level, dominant_pollutant columns exist
- Verify values are reasonable (AQI 50-150 typical for Vietnam)
- Check AQI level matches expected category

## Critical Files

| File Path | Action | Lines |
|-----------|--------|-------|
| `glue_jobs/aqi_calculator.py` | CREATE | ~300 |
| `glue_jobs/process_openaq_raw.py` | MODIFY (after line 205) | +~40 |
| `tests/test_aqi_calculator.py` | CREATE | ~400 |
| `tests/test_glue_complete.py` | MODIFY (add test method) | +~50 |

## Deployment Steps

### Local Development (Day 1-2)
1. Create `aqi_calculator.py` with EPA breakpoint tables and calculation functions
2. Create `test_aqi_calculator.py` with comprehensive unit tests
3. Run unit tests: `pytest tests/test_aqi_calculator.py -v` (expect all pass)
4. Modify `process_openaq_raw.py` to integrate AQI calculation after enrichment
5. Add integration test to `test_glue_complete.py`
6. Run full test suite: `pytest tests/test_glue_complete.py -v` (expect all pass)

### AWS Glue Dev Deployment (Day 2-3)
1. Upload `aqi_calculator.py` to S3: `s3://openaq-data-pipeline/scripts/aqi_calculator.py`
2. Upload modified `process_openaq_raw.py` to Glue job script location
3. Configure Glue job parameter: `--extra-py-files s3://openaq-data-pipeline/scripts/aqi_calculator.py`
4. Run test job manually on 1 day of data
5. Verify in CloudWatch logs: "[OK] Calculated AQI for N records"
6. Check Athena schema: `DESCRIBE aq_dev.vietnam;` (expect aqi, aqi_level, dominant_pollutant columns)
7. Validate data quality:
   ```sql
   SELECT COUNT(*) as total, COUNT(aqi) as with_aqi, AVG(aqi) as avg_aqi,
          MIN(aqi) as min_aqi, MAX(aqi) as max_aqi
   FROM aq_dev.vietnam
   WHERE year = 2025 AND month = '12';
   ```

### Production Deployment (Day 3-4)
1. Deploy same changes to prod Glue job (`openaq_transform_measurements_prod`)
2. Run manual test on 1 day of prod data
3. Verify output in `aq_prod.vietnam` table
4. Monitor first Airflow DAG run with AQI calculation
5. Verify no downstream errors in Looker/OWOX

### Documentation (Day 4-5)
1. Update README/architecture docs with new AQI columns
2. Create Looker Studio visualizations:
   - AQI time series chart
   - AQI level pie chart
   - Dominant pollutant breakdown
   - AQI heatmap by location
3. Create Athena saved queries for AQI analysis

## Expected Output Schema

**New Columns:**
- `aqi` (double): EPA Air Quality Index value (0-500+)
- `aqi_level` (string): Category - "Good", "Moderate", "Unhealthy for Sensitive Groups", "Unhealthy", "Very Unhealthy", "Hazardous"
- `dominant_pollutant` (string): Pollutant with highest individual AQI - "pm25", "pm10", "o3", "no2", "so2", "co", or "unknown"

**Sample Output:**
| location_id | datetime | pm25 | pm10 | o3 | aqi | aqi_level | dominant_pollutant |
|-------------|----------|------|------|----|----|-----------|-------------------|
| 3276359 | 2025-12-20 10:00 | 35.5 | 80 | 0.065 | 100 | Moderate | pm25 |
| 2161290 | 2025-12-20 10:00 | 55.5 | 150 | 0.050 | 151 | Unhealthy | pm25 |

## Performance Impact

**Computational Cost:**
- AQI calculation: O(1) per row (simple arithmetic, no I/O)
- Expected overhead: <10% of total Glue job time
- For 1M records/day: ~2-5 minutes additional processing time

**Storage Impact:**
- 3 new columns add ~38 bytes per record
- Daily Parquet file size increase: ~10% (~20-30 MB)

**Memory Impact:**
- Negligible (~38 MB for 1M records)

## Rollback Plan

If AQI calculation causes issues:

**Immediate Rollback:**
1. AWS Console → Glue → Jobs → Edit script
2. Remove AQI calculation section (lines added after 205)
3. Save and run job
4. Verify job completes successfully

**Alternative:**
- Git revert to previous version of `process_openaq_raw.py`
- Re-upload to S3 and Glue

**Downstream Impact:**
- Athena: New columns simply won't appear (no errors)
- Looker: AQI visualizations show no data (graceful degradation)

## Risk Mitigation

**Risk: Missing pollutant columns after pivot**
- Mitigation: Add column existence check, use `F.lit(None)` for missing columns

**Risk: UDF import error in Glue**
- Mitigation: Verify `--extra-py-files` parameter, use `sys.path.append()` if needed

**Risk: Performance degradation**
- Mitigation: Monitor job duration, convert to Pandas UDF if needed, increase workers

**Risk: Null AQI for all records**
- Mitigation: Add debug logging to show available columns and sample values, verify column names match

**Risk: Glue Crawler doesn't detect new columns**
- Mitigation: Manually trigger crawler after job completes, verify crawler configuration

## Success Criteria

- [X] All 30+ unit tests pass
- [X] Integration test passes in full transformation flow
- [X] Glue job completes successfully with AQI calculation
- [X] New columns appear in Athena: aqi, aqi_level, dominant_pollutant
- [X] AQI values are reasonable (50-150 typical range for Vietnam)
- [X] AQI levels match expected categories
- [X] Dominant pollutant correctly identified
- [X] Job duration increases by <20%
- [X] No errors in downstream Looker/OWOX

## Timeline

**Total: 3-5 days**
- Day 1: Development (4 hours) - Create modules, tests
- Day 2: Dev deployment (2 hours) - Upload to AWS, test
- Day 3: Prod deployment (2 hours) - Deploy to prod, monitor
- Day 4-5: Documentation (2 hours) - Update docs, create visualizations

## Next Steps

1. Review and approve this plan
2. Execute Phase 1: Local Development
3. Execute Phase 2-3: AWS Deployment
4. Execute Phase 4: Documentation and monitoring
