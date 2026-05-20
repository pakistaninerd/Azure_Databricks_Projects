# Azure_Databricks_Projects
Journey From AZ 900 to DP 100 - Azure Databricks personnel projets repository


## Project 1 — PySpark ETL + Delta Lake + Medallion Architecture

**Status:** Complete  
**Duration:** 2 sessions  
**Environment:** Azure Databricks Runtime 18.1 · Photon Engine · 4-core 16GB cluster · Pay-as-you-go subscription

---

### Files

| File | What is inside |
|---|---|
| `Project_01_ETL_&_Delta_Tables.ipynb` | First ever PySpark notebook — creating DataFrames, filtering, grouping, adding columns, sorting, saving Delta tables, appending rows, time travel, viewing history, fixing duplicates |
| `Project_01_Medallion_Pipelines.ipynb` | Complete Bronze to Silver to Gold pipeline — raw data ingestion, data cleaning, type conversion, business aggregations, pipeline validation report |
| `Project_01_Summary.html` | Visual summary of everything learned — concepts, AZ-900 exam scenarios, roadmap |
| `Project_01_CodeReference.html` | Every formula from both notebooks side by side with Teradata SQL equivalents |

---

### Skills Demonstrated

**Cloud Infrastructure**
- Set up a real Azure Databricks workspace on a Pay-As-You-Go subscription from scratch
- Configured a Spark cluster with auto-termination to control costs
- Understood Azure concepts — Resource Groups, Regions, PaaS, Quotas, Spot Instances

**PySpark Data Engineering**
- Created DataFrames from scratch and ran transformations across a distributed cluster
- Filtered rows, grouped data, added calculated columns, sorted results
- Removed duplicates using both exact match and column-specific deduplication
- Converted data types — string dates to proper date format
- Used aggregation functions — count, avg, max, round with column aliasing

**Delta Lake**
- Created and managed Delta tables with full ACID transaction support
- Appended new rows to existing tables without affecting existing data
- Used Time Travel to query any historical version of a table using version numbers
- Viewed the full automatic transaction history — who changed what and when
- Recovered from accidental duplicate writes using overwrite mode

**Medallion Architecture**
- Built a complete three-layer pipeline following industry-standard Medallion pattern
- Bronze layer — stored raw data exactly as received, duplicates and nulls included
- Silver layer — cleaned data, removed duplicates, handled nulls, fixed data types
- Gold layer — built business-ready aggregations for dashboards and reporting
- Validated pipeline output with row count checks across all three layers

---

### Lessons Learned

**Notebook 1 — Project_01_ETL_&_Delta_Tables**

- Learned that PySpark has two types of operations — Transformations like `filter()` and `groupBy()` that build a plan lazily, and Actions like `show()` and `count()` that actually trigger execution on the cluster. Nothing runs until an action is called.
- Discovered that `withColumn()` adds a new calculated column without touching the original DataFrame — like a non-destructive `CASE WHEN` in Teradata.
- Created my first Delta table and understood that every write automatically creates a new version tracked in the transaction log — zero configuration needed.
- Used Time Travel (`versionAsOf`) to go back to version 0 of a table before new rows were added — recovered data in one line of code, something impossible in Teradata without a full backup restore.
- Accidentally appended David twice by running the same cell twice — learned that `append` mode blindly adds rows without checking for existing duplicates, and fixed it using `dropDuplicates()` with `overwrite` mode.
- Saw my own email and cluster ID in the Delta history — realized Delta automatically records who made every change, providing a built-in audit trail for compliance.

**Notebook 2 — Project_01_Medallion_Pipelines**

- Built a Bronze layer that intentionally stored messy raw data — duplicate Bob, null country for Eve, null salary for Frank. Understood that Bronze is the source of truth and should never be modified. If Silver or Gold break, Bronze is always there to rebuild from.
- Built a Silver layer that cleaned the Bronze data — `dropDuplicates(["id"])` removed Bob's duplicate, `isNotNull()` filter removed Eve and Frank, `to_date()` converted string dates to proper date type. Seven rows became four clean rows.
- Built a Gold layer that aggregated Silver into business summaries — total employees, average salary and max salary per country. Four rows became two country-level summaries ready for a CEO dashboard.
- Understood why transformation and saving are always separate steps — if saving fails you know the transformation worked fine, if transformation fails you never accidentally save bad data to the table.
- Ran a full pipeline validation report at the end — reading all three tables fresh from Delta storage to confirm row counts and data correctness before declaring success.
- Learned that variables like `df_silver` disappear when a cluster restarts — always run all cells from top to bottom after restarting a cluster to rebuild everything in memory.

---

### Pipeline Architecture

```
Raw Data — 7 rows (duplicates + nulls included)
            |
            ▼
    BRONZE LAYER
    bronze_employees
    7 rows — everything stored as received
    No changes, no cleaning, no filtering
            |
            ▼
    SILVER LAYER
    silver_employees
    4 rows — cleaned and trusted
    ✓ Removed duplicate Bob (same id)
    ✓ Removed Eve (null country)
    ✓ Removed Frank (null salary)
    ✓ Converted hire_date string to date type
            |
            ▼
    GOLD LAYER
    gold_employee_summary
    2 rows — business ready
    ✓ UAE: 3 employees, avg salary, max salary
    ✓ KSA: 1 employee, avg salary, max salary
    Ready for dashboards and executive reports
```

---

### Key Concepts vs Teradata

| PySpark concept | Teradata equivalent | What it does |
|---|---|---|
| `df.filter()` | `WHERE` | Filter rows by condition |
| `df.groupBy().agg()` | `GROUP BY` with aggregates | Group and summarize |
| `df.withColumn()` | `CASE WHEN` derived column | Add calculated column |
| `df.dropDuplicates(["id"])` | `SELECT DISTINCT id` logic | Remove duplicate rows |
| `col("x").isNotNull()` | `x IS NOT NULL` | Check for null values |
| `to_date(col("x"), "format")` | `CAST(x AS DATE FORMAT ...)` | Convert string to date |
| `.mode("append")` | `INSERT INTO` | Add rows to existing table |
| `.mode("overwrite")` | `TRUNCATE + INSERT` | Replace all data in table |
| `.alias("name")` | `AS name` | Rename a column |
| `versionAsOf(0)` | No equivalent | Time travel to any version |
| `delta_table.history()` | No equivalent | Full automatic audit log |

---

### Azure Concepts Covered — AZ-900 Prep

| Concept | What I learned |
|---|---|
| **Resource Group** | Organizes all related Azure services together — delete one group to clean up everything |
| **PaaS** | Databricks is Platform as a Service — Azure manages all infrastructure automatically |
| **Regions** | Chose UAE North — physical location of servers affects latency and compliance |
| **Subscription Quotas** | New accounts have CPU core limits — hit this when creating first cluster |
| **Pay-as-you-go** | Charged per DBU per hour — only while cluster is running |
| **Auto-termination** | Cluster stops after 30 min idle — saves money automatically |
| **Spot Instances** | Uses spare Azure capacity at up to 90% discount — perfect for learning |
| **Virtual Networks** | Databricks workspace runs inside a private network — not exposed publicly |
| **Azure Entra ID** | Single login gives access to Databricks — identity managed by Microsoft |
| **Photon Engine** | Databricks C++ engine — up to 8x faster than standard Spark, auto-enabled |

---

## Full Roadmap

- [x] Project 1 — PySpark fundamentals + Delta Lake + Medallion Architecture
- [ ] Project 2 — Real CSV datasets from Azure Blob Storage + Azure Data Factory pipelines
- [ ] Project 3 — SQL Analytics + Databricks dashboards and visualizations
- [ ] AZ-900 — Azure Fundamentals exam
- [ ] DP-900 — Azure Data Fundamentals exam
- [ ] DP-100 — Azure Data Scientist Associate exam

---

## How I Learn

Every project follows the same structure so concepts stick:

1. **Build first** — hands-on on real Azure cloud before reading theory
2. **Compare to what I know** — every PySpark concept mapped to Teradata SQL I already understand
3. **Understand the why** — not just syntax but why each tool, layer and decision exists
4. **Document everything** — code reference, summary and lessons learned saved for revision

---
