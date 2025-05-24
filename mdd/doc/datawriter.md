
# 📘 AutoLoaderWriter Class – Detailed Summary

The `AutoLoaderWriter` is a high-level PySpark class designed for **structured streaming ingestion** into **Delta Lake tables**, with strong support for **schema enforcement**, **upserts**, **stream deduplication**, and **SQL-based data transformations**. It’s meant to streamline ingestion logic from Auto Loader sources (e.g., CSV/JSON/parquet files landing in cloud storage) using a configuration-first approach.

---

## 🧱 Primary Responsibilities

1. **Schema Projection via SQL Script**  
   Dynamically transforms incoming records using a templated SQL script that can extract metadata, reformat fields, or calculate new ones.

2. **Stream Deduplication via Metadata**  
   Automatically filters out already-processed files using `_source_name` and `_source_timestamp` fields, ensuring **idempotent ingestion**.

3. **Update vs Append Modes**  
   - `append`: Simple write of new data.
   - `update`: Uses **Delta merge** to upsert records based on a **primary key**, optionally skipping updates if no fields have changed.

4. **Dynamic Schema Alignment**  
   Casts and reorders columns to match the target table’s schema, excluding generated columns (e.g., `_record_id`).

5. **Trigger Configuration**  
   Supports `once`, `availableNow`, and `processingTime` triggers through config.

6. **Pre/Post Scripts Execution**  
   Executes optional SQL scripts before and after each batch—ideal for logging, audit, or cleanup.

7. **Validator Hook (Pluggable)**  
   Accepts a list of validators to inspect the data before writing.

---

## 🧰 Constructor Parameters

```python
AutoLoaderWriter(spark: SparkSession, df: DataFrame, config: dict, debug: bool = False)
```

| Parameter | Type | Description |
|----------|------|-------------|
| `spark` | `SparkSession` | Spark session for query execution. |
| `df` | `DataFrame` | The streaming source DataFrame from Auto Loader. |
| `config` | `dict` | Dictionary defining sink behavior (see below). |
| `debug` | `bool` | Enables detailed logging when `True`. |

---

## ⚙️ Config Structure

| Config Key | Required | Description |
|------------|----------|-------------|
| `sink_name` | ✅ | Table name (`catalog.db.table`). |
| `sink_projected_script` | ✅ | SQL with placeholder `vw_temp_source_data_uuid`. |
| `sink_write_mode` | ✅ | `"append"` or `"update"`. |
| `sink_primarykey` | ⚠️ Yes if `update` | Primary key for merge condition (comma-separated). |
| `sink_update_changes_only` | ✅ | Update only when values change (bool). |
| `sink_write_options` | ✅ | `.writeStream()` options, must include `checkpointLocation`. |
| `sink_write_trigger` | ✅ | One of `"processingTime"`, `"once"`, `"availableNow"`. |
| `sink_write_trigger_value` | ✅ if `"processingTime"` | Interval (e.g., `"10 seconds"`). |
| `sink_write_prescript` | ⛔ optional | SQL to run before each batch. |
| `sink_write_postscript` | ⛔ optional | SQL to run after each batch. |
| `sink_validators` | ⛔ optional | Custom functions to validate data. |

---

## ⚡ Method Overview

### `write_stream()`
Starts the streaming query by:
- Projecting the input DataFrame via SQL
- Adding metadata/system columns
- Filtering already-processed records
- Running pre/post batch SQL
- Writing via append or upsert logic

**Returns**: `StreamingQuery`  
**Raises**: Exception if stream fails

---

### `_projected(df, script) → DataFrame`
Replaces view placeholder in script, registers temp view, and runs SQL to project columns. Ensures system columns (`_source_name`, etc.) are preserved.

---

### `_match_columns(df, sink_name) → DataFrame`
Validates schema match (excluding generated cols) and casts columns to match types in target table.

---

### `_validate_source_columns_in_sink(df, sink_name)`
Ensures all source columns exist in target Delta table. Raises if unknown columns are found.

---

### `_upsert_to_delta(batch_df, batch_id)`
Performs a Delta merge:
- Uses primary key for matching
- Updates only changed fields if configured
- Inserts new rows excluding generated fields

---

### `_filter_already_processed_files(df) → (DataFrame, List[str])`
Joins input with the Delta table on `_source_name` and `_source_timestamp`. Filters out matching records and returns both the filtered DataFrame and a loggable list of skipped files.

---

### `_get_trigger_args() → dict`
Translates trigger config into the format accepted by `.trigger()`.

---

## 🔄 Flow Diagram

```
+------------+
|  Input DF  |
+------------+
      |
[ _projected() ]
      |
[ Add system columns (_source_name, _record_id, ...) ]
      |
[ _filter_already_processed_files() ]
      |
      +--------> Skip batch if files already processed
      |
[ Write Logic ]
  |    |
  |    +--> append --> write to Delta
  |
  +--> update
         |
    [ _validate_source_columns_in_sink() ]
    [ _upsert_to_delta() via merge ]
```

---

## 🧪 Sample Test Code

```python
from mdd.writer.autoloader_writer import AutoLoaderWriter

df = spark.readStream.format("cloudFiles")     .option("cloudFiles.format", "json")     .load("/mnt/source")

config = {
    "sink_name": "bronze.my_table",
    "sink_projected_script": """
        SELECT *, split(_metadata.file_name, '_')[0] AS file_date
        FROM vw_temp_source_data_uuid
    """,
    "sink_write_mode": "update",
    "sink_primarykey": "id",
    "sink_update_changes_only": True,
    "sink_write_options": {
        "checkpointLocation": "my_table_chkpt"
    },
    "sink_write_trigger": "processingTime",
    "sink_write_trigger_value": "30 seconds",
    "sink_write_prescript": "",
    "sink_write_postscript": "",
    "sink_validators": []
}

writer = AutoLoaderWriter(spark, df, config, debug=True)
query = writer.write_stream()
query.awaitTermination()
```

---

## 🛡️ Design Considerations

- **Idempotency**: Ensures records from the same file are not written twice.
- **Maintainability**: All logic driven by `config` dictionary—no hardcoded pipeline behavior.
- **Schema Safety**: Raises clear errors for schema mismatches or casting issues.
- **Traceability**: Rich debug logging and support for custom validation hooks.
