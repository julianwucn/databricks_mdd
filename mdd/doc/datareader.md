
# 📘 AutoLoaderReader Class – Detailed Summary

The `AutoLoaderReader` is a utility class built on top of **PySpark Structured Streaming**, designed to configure and instantiate streaming DataFrames using **Databricks Auto Loader**. It supports **custom schema handling**, **dynamic path construction**, and **metadata column injection** for resilient and schema-evolving ingestion pipelines.

---

## 🧱 Primary Responsibilities

1. **Load Data from Auto Loader**  
   Uses `readStream.format("cloudFiles")` with schema location and format specified in config.

2. **Schema Management**  
   Automatically ensures that `_corrupt_record` and `_rescue_data` columns are appended to the schema if not already present.

3. **Metadata Injection**  
   Adds required system metadata fields (`_metadata`) to the output stream for downstream use.

4. **Dynamic Path Resolution**  
   Constructs `source_path` and `cloudFiles.schemaLocation` paths using environment settings.

---

## 🧰 Constructor Parameters

```python
AutoLoaderReader(spark: SparkSession, config: Dict, debug: bool = False)
```

| Parameter | Type | Description |
|----------|------|-------------|
| `spark` | `SparkSession` | Spark session for the streaming job. |
| `config` | `dict` | Dictionary of source options, paths, and schema. |
| `debug` | `bool` | Enables verbose logging if `True`. |

---

## ⚙️ Config Structure

| Key | Required | Description |
|-----|----------|-------------|
| `source_format` | ✅ | Format used by Auto Loader (e.g., `cloudFiles`, `json`, `csv`). |
| `source_options` | ✅ | Dictionary of Auto Loader options, including `cloudFiles.schemaLocation`. |
| `source_schema` | ✅ | User-defined schema string, optionally excluding rescue/corrupt columns. |
| `source_relative_path` | ✅ | Relative path to the data root. |
| `sink_name` | ✅ | Used to derive paths and schema checkpoints. |

---

## ⚡ Method Overview

### `read_stream() → DataFrame`
Initializes a streaming DataFrame with the following enhancements:

- Ensures the `_corrupt_record` column is present
- Ensures the `_rescue_data` column is present
- Adds `_metadata` to support Delta/Auto Loader semantics

**Returns**: A PySpark streaming `DataFrame`  
**Raises**: Exceptions if paths or schema config is missing or malformed

---

## 🧪 Sample Usage

```python
from mdd.reader.autoloader_reader import AutoLoaderReader

config = {
    "source_format": "cloudFiles",
    "source_options": {
        "cloudFiles.format": "json",
        "cloudFiles.schemaLocation": "my_schema"
    },
    "source_schema": "id INT, name STRING",
    "source_relative_path": "incoming/json",
    "sink_name": "bronze.my_table"
}

reader = AutoLoaderReader(spark, config, debug=True)
streaming_df = reader.read_stream()
```

---

## 🛡️ Design Considerations

- **Notebook Resilience**: Imports like `col`, `lit` are done inside `read_stream()` to prevent scope issues in notebooks.
- **Schema Flexibility**: Injects optional error-handling and rescue fields if not explicitly defined.
- **Environment-Aware**: Path construction and schema location use centralized `Environment` config.

---
