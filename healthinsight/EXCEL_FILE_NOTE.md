# About GL 2023 and 2024.xlsx

## Location
The file `GL 2023 and 2024.xlsx` remains in the **root DLT directory** (not in `healthinsight/` folder).

## Why it's not in healthinsight/
1. **Historical Data**: Contains FIN_GL data for 2023 and 2024, which are **past years** that cannot change.
2. **Already in Database**: The data from this file has already been loaded into DuckDB tables:
   - `FIN_GL_2023_RAW`
   - `FIN_GL_2024_RAW`
   - All derived tables from these (via `auto_update_database.py`)

3. **Used by Update Script**: The `auto_update_database.py` script (in root) uses this file to refresh FIN_GL tables for 2023 and 2024, but since the data is historical and doesn't change, it's only needed if you want to rebuild those tables from scratch.

## When is it needed?
- Only if you need to **rebuild** the 2023/2024 FIN_GL tables from scratch
- The `auto_update_database.py` script looks for it in the root directory when updating FIN_GL_2023_RAW and FIN_GL_2024_RAW tables

## Current Status
✅ **Safe to leave in root** - It's not part of the AI analysis tools, it's a data source file used by the database update process.

## Note
For 2025 data, the update script uses `dlt_sources.py` to fetch directly from EACCOUNT database, so no Excel file is needed for current year data.

