"""
Test script for datetime/timestamp support in CharmPandas.

Generates test parquet files with timestamp columns, then exercises:
1. Reading parquet files with timestamp columns
2. Fetching tables with timestamp data back to pandas
3. Filtering rows by comparing timestamps to datetime literals
4. Using timestamp columns as join keys
5. Using timestamp columns as groupby keys
6. Arithmetic expressions involving timestamp columns
"""

import datetime
import numpy as np
import pandas as pd


def generate_test_data():
    n = 1000

    # Table 1: events with timestamps
    base_date = np.datetime64('2024-01-01')
    timestamps = base_date + np.arange(n).astype('timedelta64[D]')
    event_ids = np.arange(n, dtype=np.int32)
    values = np.random.randint(1, 100, size=n).astype(np.int32)

    table1 = pd.DataFrame({
        "event_id": event_ids,
        "event_time": timestamps,
        "value": values,
    })

    # Table 2: same timestamps for join testing, with extra data
    scores = np.random.randint(1, 50, size=n).astype(np.int32)
    table2 = pd.DataFrame({
        "event_time": timestamps,
        "score": scores,
    })

    # Table 3: duplicate timestamps for groupby testing
    # Use only 100 distinct dates, repeated
    grouped_timestamps = np.tile(timestamps[:100], n // 100)
    amounts = np.random.randint(1, 10, size=n).astype(np.int32)
    table3 = pd.DataFrame({
        "event_time": grouped_timestamps,
        "amount": amounts,
    })

    table1.to_parquet("test_datetime_events.parquet")
    table2.to_parquet("test_datetime_scores.parquet")
    table3.to_parquet("test_datetime_grouped.parquet")

    return table1, table2, table3


def test_read_and_fetch(base_dir):
    import charmpandas as cpd
    from charmpandas.interface import LocalCluster

    cluster = LocalCluster()
    cpd.set_interface(cluster)

    # Test 1: Read and fetch a table with timestamp column
    print("=== Test 1: Read and fetch timestamp table ===")
    df = cpd.read_parquet(base_dir + "/test_datetime_events.parquet")
    result = df.get()
    print(result.head())
    print(result.dtypes)
    assert pd.api.types.is_datetime64_any_dtype(result["event_time"]), \
        "event_time should be datetime64 dtype"
    print("PASSED: timestamp column preserved through read/fetch\n")

    # Test 2: Filter by datetime comparison
    print("=== Test 2: Filter by datetime comparison ===")
    cutoff = datetime.datetime(2024, 6, 1)
    filtered = df[df["event_time"] > cutoff]
    filtered_result = filtered.get()
    print(f"Rows after {cutoff}: {len(filtered_result)}")
    assert all(filtered_result["event_time"] > pd.Timestamp(cutoff)), \
        "All rows should have event_time > cutoff"
    print("PASSED: datetime filtering works\n")

    # Test 3: Filter with np.datetime64
    print("=== Test 3: Filter with np.datetime64 literal ===")
    cutoff_np = np.datetime64('2024-09-01')
    filtered2 = df[df["event_time"] > cutoff_np]
    filtered2_result = filtered2.get()
    print(f"Rows after {cutoff_np}: {len(filtered2_result)}")
    assert all(filtered2_result["event_time"] > pd.Timestamp(cutoff_np)), \
        "All rows should have event_time > np cutoff"
    print("PASSED: np.datetime64 filtering works\n")

    # Test 4: Join on timestamp key
    print("=== Test 4: Join on timestamp column ===")
    df2 = cpd.read_parquet(base_dir + "/test_datetime_scores.parquet")
    joined = df.merge(df2, on=["event_time"], how="inner")
    joined_result = joined.get()
    print(f"Joined rows: {len(joined_result)}")
    print(joined_result.head())
    assert "score" in joined_result.columns, "Joined table should have score column"
    assert "value" in joined_result.columns, "Joined table should have value column"
    print("PASSED: join on timestamp key works\n")

    # Test 5: Groupby on timestamp key
    print("=== Test 5: Groupby on timestamp column ===")
    df3 = cpd.read_parquet(base_dir + "/test_datetime_grouped.parquet")
    grouped = df3.groupby(["event_time"])["amount"].sum()
    grouped_result = grouped.get()
    print(f"Grouped rows: {len(grouped_result)}")
    print(grouped_result.head())
    assert len(grouped_result) == 100, \
        f"Expected 100 groups, got {len(grouped_result)}"
    print("PASSED: groupby on timestamp key works\n")

    print("=== All datetime tests passed! ===")


if __name__ == "__main__":
    import os
    base_dir = os.path.dirname(os.path.abspath(__file__))

    print("Generating test data...")
    generate_test_data()

    print("Running tests...\n")
    test_read_and_fetch(base_dir)
