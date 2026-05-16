import great_expectations as gx
import duckdb
import json
from datetime import datetime
from pathlib import Path

# Read Bronze Parquet into pandas via DuckDB
conn = duckdb.connect()
df = conn.execute("SELECT * FROM read_parquet('data/bronze/**/*.parquet')").df()

print(f"[GX] Validating {len(df)} rows from Bronze layer...")

# Use the new GX v1.0 API
context = gx.get_context()

# Create a data source from the dataframe directly
data_source = context.data_sources.add_pandas("bronze_source")
data_asset = data_source.add_dataframe_asset("bronze_ticks")
batch_definition = data_asset.add_batch_definition_whole_dataframe("full_batch")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

# Create expectation suite
suite = context.suites.add(gx.ExpectationSuite(name="bronze_suite"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="symbol"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="close"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="volume"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="symbol", value_set=["AAPL","GOOGL","TSLA","MSFT","AMZN"]))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="close", min_value=0.01, max_value=100000))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="volume", min_value=1, max_value=10000000))

# Run validation
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(name="bronze_validation", data=batch_definition, suite=suite)
)
results = validation_definition.run(batch_parameters={"dataframe": df})

# Save report
Path("quality").mkdir(exist_ok=True)
report = {
    "run_time": datetime.utcnow().isoformat(),
    "total_rows": len(df),
    "success": results.success,
    "failed_expectations": [
        r.expectation_config.type
        for r in results.results
        if not r.success
    ]
}

report_path = f"quality/quality_report_{datetime.utcnow().strftime('%Y%m%d')}.json"
with open(report_path, "w") as f:
    json.dump(report, f, indent=2)

print(f"[GX] Validation {'PASSED ✓' if results.success else 'FAILED ✗'}")
print(f"[GX] Report saved to {report_path}")