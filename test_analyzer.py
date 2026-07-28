#!/usr/bin/env python3
"""
End-to-End Automated Verification Test Suite for Snowflake CSV Analyzer.
"""

import os
import sys
import subprocess

def run_command(cmd: str) -> None:
    print(f"\n[TEST EXEC] {cmd}")
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(res.stdout)
    if res.returncode != 0:
        print(f"[ERROR] Command failed with return code {res.returncode}")
        print(res.stderr)
        sys.exit(res.returncode)

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(base_dir)
    
    csv_file = "test_synthetic_snowflake.csv"
    parquet_file = "test_synthetic_snowflake.parquet"

    print("==================================================")
    print("STEP 1: Generating Synthetic Benchmark CSV (25,000 rows)...")
    print("==================================================")
    run_command(f"python3 cli.py generate-mock -o {csv_file} -r 25000")
    assert os.path.exists(csv_file), "CSV file generation failed!"

    print("==================================================")
    print("STEP 2: Inspecting CSV Schema and Sample Rows...")
    print("==================================================")
    run_command(f"python3 cli.py inspect {csv_file} --limit 3")

    print("==================================================")
    print("STEP 3: Converting CSV to Compressed Parquet Format...")
    print("==================================================")
    run_command(f"python3 cli.py convert {csv_file} {parquet_file}")
    assert os.path.exists(parquet_file), "Parquet conversion failed!"

    print("==================================================")
    print("STEP 4: Inspecting Parquet Metadata...")
    print("==================================================")
    run_command(f"python3 cli.py inspect {parquet_file} --limit 3")

    print("==================================================")
    print("STEP 5: Running Event Path Analytics & Funnel Analysis...")
    print("==================================================")
    run_command(f"python3 cli.py analyze {parquet_file} --funnel 'Home,Product_View,Add_To_Cart,Checkout,Payment,Order_Confirmation'")

    print("==================================================")
    print("STEP 6: Searching Sessions Matching Event Filters...")
    print("==================================================")
    run_command(f"python3 cli.py search {parquet_file} --subpath 'Add_To_Cart->Checkout' --limit 5")

    print("==================================================")
    print("STEP 7: Executing Custom DuckDB SQL Query...")
    print("==================================================")
    run_command(f"python3 cli.py query {parquet_file} --sql \"SELECT TOTAL_EVENTS, COUNT(*) as sessions FROM data GROUP BY 1 ORDER BY 1 LIMIT 5\"")

    print("\n==================================================")
    print("✅ ALL VERIFICATION TESTS PASSED SUCCESSFULLY!")
    print("==================================================")

if __name__ == "__main__":
    main()
