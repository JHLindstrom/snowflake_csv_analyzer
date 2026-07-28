#!/usr/bin/env python3
"""
Synthetic CSV Generator for Snowflake Session Data
Generates mock data matching: SESSION,EVENT_PATH,TOTAL_EVENTS with '->' path separators.
"""

import csv
import random
import sys
import uuid
from typing import List

DEFAULT_EVENTS = [
    "Home", "Search", "Product_View", "Add_To_Cart", 
    "Checkout", "Payment", "Order_Confirmation", 
    "Account_Settings", "Help_Center", "Category_Browse"
]

def generate_session_path(events_pool: List[str] = None, max_depth: int = 8) -> tuple[str, int]:
    if not events_pool:
        events_pool = DEFAULT_EVENTS
    
    # Generate realistic user funnel paths with higher dropoff probabilities
    num_events = random.choices(range(1, max_depth + 1), weights=[25, 20, 18, 15, 10, 6, 4, 2])[0]
    
    path = []
    current = random.choice(["Home", "Search", "Category_Browse"])
    path.append(current)
    
    for _ in range(num_events - 1):
        if current == "Home":
            current = random.choice(["Search", "Category_Browse", "Product_View"])
        elif current in ["Search", "Category_Browse"]:
            current = random.choice(["Product_View", "Home", "Search"])
        elif current == "Product_View":
            current = random.choice(["Add_To_Cart", "Category_Browse", "Product_View"])
        elif current == "Add_To_Cart":
            current = random.choice(["Checkout", "Product_View", "Home"])
        elif current == "Checkout":
            current = random.choice(["Payment", "Add_To_Cart"])
        elif current == "Payment":
            current = random.choice(["Order_Confirmation", "Checkout"])
        else:
            current = random.choice(events_pool)
        path.append(current)
        
    return "->".join(path), len(path)

def generate_csv(output_path: str, num_rows: int = 100000) -> None:
    print(f"Generating {num_rows:,} mock session rows to '{output_path}'...")
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["SESSION", "EVENT_PATH", "TOTAL_EVENTS"])
        
        for i in range(1, num_rows + 1):
            session_id = f"SESS_{uuid.uuid4().hex[:12].upper()}"
            event_path, total_events = generate_session_path()
            writer.writerow([session_id, event_path, total_events])
            
            if i % 25000 == 0 or i == num_rows:
                print(f" Generated {i:,}/{num_rows:,} rows...")

if __name__ == "__main__":
    out_file = sys.argv[1] if len(sys.argv) > 1 else "sample_snowflake_data.csv"
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 50000
    generate_csv(out_file, count)
