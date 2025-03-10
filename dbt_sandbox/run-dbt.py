# run-dbt.py
import os
import random
from datetime import date, timedelta
from dbt.cli.main import dbtRunner

# -------------------------------
# Simulation Period Configuration
# -------------------------------
# Set the starting date for the simulation (e.g., February 7, 2025)
starting_date = date(2025, 2, 7)
# Define the simulation period for 6 months (approximately 182 days)
ending_date = starting_date + timedelta(days=182)

#print(f"Simulating orders from {starting_date} to {ending_date}")

# -------------------------------
# Generate Dynamic Parameters
# -------------------------------
dynamic_base_renewal_prob = round(random.uniform(0.8, 0.9), 3)   # Value between 0.8 and 0.9
dynamic_max_renewal_prob  = round(random.uniform(0.92, 0.96), 3)  # Value between 0.92 and 0.96

print(f"Dynamic probabilities: base_renewal_prob = {dynamic_base_renewal_prob}, max_renewal_prob = {dynamic_max_renewal_prob}")

# -------------------------------
# Loop to Execute dbt run for Each Day in the Simulation Period
# -------------------------------
current_date = starting_date
while current_date <= ending_date:
    print(f"\nProcessing date: {current_date}")
    dbt = dbtRunner()
    cli_args = [
        "run",
       # "--project-dir", "./dbt_sandbox/dbt_sandbox",  # Make sure this points to the folder containing dbt_project.yml
       # "--profiles-dir", "./dbt_sandbox/dbt_sandbox/azdevops",      # Path to profiles.yml
        "--vars", f'{{"gen_date": "{current_date}", "base_renewal_prob": {dynamic_base_renewal_prob}, "max_renewal_prob": {dynamic_max_renewal_prob} }}'
    ]
    res = dbt.invoke(cli_args)
    if res.result is None:
        print("No output for this date.")
    else:
        try:
            for r in res.result:
                print(f"Model: {r.node.name}, Status: {r.status}")
        except Exception as e:
            print("No output:", e)
    current_date += timedelta(days=1)

# -------------------------------
# Optional: Copy Data to an Individual Schema (e.g., for demos)
# -------------------------------
print("\nCopying orders to individual schemas")
# Verify that the necessary environment variables are set
username = os.getenv('SNOWFLAKE_USERNAME_2')
if not username:
    print("Error: SNOWFLAKE_USERNAME_2 is not set. Cannot copy orders.")
else:
    try:
        import snowflake.connector
        con = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USERNAME_2'),
            password=os.getenv('SNOWFLAKE_PASSWORD_2'),
            account='VBJUNST-WCB45803',
            role='ACCOUNTADMIN',
            warehouse='COMPUTE_WH',
            database='DIMS',
            schema=os.getenv('SF_SANDBOX_SCHEMA')
        )
        cur = con.cursor()
        cur.execute("CREATE OR REPLACE TABLE PUBLIC.gsc_orders AS SELECT * FROM orders")
        print("SL copy success")
    except Exception as e:
        print("SL copy failed!", e)
