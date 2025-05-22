# dbt_sandbox

---

## Overview

This project uses dbt to generate fake company contract data and then analyze this data to produce an Annual Recurring Revenue (ARR) bridge. This bridge helps in understanding ARR movements like new customers, upsells, downsells, and churn.

## Key Components

*   **Data Generation (`dbt_sandbox/models/gsc_data_gen/`):** A set of dbt models that simulate customer contract events (new sales, renewals, add-ons) over time.
*   **ARR Analysis Models (`dbt_sandbox/models/gsc_data_gen/analysis/`):**
    *   `arr_monthly_snapshot.sql`: Creates monthly ARR snapshots per customer/product.
    *   `arr_bop_eop_calc.sql`: Calculates Beginning of Period (BOP), End of Period (EOP) ARR, and the change per customer/product.
    *   `arr_bridge_categorization.sql`: Aggregates changes at the customer level and categorizes them into 'New Customer', 'Lost Customer', 'Upsell', 'Downsell', or 'No Change'.
*   **Simulation Runner (`dbt_sandbox/run-dbt.py`):** A Python script that automates running the dbt models over a defined simulation period (e.g., daily for 6 months), passing dynamic parameters to dbt.
*   **Snowflake Integration:** The project is designed to run against a Snowflake data warehouse.

## Getting Started: Running the Simulation and Uploading to Snowflake

To run the full data simulation and have the results (including the ARR bridge analysis) created in your Snowflake instance, follow these steps:

**1. Configure Your dbt Profile:**

   dbt uses a `profiles.yml` file for Snowflake connection details. This file is typically located at `~/.dbt/profiles.yml` (or its location can be specified by the `DBT_PROFILES_DIR` environment variable).

   Your `profiles.yml` should contain a profile named `dbt_sandbox` (as specified in `dbt_sandbox/dbt_project.yml`). Configure it with your Snowflake account details, credentials, and target database/schema. Example structure:

   ```yaml
   dbt_sandbox:
     target: dev
     outputs:
       dev:
         type: snowflake
         account: "<your_snowflake_account>" # e.g., VBJUNST-WCB45803
         user: "{{ env_var('SNOWFLAKE_USER') }}"    # Your Snowflake username
         password: "{{ env_var('SNOWFLAKE_PASSWORD') }}" # Your Snowflake password
         role: "<your_snowflake_role>" # e.g., ACCOUNTADMIN (use a specific role if possible)
         warehouse: "<your_snowflake_warehouse>" # e.g., COMPUTE_WH
         database: "DIMS" # Target database
         schema: "PUBLIC"   # Target schema for tables/views
         threads: 1
         # client_session_keep_alive: False # Optional
   ```

   **Important:** For security, provide your Snowflake user and password via environment variables (e.g., `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`).

**2. Run the Simulation Script:**

   The `dbt_sandbox/run-dbt.py` script handles the day-by-day execution of dbt models.

   *   Ensure your configured dbt profile (from Step 1) is active.
   *   Set the required environment variables for your Snowflake credentials (e.g., `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`). The script also uses `SF_SANDBOX_SCHEMA` for an optional final copy step.
   *   Navigate to the `dbt_sandbox` directory in your terminal:
     ```bash
     cd dbt_sandbox
     ```
   *   Run the script:
     ```bash
     python run-dbt.py
     ```

**3. Output in Snowflake:**

   *   The `run-dbt.py` script will trigger `dbt run` for each day in its simulation period.
   *   This process creates or updates all relevant tables and views in your target Snowflake schema.
   *   The ARR analysis models (`arr_monthly_snapshot`, `arr_bop_eop_calc`, `arr_bridge_categorization`) will be created as **views** by default.
   *   You can then query these views in Snowflake (e.g., `SELECT * FROM DIMS.PUBLIC.arr_bridge_categorization;`) to see the ARR bridge data.

**4. Materializing Analysis Models as Tables (Optional):**

   If you prefer the ARR analysis models to be physical tables in Snowflake instead of views (which can be better for performance if queried frequently):
   1.  Edit `dbt_sandbox/dbt_project.yml`.
   2.  Under the `models: dbt_sandbox: gsc_data_gen:` section, add a configuration for the `analysis` path:
       ```yaml
       models:
         dbt_sandbox:
           # ... other model configurations ...
           gsc_data_gen:
             # ... existing active, calc, prep, append configs ...
             analysis: # New entry for analysis models
               +materialized: table
       ```
   3.  Save the file. The next time `dbt run` executes for these models, they will be created as tables.

## Other Scripts

*   **`generate_fake_customers.py`:** Generates the initial `dim_customers_v2.csv` file. This is typically run once or when you need to refresh the base customer list.
*   **`upload_snowflake.py`:** A utility to upload CSV files from `dbt_sandbox/seeds/dims/` directly to Snowflake. Note that `dbt seed` is the more integrated way to handle seed data within the dbt project.

---