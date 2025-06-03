from datetime import date, timedelta
from dbt.cli.main import dbtRunner, dbtRunnerResult
import snowflake.connector
import os

sf_user = os.getenv('SNOWFLAKE_USERNAME')
sf_password = os.getenv('SNOWFLAKE_PASSWORD')
sf_schema = os.getenv('SF_SANDBOX_SCHEMA')

con = None
if sf_user and sf_password and sf_schema:
    con = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account='xwa97574',
        role='sandbox_owner',
        warehouse='compute_wh',
        database='sandbox',
        schema=sf_schema
    )
else:
    print("Snowflake credentials not found. Skipping database connection.")

if con:
    try:
        query_output = con.cursor().execute("select last_gen_date from last_gen")
        for last_gen_date in query_output:
            starting_date = last_gen_date[0] + timedelta(days=1)
    except Exception:
        starting_date = date(2021, 11, 2)
else:
    starting_date = date(2021, 11, 2)

ending_date = date.today() - timedelta(days = 1)

date_index = timedelta(days = 1)

print(starting_date)

print("Seeding Inputs")
dbt = dbtRunner()
cli_args = ["seed", "--profiles-dir", "./azdevops"]
res: dbtRunnerResult = dbt.invoke(cli_args)

print("Running Simulated Orders Through Today")
while starting_date <= ending_date:
    print(starting_date)
    dbt = dbtRunner()
    cli_args = ["run", "--profiles-dir", "./azdevops", "--vars", f"{{\"gen_date\": \"{starting_date}\"}}"]
    res: dbtRunnerResult = dbt.invoke(cli_args)
    starting_date += date_index
    try:
        for r in res.result:
            print(f"{r.node.name}: {r.status}")
    except:
        print("No output")

if con:
    print("Copying to individual schemas")
    try:
        con.cursor().execute(
            "create or replace table dbt_exercise_sl.gsc_orders as select * from orders"
        )
        print("SL copy success")
    except Exception:
        print("SL copy failed!")
