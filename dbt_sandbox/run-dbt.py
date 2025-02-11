from datetime import date, timedelta
from dbt.cli.main import dbtRunner, dbtRunnerResult
import snowflake.connector
import os
import subprocess

con = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USERNAME'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account='xwa97574',
    role='sandbox_owner',
    warehouse='compute_wh',
    database='sandbox',
    schema=os.getenv('SF_SANDBOX_SCHEMA')
)

try:
    query_output = con.cursor().execute("select last_gen_date from last_gen")
    for last_gen_date in query_output:
        starting_date = last_gen_date[0] + timedelta(days = 1)
except:
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

print("Copying to individual schemas")
try:
    query_output = con.cursor().execute("create or replace table dbt_exercise_sl.gsc_orders as select * from orders")
    print("SL copy success")
except:
    print("SL copy failed!")