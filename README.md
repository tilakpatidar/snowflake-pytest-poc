### Snowflake-pytest-poc

Create BDD style tests for snowflake.

Refer `tests/features/example.feature` for a sample BDD test.

Add bdd parsers in `tests/conftest.py`

#### How it works

- Using snowflake temporary tables, real table is shadowed with setup data for the entire session.
- If table does not exist, then a temporary table is created.
- Data is read into pandas dataframe and asserted against the expected pandas dataframe.

```gherkin
Feature: ExampleFeature for snowflake testing

  Scenario: example_scenario
    Given a snowflake connection
    When a table called "SNOWFLAKE_LIQUIBASE.PUBLIC.DEPARTMENT" has
      | dept_id: INTEGER | dept_name: STRING      |
      | 1                | "Computer Science"     |
      | 2                | "Software Engineering" |
    When a table called "SNOWFLAKE_LIQUIBASE.PUBLIC.PEOPLE" has
      | people_id: INTEGER | name: STRING | dept_id: INTEGER |
      | 10                 | "tilak"      | 1                |
    Then a sql script "./sql/example.sql" runs and the result is
      | people_id: INTEGER | name: STRING | dept_id: INTEGER | dept_name: STRING  |
      | 10                 | "tilak"      | 1                | "Computer Science" |
```

- `dept_id: INTEGER` In this header, `dept_id` is the column name and `INTEGER` is the snowflake data type.
- The step `a table called "<fully_qualified_table_name>" has`
  Replaces the existing table with a `temporary` table. And adds data to the temporary table. This shadows the existing
  table in snowflake for the entire session. Any changes done to the temporary table does not reflect on the actual
  database.
- The step `Then a sql script "<sql_script_path>" runs and the result is` This runs the sql script and compares the
  output with given dataframe.

#### Setup the POC

```shell
# If you have conda installed
conda environment -f environment.yml
# or
pip install -r requirements.txt

```

#### Run tests

```shell
# Run all feature tests in tests/features directory
SNOWFLAKE_USER=tilakpr SNOWFLAKE_PASSWORD='<password>' SNOWFLAKE_ACCOUNT='jn29444.southeast-asia.azure' py.test -s tests/test_features.py

# Run specific feature
SNOWFLAKE_USER=tilakpr SNOWFLAKE_PASSWORD='<password>' SNOWFLAKE_ACCOUNT='jn29444.southeast-asia.azure' py.test -s tests/test_features.py::test_example_scenario  # test_<scenario_name>

# Run tests in parallel 2 threads
SNOWFLAKE_USER=tilakpr SNOWFLAKE_PASSWORD='<password>' SNOWFLAKE_ACCOUNT='jn29444.southeast-asia.azure' py.test --dist=loadscope --tx '2*popen//python=python3.9' -s tests/test_features.py

```
