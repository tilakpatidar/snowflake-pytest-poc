### Snowflake-pytest-poc

Create BDD style tests for snowflake.

Refer `tests/features/example.feature` for a sample BDD test.

Add bdd parsers in `tests/conftest.py`

#### How it works
- Using snowflake temporary tables, real table is shadowed with setup data for the entire session.
- Tables in below test are pandas dataframe, data is read into pandas dataframe and asserted against the expected pandas dataframe.
```gherkin
Feature: ExampleFeature for snowflake testing

  Scenario: example_scenario
    Given a snowflake connection
    When a table called "SNOWFLAKE_LIQUIBASE.PUBLIC.DEPARTMENT" has
      | id: int64 | name: str | active:bool |
      | 1         | tilak     | 1           |
    Then a sql script "./sql/example.sql" runs and the result is
      | id: int64 | name: str | active:bool |
      | 1         | tilak     | 1           |
```
- `id: int64` In this header, `id` is the column name and `int64` is the pandas data type.
- The step `a table called "<fully_qualified_table_name>" has`
 Replaces the existing table with a `temporary` table. And adds data to the temporary table.
This shadows the existing table in snowflake for the entire session. Any changes done to the temporary table does not reflect on the actual database.
- The step `Then a sql script "<sql_script_path>" runs and the result is` This runs the sql script and compares the output with given dataframe.

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
