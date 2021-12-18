### Snowflake-pytest-poc

Create BDD style tests for snowflake.

Using [pytest-snowflake-bdd](https://pypi.org/project/pytest-snowflake-bdd/) package.

Refer `tests/features/example.feature` for a sample BDD test.

#### Setup the POC

```shell
pip install -r requirements.txt

```

#### Run tests

```shell
# Run all feature tests in tests/features directory
py.test -s tests/test_features.py  --snowflake_user tilakpr --snowflake_password '<password>' --snowflake_account='jn29444.southeast-asia.azure'
```


