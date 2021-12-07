Feature: ExampleFeature for snowflake testing

  Scenario: example_scenario
    Given a snowflake connection
    When a table called "SNOWFLAKE_LIQUIBASE.PUBLIC.DEPARTMENT" has
      | id: int64 | name: str | active:bool |
      | 1         | tilak     | 1           |
    Then a sql script "./sql/example.sql" runs and the result is
      | id: int64 | name: str | active:bool |
      | 1         | tilak     | 1           |