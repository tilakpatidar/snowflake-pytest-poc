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