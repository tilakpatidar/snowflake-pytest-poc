select p.people_id,
       p.name,
       d.dept_id,
       d.dept_name
from "SNOWFLAKE_LIQUIBASE"."PUBLIC"."PEOPLE" p
         INNER JOIN "SNOWFLAKE_LIQUIBASE"."PUBLIC"."DEPARTMENT" d
                    ON p.dept_id = d.dept_id