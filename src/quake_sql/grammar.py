from __future__ import annotations

from quake_sql.schema import TABLE_NAME


GRAMMAR = f"""
start: statement

statement: select_stmt
         | "UNSUPPORTED"

select_stmt: "SELECT"i select_list "FROM"i "{TABLE_NAME}" where_clause? group_by_clause? order_by_clause? limit_clause?

select_list: select_item ("," select_item)*
select_item: aggregate_expr alias?
           | bucket_expr alias?
           | selectable_column alias?

where_clause: "WHERE"i boolean_expr
group_by_clause: "GROUP"i "BY"i group_list
order_by_clause: "ORDER"i "BY"i order_list
limit_clause: "LIMIT"i LIMIT_INT

group_list: group_item ("," group_item)*
group_item: bucket_expr
          | groupable_column
          | time_column

order_list: order_item ("," order_item)*
order_item: order_expr sort_direction?
order_expr: aggregate_expr
          | bucket_expr
          | selectable_column
          | ALIAS

boolean_expr: boolean_term ((AND | OR) boolean_term)*
boolean_term: comparison
            | "(" boolean_expr ")"

comparison: numeric_expr NUMERIC_OP numeric_value
          | time_expr TIME_OP time_value
          | string_expr STRING_OP string_value
          | string_expr MATCH_OP STRING
          | nullable_column NULL_OP

numeric_expr: numeric_column
time_expr: time_column
         | bucket_expr
string_expr: string_column

aggregate_expr: COUNT_EXPR
              | aggregate_function "(" aggregate_arg ")"

aggregate_arg: "*"
             | selectable_column
             | bucket_expr

bucket_expr: TIME_BUCKET "(" time_column ")"

alias: "AS"i ALIAS
sort_direction: "ASC"i | "DESC"i

numeric_value: SIGNED_NUMBER
             | "NULL"i

string_value: STRING
            | "NULL"i

time_value: "now()"i
          | "today()"i
          | relative_time_expr
          | "toDateTime"i "(" DATETIME_STRING "," TZ_STRING ")"
          | DATE_STRING
          | DATETIME_STRING

relative_time_expr: "now()"i "-" "INTERVAL"i INT TIME_UNIT
                  | "today()"i "-" INT DAY_UNIT
                  | "toStartOfDay"i "(" "now()"i ")" "-" "INTERVAL"i INT DAY_UNIT
                  | "toStartOfHour"i "(" "now()"i ")" "-" "INTERVAL"i INT TIME_UNIT

selectable_column: time_column
                 | numeric_column
                 | string_column

nullable_column: "magnitude"i
               | "station_count"i
               | "azimuthal_gap"i
               | "distance_to_station_deg"i
               | "rms_residual"i
               | "horizontal_error_km"i
               | "depth_error_km"i
               | "magnitude_error"i
               | "magnitude_station_count"i

time_column: "event_time"i
           | "updated_at"i

numeric_column: "magnitude"i
              | "depth_km"i
              | "latitude"i
              | "longitude"i
              | "station_count"i
              | "azimuthal_gap"i
              | "distance_to_station_deg"i
              | "rms_residual"i
              | "horizontal_error_km"i
              | "depth_error_km"i
              | "magnitude_error"i
              | "magnitude_station_count"i

string_column: "event_id"i
             | "magnitude_type"i
             | "event_type"i
             | "status"i
             | "source_net"i
             | "place"i
             | "region"i
             | "location_source"i
             | "magnitude_source"i

groupable_column: "region"i
                | "event_type"i
                | "status"i
                | "source_net"i
                | "magnitude_type"i
                | "location_source"i
                | "magnitude_source"i

aggregate_function: "sum"i
                  | "avg"i
                  | "min"i
                  | "max"i
                  | "count"i

COUNT_EXPR: "count()"i | "count(*)"i
TIME_BUCKET: "toDate"i | "toStartOfHour"i | "toStartOfDay"i | "toStartOfWeek"i | "toStartOfMonth"i
NUMERIC_OP: "=" | "!=" | ">" | ">=" | "<" | "<="
TIME_OP: "=" | "!=" | ">" | ">=" | "<" | "<="
STRING_OP: "=" | "!="
MATCH_OP: "LIKE"i | "ILIKE"i
NULL_OP: "IS NULL"i | "IS NOT NULL"i
AND: "AND"i
OR: "OR"i
TIME_UNIT: "HOUR"i | "DAY"i | "WEEK"i | "MONTH"i
DAY_UNIT: "DAY"i
// LIMIT capped at 500 at the grammar level — values 1-500 only.
LIMIT_INT: /[1-9]/ | /[1-9][0-9]/ | /[1-4][0-9][0-9]/ | "500"
ALIAS: /[a-z_][a-z0-9_]*/
DATE_STRING: /'\\d{{4}}-\\d{{2}}-\\d{{2}}'/
DATETIME_STRING: /'\\d{{4}}-\\d{{2}}-\\d{{2}}(?:[ T]\\d{{2}}:\\d{{2}}:\\d{{2}}(?:\\.\\d{{1,3}})?(?:Z)?)'/
TZ_STRING: /'[^']+'/
STRING: /'[^']*'/

%import common.INT
%import common.SIGNED_NUMBER
%import common.WS
%ignore WS
""".strip()


SYSTEM_PROMPT = """
You translate natural language questions into ClickHouse SQL for a single table.
Return exactly one SQL statement that matches the custom grammar, or the literal
UNSUPPORTED when the request should be rejected.

Do not explain your reasoning.
Do not wrap the SQL in markdown.
Do not emit comments or extra text.
Prefer the simplest valid SQL that answers the request.
Never use OFFSET or ClickHouse LIMIT-with-offset syntax.
Do not add filters that the user did not ask for.
""".strip()
