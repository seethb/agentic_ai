# agentic_ai
Agentic_AI_Metadata
# To run this file
python3 ybmetadata_ai.py

# sample output
(arm_env) bseetharaman@bs-mbp-7xhdq ~ % python3 pgmetaopenai.py
Connected to PostgreSQL database successfully!
..........
"yugabyte_metadata.yugabyte_imported_event_count_by_table": {
    "columns": [
      {
        "column_name": "migration_uuid",
        "data_type": "uuid"
      },
      {
        "column_name": "table_name",
        "data_type": "character varying"
      },
      {
        "column_name": "channel_no",
        "data_type": "integer"
      },
      {
        "column_name": "total_events",
        "data_type": "bigint"
      },
      {
        "column_name": "num_inserts",
        "data_type": "bigint"
      },
      {
        "column_name": "num_deletes",
        "data_type": "bigint"
      },
      {
        "column_name": "num_updates",
        "data_type": "bigint"
      }
    ],
    "indexes": [
      {
        "indexname": "yugabyte_imported_event_count_by_table_pkey",
        "indexdef": "CREATE UNIQUE INDEX yugabyte_imported_event_count_by_table_pkey ON yugabyte_metadata.yugabyte_imported_event_count_by_table USING lsm (migration_uuid ASC, table_name ASC, channel_no ASC)"
      }
    ],
    "constraints": [
      {
        "conname": "yugabyte_imported_event_count_by_table_pkey",
        "contype": "p",
        "constraint_def": "PRIMARY KEY (migration_uuid, table_name, channel_no)"
      }
    ]
  }
}

Enter your natural language query or SQL query (or type 'cancel' to exit): identify the top 10 tables with high record count
 
Based on the above data, please identify the top 10 tables with the highest record counts and provide any insights or recommendations for performance tuning.
 
Response from OpenAI:
The top 10 tables with the highest record counts are as follows:
 
1. product.prov_subs_prod_det - 4,113,940 records
2. package.bundle_subs_details - 4,048,871 records
3. accounting.user_session_txn_store - 1,126,429 records
4. accounting.user_txn_store - 1,126,382 records
5. product.prod_subs_bucket_det - 1,033,640 records
6. package.bundle_proc_log - 1,021,242 records
7. package.bundle_subs_pkg_details - 1,012,219 records
 
 
Enter your natural language query or SQL query (or type 'cancel' to exit):


User Query: Show me the constraints for 'accounting.account' table

Based on the metadata, please provide a clear, concise answer.

Response from OpenAI:
{
  "table_name": "accounting.account",
  "constraints": {
    "primary_key": {
      "name": "account_pkey",
      "columns": ["account_id"]
    },
    "foreign_keys": [
      {
        "name": "account_account_type_id_fkey",
        "columns": ["account_type_id"],
        "foreign_table": "accounting.account_type_mast",
        "foreign_columns": ["account_type_id"]
      },
      {
        "name": "account_customer_id_fkey",
        "columns": ["customer_id"],
        "foreign_table": "accounting.customer_mast",
        "foreign_columns": ["customer_id"]

Enter your natural language query or SQL query (or type 'cancel' to exit):

User Query: How many tables are in the product schema?

Based on the metadata, please provide a clear, concise answer.

Response from OpenAI:
There are a total of 88 tables in the product schema.


Enter your natural language query or SQL query (or type 'cancel' to exit): Identify the top 10 slow queries based on average execution time.

Full Prompt to OpenAI:
Below is performance data from pg_stat_statements for our PostgreSQL database:

[
  [
    "SELECT t.table_schema, t.table_name, c.column_name, c.data_type\n            FROM information_schema.tables t\n            JOIN information_schema.columns c\n              ON t.table_schema = c.table_schema AND t.table_name = c.table_name\n            WHERE t.table_schema NOT IN ($1, $2)\n              AND t.table_type = $3\n            ORDER BY t.table_schema, t.table_name, c.ordinal_position",
    38,
    49817.3390263947,
    1893058.883003
  ],
  [
    "SELECT count(t.table_name)\n            FROM information_schema.tables t\n            JOIN information_schema.columns c\n              ON t.table_schema = c.table_schema AND t.table_name = c.table_name\n            WHERE t.table_schema NOT IN ($1, $2)\n              AND t.table_type = $3",
    1,
    45225.401369,
    45225.401369
  ],
  [
    "SELECT schemaname, tablename, indexname, indexdef\n            FROM pg_indexes\n            WHERE schemaname NOT IN ($1, $2)\n            ORDER BY tablename",
    8,
    1792.110421125,
    14336.883369
  ],
  [
    "CREATE EXTENSION IF NOT EXISTS citext",
    1,
    1299.341172,
    1299.341172
  ],
  [
    "SELECT tablename, indexname, indexdef\n            FROM pg_indexes\n            WHERE schemaname NOT IN ($1, $2)\n            ORDER BY tablename",
    10,
    1242.6365957,
    12426.365957
  ],
  [
    "SELECT n.nspname as schema, c.relname as table, conname, contype, pg_get_constraintdef(pg_constraint.oid) as definition\n            FROM pg_constraint\n            JOIN pg_class c ON c.oid = conrelid\n            JOIN pg_namespace n ON n.oid = c.relnamespace\n            WHERE n.nspname NOT IN ($1, $2)",
    1,
    949.93421,
    949.93421
  ],
  [
    "create database s1",
    1,
    933.467196,
    933.467196
  ],
  [
    "SELECT schemaname, tablename, indexname, indexdef\n            FROM pg_indexes\n            WHERE schemaname NOT IN ($1, $2)\n            ORDER BY schemaname, tablename",
    3,
    908.841132,
    2726.523396
  ],
  [
    "create database c23",
    1,
    877.857898,
    877.857898
  ],
  [
    "create database c2",
    1,
    867.183821,
    867.183821
  ]
]

Based on the above data, please identify the top 10 slow queries by average execution time and provide specific performance tuning recommendations (for example, suggestions for adding indexes, rewriting queries, or adjusting configuration settings).

Response from OpenAI:
1. Query 1:
   - Average execution time: 49817.3390263947
   - Recommendation: This query involves joining the information_schema.tables and information_schema.columns tables and filtering by table_schema and table_type. To improve performance, consider creating indexes on the columns used in the join and filter conditions. Additionally, reviewing the query plan and optimizing it for better performance could also be beneficial.

2. Query 2:
   - Average execution time: 45225.401369
   - Recommendation: This query also involves joining information_schema.tables and information_schema.columns tables. Similar to Query 1, consider creating indexes on the columns used in the join and filter conditions. Additionally, reviewing the query plan and optimizing it could

Enter your natural language query or SQL query (or type 'cancel' to exit):

