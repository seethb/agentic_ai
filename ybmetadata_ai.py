#!/usr/bin/env python
import psycopg2
import json
import re
import openai

# Set your OpenAI API key (if you are using OpenAI for LLM responses)
openai.api_key = "xxxxxxxxxxx"


##############################
# Database Connection & Basic Metadata
##############################

def connect_to_postgres(db_config):
    """
    Connect to PostgreSQL using the provided configuration.
    """
    try:
        conn = psycopg2.connect(**db_config)
        print("Connected to PostgreSQL database successfully!")
        return conn
    except Exception as e:
        print("Error connecting to database:", e)
        raise

def get_metadata(conn):
    """
    Retrieve column information for all user tables.
    Returns a dictionary mapping fully qualified table names to a list of column details.
    """
    metadata = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT t.table_schema, t.table_name, c.column_name, c.data_type
            FROM information_schema.tables t
            JOIN information_schema.columns c
              ON t.table_schema = c.table_schema AND t.table_name = c.table_name
            WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema')
              AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_schema, t.table_name, c.ordinal_position;
        """)
        rows = cur.fetchall()
        for table_schema, table_name, column_name, data_type in rows:
            qualified_name = f"{table_schema}.{table_name}"
            metadata.setdefault(qualified_name, []).append({
                "column_name": column_name,
                "data_type": data_type
            })
    return metadata

def find_table_metadata(metadata, table_query_name):
    """
    Search for a table in the metadata dictionary.
    If table_query_name contains a dot, assume it's fully qualified and perform an exact (case-insensitive) match;
    otherwise, check if any qualified name ends with '.' + table_query_name.
    Returns a tuple: (qualified_name, details) if found; otherwise, (None, None).
    """
    if '.' in table_query_name:
        for qualified_name, details in metadata.items():
            if qualified_name.lower() == table_query_name.lower():
                return qualified_name, details
    else:
        for qualified_name, details in metadata.items():
            if qualified_name.lower().endswith("." + table_query_name.lower()):
                return qualified_name, details
    return None, None

##############################
# Additional Metadata: Indexes and Constraints
##############################

def get_pg_index_info(conn):
    """
    Retrieve index information for all user tables from pg_indexes.
    Returns a list of rows containing schemaname, tablename, indexname, and indexdef.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT schemaname, tablename, indexname, indexdef
            FROM pg_indexes
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY tablename;
        """)
        indexes = cur.fetchall()
    return indexes

def get_all_constraints(conn):
    """
    Retrieve constraint information for all user tables from pg_constraint.
    Returns a list of rows containing schema, table, constraint name, constraint type, and constraint definition.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT n.nspname AS schema, c.relname AS table, conname, contype,
                   pg_get_constraintdef(pg_constraint.oid) AS constraint_def
            FROM pg_constraint
            JOIN pg_class c ON c.oid = conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY c.relname;
        """)
        constraints = cur.fetchall()
    return constraints

def get_full_metadata(conn):
    """
    Merge column, index, and constraint metadata.
    Returns a dictionary mapping fully qualified table names to a dictionary with keys:
      'columns', 'indexes', and 'constraints'.
    """
    columns = get_metadata(conn)
    indexes = get_pg_index_info(conn)
    constraint_rows = get_all_constraints(conn)
    
    # Build a dictionary for constraints.
    constraints = {}
    for schema, table, conname, contype, constraint_def in constraint_rows:
        qualified_name = f"{schema}.{table}"
        constraints.setdefault(qualified_name, []).append({
            "conname": conname,
            "contype": contype,
            "constraint_def": constraint_def
        })
    
    full_metadata = {}
    for table, cols in columns.items():
        full_metadata[table] = {
            "columns": cols,
            "indexes": [ {"indexname": idx, "indexdef": idxdef}
                         for s, t, idx, idxdef in indexes if f"{s}.{t}".lower() == table.lower() ],
            "constraints": constraints.get(table, [])
        }
    return full_metadata

##############################
# Performance & Record Count Functions
##############################

def get_pg_stat_statements(conn):
    """
    Retrieve the top 10 slow queries based on average execution time from pg_stat_statements.
    Returns a list of rows.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT query, calls, mean_time, total_time
            FROM pg_stat_statements
            ORDER BY mean_time DESC
            LIMIT 10;
        """)
        slow_queries = cur.fetchall()
    return slow_queries

def get_frequent_slow_queries(conn):
    """
    Retrieve the 10 most frequently executed slow queries.
    For demonstration, we use a threshold on mean_time.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT query, calls, mean_time, total_time
            FROM pg_stat_statements
            WHERE mean_time > 100
            ORDER BY calls DESC
            LIMIT 10;
        """)
        freq_slow = cur.fetchall()
    return freq_slow

def get_pg_stat_activity(conn):
    """
    Retrieve active queries from pg_stat_activity.
    Returns a list of rows with duration formatted as a string.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pid, usename, query, state, to_char(now() - query_start, 'HH24:MI:SS') AS duration
            FROM pg_stat_activity
            WHERE state = 'active'
            ORDER BY query_start ASC
            LIMIT 10;
        """)
        activity = cur.fetchall()
    return activity

def get_table_counts(conn):
    """
    Retrieve approximate record counts for all user tables using pg_class.
    Returns a dictionary mapping fully qualified table names to an approximate row count.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT n.nspname AS schema, c.relname AS table, c.reltuples::bigint AS approximate_row_count
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY c.reltuples DESC;
        """)
        table_counts = cur.fetchall()
    counts = {f"{row[0]}.{row[1]}": row[2] for row in table_counts}
    return counts

##############################
# Query Execution & LLM Response Functions
##############################

def execute_sql_query(conn, query):
    """
    Execute an arbitrary SQL query on the PostgreSQL database.
    Returns the results.
    """
    with conn.cursor() as cur:
        cur.execute(query)
        try:
            results = cur.fetchall()
        except psycopg2.ProgrammingError:
            results = None
        return results

def generate_openai_response(prompt):
    """
    Use OpenAI's ChatCompletion API to generate a response for the given prompt.
    Returns the generated text.
    """
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=150,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Error during OpenAI API call: {str(e)}"

def filter_metadata(metadata, keyword):
    """
    Return a filtered metadata dictionary that only includes tables whose names contain the given keyword.
    """
    filtered = {table: data for table, data in metadata.items() if keyword.lower() in table.lower()}
    return filtered

def summarize_metadata(metadata):
    """
    Return a summary (the list of fully qualified table names) of the metadata.
    """
    return list(metadata.keys())

def process_natural_language_query(nl_query, full_metadata, conn):
    """
    Process the user's query. Depending on keywords, this function supports:
      - Direct SQL queries (if the query starts with "select")
      - Record count queries (if the query mentions "record count" or "high record count")
      - Schema-specific count queries (if the query mentions "in <schema> schema")
      - Performance/profiling queries (e.g., slow queries, active queries, index info)
      - A branch for index count queries in a specific schema (e.g., "list the table in pcc_tssgui schema which has more indexes")
      - A branch for tables with zero indexes in a specific schema (e.g., "list a table which has got zero indexes in java_abmf schema")
      - Schema requests for a specific table (if the query includes "schema for the 'table_name' table")
      - General natural language queries using metadata.
    
    Returns a tuple: (query_type, query_or_prompt)
    """
    nl_lower = nl_query.lower()

    # 1. SQL Query Check
    if re.match(r"^\s*select", nl_query, re.IGNORECASE):
        return "sql", nl_query

    # 2. Record Count Query Check
    if "record count" in nl_lower or "high record count" in nl_lower:
        counts = get_table_counts(conn)
        sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]
        counts_json = json.dumps(sorted_counts, indent=2)
        prompt = (
            "Below is the approximate record count for tables in our PostgreSQL database:\n\n"
            f"{counts_json}\n\n"
            "Based on the above data, please identify the top 10 tables with the highest record counts and provide any insights or recommendations for performance tuning."
        )
        return "llm", prompt

    # 3. Schema-Specific Count Query Check
    schema_count_match = re.search(r'in\s+["\']?(\w+)["\']?\s+schema', nl_lower)
    if schema_count_match:
        schema = schema_count_match.group(1)
        if schema.lower() in ["all", "entire"]:
            table_count = len(full_metadata)
            prompt = f"According to the metadata, there are {table_count} tables in the database."
            return "llm", prompt
        else:
            filtered_metadata = {table: data for table, data in full_metadata.items() if table.lower().startswith(schema.lower() + ".")}
            table_count = len(filtered_metadata)
            prompt = f"According to the metadata, there are {table_count} tables in the \"{schema}\" schema."
            return "llm", prompt

    # 4. Performance/Profiling Queries
    if "slow quer" in nl_lower:
        slow_queries = get_pg_stat_statements(conn)
        performance_data = json.dumps(slow_queries, indent=2)
        prompt = (
            "Below is performance data from pg_stat_statements for our PostgreSQL database:\n\n"
            f"{performance_data}\n\n"
            "Based on the above data, please identify the top 10 slow queries by average execution time and provide specific performance tuning recommendations (for example, suggestions for adding indexes, rewriting queries, or adjusting configuration settings)."
        )
        return "llm", prompt

    if "frequent" in nl_lower and "slow" in nl_lower:
        freq_slow = get_frequent_slow_queries(conn)
        performance_data = json.dumps(freq_slow, indent=2)
        prompt = (
            "Below is performance data from pg_stat_statements showing frequently executed slow queries:\n\n"
            f"{performance_data}\n\n"
            "Based on the above data, please identify the queries that are most frequently slow and suggest specific tuning recommendations."
        )
        return "llm", prompt

    if "active query" in nl_lower or "pg_stat_activity" in nl_lower:
        activity = get_pg_stat_activity(conn)
        performance_data = json.dumps(activity, indent=2)
        prompt = (
            "Below is a list of currently active queries from pg_stat_activity:\n\n"
            f"{performance_data}\n\n"
            "Based on the above data, please provide an analysis of the active queries and suggest any performance improvements."
        )
        return "llm", prompt

    # 4.5. New Branch: Index Count Query for a Specific Schema (e.g., "list the table in pcc_tssgui schema which has more indexes")
    if ("pcc_tssgui" in nl_lower) and ("index" in nl_lower) and (("more" in nl_lower) or ("highest" in nl_lower) or ("most" in nl_lower)):
        index_data = get_pg_index_info(conn)  # Returns rows: (schemaname, tablename, indexname, indexdef)
        filtered_indexes = [row for row in index_data if row[0].lower() == "pcc_tssgui"]
        index_counts = {}
        for schemaname, tablename, indexname, indexdef in filtered_indexes:
            qualified_table = f"{schemaname}.{tablename}"
            index_counts[qualified_table] = index_counts.get(qualified_table, 0) + 1
        sorted_index_counts = sorted(index_counts.items(), key=lambda x: x[1], reverse=True)
        if sorted_index_counts:
            top_table, top_count = sorted_index_counts[0]
            prompt = (
                f"Based on the index data for the pcc_tssgui schema, the table with the highest index count is:\n\n"
                f"{top_table} with {top_count} indexes.\n\n"
                "Please provide any recommendations for index optimization if applicable."
            )
        else:
            prompt = "No index data found for the pcc_tssgui schema."
        return "llm", prompt

    # 4.6. New Branch: Index Count Query for a Specific Schema with Zero Indexes (e.g., "list a table which has got zero indexes in java_abmf schema")
    if ("java_abmf" in nl_lower) and ("zero index" in nl_lower):
        tables_in_schema = {table: data for table, data in full_metadata.items() if table.lower().startswith("java_abmf.")}
        index_data = get_pg_index_info(conn)  # Returns (schemaname, tablename, indexname, indexdef)
        tables_with_indexes = {f"{row[0]}.{row[1]}".lower() for row in index_data if row[0].lower() == "java_abmf"}
        tables_zero_indexes = [table for table in tables_in_schema if table.lower() not in tables_with_indexes]
        if tables_zero_indexes:
            tables_json = json.dumps(tables_zero_indexes, indent=2)
            prompt = (
                "Below is the list of tables in the java_abmf schema that have zero indexes according to the metadata:\n\n"
                f"{tables_json}\n\n"
                "Based on this information, please provide any recommendations for indexing these tables if appropriate."
            )
        else:
            prompt = "All tables in the java_abmf schema have at least one index according to the metadata."
        return "llm", prompt

    # 5. General Index Query (if not covered above)
    if "index" in nl_lower or "indexes" in nl_lower:
        index_data = get_pg_index_info(conn)
        performance_data = json.dumps(index_data, indent=2)
        prompt = (
            "Below is the index information from pg_indexes for our PostgreSQL database:\n\n"
            f"{performance_data}\n\n"
            "Based on the above information, analyze the current index structure. Identify any indexes that are missing, redundant, or underutilized, and provide specific recommendations for index optimization."
        )
        return "llm", prompt

    # 6. General Natural Language Query Using Metadata
    keyword_match = re.search(r"tables\s+with\s+(\w+)", nl_lower)
    if keyword_match:
        keyword = keyword_match.group(1)
        filtered_metadata = filter_metadata(full_metadata, keyword)
        metadata_json = json.dumps(filtered_metadata, indent=2)
    else:
        summary = summarize_metadata(full_metadata)
        metadata_json = json.dumps(summary, indent=2)
    
    MAX_PROMPT_LENGTH = 4000
    if len(metadata_json) > MAX_PROMPT_LENGTH:
        metadata_json = metadata_json[:MAX_PROMPT_LENGTH] + "\n... (truncated)"
    
    prompt = (
        "You are an expert database analyst. Below is the full PostgreSQL metadata (including columns, indexes, and constraints) in JSON format:\n\n"
        f"{metadata_json}\n\n"
        f"User Query: {nl_query}\n\n"
        "Based on the metadata, please provide a clear, concise answer."
    )
    return "llm", prompt

##############################
# Main Function
##############################

def main():
    db_config = {
        "dbname": "ccp",
        "user": "yugabyte",
        "password": "yugabyte",
        "host": "10.98.41.67",
        "port": 5433
    }
    conn = connect_to_postgres(db_config)
    # Retrieve full metadata including columns, indexes, and constraints.
    full_metadata = get_full_metadata(conn)
    print("\nFull PostgreSQL Metadata collected:")
    print(json.dumps(full_metadata, indent=2))
    
    while True:
        try:
            nl_query = input("\nEnter your natural language query or SQL query (or type 'cancel' to exit): ")
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            break
        if nl_query.strip().lower() == "cancel":
            print("Operation cancelled.")
            break
        
        query_type, query_or_prompt = process_natural_language_query(nl_query, full_metadata, conn)
        if query_type == "sql":
            print("\nExecuting SQL query directly on the database...")
            results = execute_sql_query(conn, query_or_prompt)
            print("\nResults from the database:")
            print(json.dumps(results, indent=2))
        else:
            print("\nFull Prompt to OpenAI:")
            print(query_or_prompt)
            response = generate_openai_response(query_or_prompt)
            print("\nResponse from OpenAI:")
            print(response)
    
    conn.close()

if __name__ == "__main__":
    main()
