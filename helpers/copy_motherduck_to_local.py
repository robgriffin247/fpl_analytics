import duckdb
import os

def copy_motherduck_to_local():
    local_file = "data/fpl_analytics.duckdb"
    motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
    motherduck_database = "fpl_analytics"

    # Connect to local file
    con = duckdb.connect(local_file)
    
    # Install and load the MotherDuck extension
    con.execute("INSTALL motherduck")
    con.execute("LOAD motherduck")
    
    # Set the MotherDuck token
    con.execute(f"SET motherduck_token='{motherduck_token}'")
    
    # Attach MD database
    con.execute(f"ATTACH 'md:{motherduck_database}' AS motherduck_db")
    
    # List all tables using SHOW
    result = con.execute("SHOW ALL TABLES").fetchall()
    
    # Group by schema
    tables_by_schema = {}
    for row in result:
        # SHOW ALL TABLES returns: database, schema, name, column_names, column_types, temporary
        database, schema_name, table_name = row[0], row[1], row[2]
        
        if database == 'motherduck_db' and schema_name not in ('information_schema', 'pg_catalog'):
            if schema_name not in tables_by_schema:
                tables_by_schema[schema_name] = []
            tables_by_schema[schema_name].append(table_name)
    
    # Copy tables
    for schema_name, tables in tables_by_schema.items():
        print(f"\nProcessing schema: {schema_name}")
        # Create schema in local database
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        
        for table_name in tables:
            print(f"  Copying {schema_name}.{table_name}...", end=" ")
            
            con.execute(f"""
                CREATE OR REPLACE TABLE {schema_name}.{table_name} AS 
                SELECT * FROM motherduck_db.{schema_name}.{table_name}
            """)
            
            count = con.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}").fetchone()[0]
            print(f"{count:,} rows")
            
    con.close()
    print(f"\n✓ Copy complete!")


if __name__ == "__main__":
    copy_motherduck_to_local()