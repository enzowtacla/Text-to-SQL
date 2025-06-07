#PostgreSQL
import psycopg2

DB_HOST_PG = "localhost"
DB_NAME_PG = "University"
DB_USER_PG = "postgres"
DB_PASSWORD_PG = "Abcd1234!"

def connect_postgresql(host=DB_HOST_PG, database=DB_NAME_PG, user=DB_USER_PG, password=DB_PASSWORD_PG):

    try:
        db = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        return db
    except psycopg2.Error as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None

def get_postgresql_schema(db_connection, schema_name='public'):

    if not db_connection:
        return "Schema não disponível (falha na conexão PostgreSQL)."

    cursor = db_connection.cursor()
    # Tenta obter o nome do banco de dados da string de conexão DSN
    try:
        dsn_params = dict(param.split('=') for param in db_connection.dsn.split())
        current_db_name = dsn_params.get('dbname', DB_NAME_PG)
    except: # Em caso de qualquer erro ao parsear o DSN
        current_db_name = DB_NAME_PG

    schema_parts = [f"### Schema do Banco de Dados PostgreSQL: {current_db_name} (Schema: {schema_name})"]

    try:
        # Listar tabelas base no schema especificado
        cursor.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE';
        """, (schema_name,))
        tables = cursor.fetchall()

        if not tables:
            schema_parts.append(f"Nenhuma tabela encontrada no schema '{schema_name}'.")
            return "\n".join(schema_parts)

        for table_row in tables:
            table_name = table_row[0]
            schema_parts.append(f"\n-- Tabela: {schema_name}.{table_name}")
            
            # Coletar informações das colunas
            # Uma query mais completa para tentar montar algo similar a CREATE TABLE
            query_cols = f"""
            SELECT 
                a.attname AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                CASE WHEN a.attnotnull THEN 'NOT NULL' ELSE '' END AS null_constraint,
                (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid) 
                 FROM pg_catalog.pg_attrdef d 
                 WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) AS default_value,
                CASE 
                    WHEN pk.contype = 'p' THEN 'PRIMARY KEY'
                    ELSE ''
                END AS primary_key_constraint
            FROM 
                pg_catalog.pg_attribute a
            LEFT JOIN 
                pg_catalog.pg_constraint pk ON pk.conrelid = a.attrelid AND pk.contype = 'p' AND a.attnum = ANY(pk.conkey)
            WHERE 
                a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY 
                a.attnum;
            """
            qualified_table_name = f"{schema_name}.{table_name}"
            cursor.execute(query_cols, (qualified_table_name,)) # Passar nome qualificado da tabela
            columns_details = cursor.fetchall()

            schema_parts.append(f"CREATE TABLE {qualified_table_name} (")
            col_definitions = []
            for i, col_detail in enumerate(columns_details):
                col_name, data_type, null_constr, default_val, pk_constr = col_detail
                
                col_def_parts = [f"  \"{col_name}\"", data_type] # Aspas duplas para nomes de colunas
                if pk_constr: col_def_parts.append(pk_constr)
                if null_constr: col_def_parts.append(null_constr)
                if default_val: col_def_parts.append(f"DEFAULT {default_val}")
                
                line = " ".join(filter(None, col_def_parts)) # Remove partes vazias e junta
                if i < len(columns_details) - 1:
                    line += ","
                col_definitions.append(line)
            
            schema_parts.extend(col_definitions)
            schema_parts.append(");")
            schema_parts.append("") # Linha em branco para separar tabelas

    except psycopg2.Error as e:
        db_connection.rollback() # Importante para psycopg2 em caso de erro em transação
        return f"Erro ao buscar schema do PostgreSQL: {e}"
    finally:
        if cursor:
            cursor.close()
            
    return "\n".join(schema_parts)

# Se você quiser executar este arquivo diretamente para testar (opcional)
#if __name__ == "__main__":
    print("Executando PGConnection.py como script principal...")
    db_conn_pg = connect_postgresql()
    if db_conn_pg:
        print("Conexão PostgreSQL estabelecida para teste.")

        # Teste original de listagem de tabelas e colunas
        print(f"\n--- Informações do banco '{DB_NAME_PG}' (Execução direta) ---")
        main_cursor_pg = db_conn_pg.cursor()
        main_cursor_pg.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
        """)
        tables_direct_pg = main_cursor_pg.fetchall()
        for table_direct_pg_row in tables_direct_pg:
            table_name_direct_pg = table_direct_pg_row[0]
            print(f"\n--- Tabela: {table_name_direct_pg} ---")
            main_cursor_pg.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s;
            """, (table_name_direct_pg,))
            columns_direct_pg = main_cursor_pg.fetchall()
            print("Campos:")
            for col_direct_pg in columns_direct_pg:
                print(f"  - {col_direct_pg[0]} {col_direct_pg[1]} {'NULL' if col_direct_pg[2] == 'YES' else 'NOT NULL'}")
        main_cursor_pg.close()

        print("\n--- Schema Formatado para LLM (Execução direta) ---")
        schema_output_pg = get_postgresql_schema_as_string(db_conn_pg)
        print(schema_output_pg)

        db_conn_pg.close()
        print("\nConexão PostgreSQL fechada (execução direta).")
    else:
        print("Falha ao conectar ao PostgreSQL para teste.")