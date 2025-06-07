#MySQL
import MySQLdb

DB_HOST_MYSQL = "localhost"
DB_USER_MYSQL = "root"
DB_PASSWORD_MYSQL = "Abcd1234!"
DB_NAME_MYSQL = "University"

def connect_mysql(host=DB_HOST_MYSQL, user=DB_USER_MYSQL, password=DB_PASSWORD_MYSQL, db_name=DB_NAME_MYSQL):
    try:
        db = MySQLdb.connect(
            host=host,
            user=user,
            password=password,
            db=db_name
        )
        return db
    except MySQLdb.Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return None

def get_mysql_schema(db_connection):

    if not db_connection:
        return "Schema não disponível (falha na conexão MySQL)."

    cursor = db_connection.cursor()
    # Tenta obter o nome do banco de dados da conexão, se possível.
    # Para MySQLdb, db_connection.db retorna bytes, então decodificamos.
    try:
        current_db_name = db_connection.db.decode()
    except AttributeError: # Caso o atributo 'db' não esteja disponível ou não seja bytes
        current_db_name = DB_NAME_MYSQL # Fallback para o nome usado na conexão

    schema_parts = [f"### Schema do Banco de Dados MySQL: {current_db_name}"]
    
    try:
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        for table_row in tables:
            table_name = table_row[0]
            schema_parts.append(f"\n-- Tabela: {table_name}")
            
            try:
                # SHOW CREATE TABLE fornece uma representação que o LLM entende bem
                cursor.execute(f"SHOW CREATE TABLE `{table_name}`") # Usar backticks para nomes de tabela
                create_table_statement = cursor.fetchone()[1]
                schema_parts.append(f"{create_table_statement};")
            except MySQLdb.Error as e_create:
                # Fallback para DESCRIBE se SHOW CREATE TABLE falhar
                schema_parts.append(f"  -- Erro ao obter 'SHOW CREATE TABLE' para {table_name}: {e_create}. Usando DESCRIBE.")
                schema_parts.append("  -- Colunas:")
                cursor.execute(f"DESCRIBE `{table_name}`") # Usar backticks
                columns = cursor.fetchall()
                for col in columns:
                    col_name, col_type, col_null, col_key, col_default, col_extra = col
                    col_details = [f"`{col_name}`", col_type]
                    if col_null == "NO":
                        col_details.append("NOT NULL")
                    if col_key == "PRI":
                        col_details.append("PRIMARY KEY")
                    if col_default is not None:
                        col_details.append(f"DEFAULT '{col_default}'") # Adicionar aspas para strings default
                    if col_extra:
                        col_details.append(col_extra)
                    schema_parts.append(f"    -- { ' '.join(col_details) }")
            schema_parts.append("") # Linha em branco para separar tabelas

    except MySQLdb.Error as e_tables:
        return f"Erro ao buscar tabelas/colunas do MySQL: {e_tables}"
    finally:
        if cursor:
            cursor.close()
    
    return "\n".join(schema_parts)

# Se você quiser executar este arquivo diretamente para testar (opcional)
#if __name__ == "__main__":
    print("Executando MySQLConnection.py como script principal...")
    db_conn = connect_mysql()
    if db_conn:
        print("Conexão MySQL estabelecida para teste.")
        
        # Teste original de listagem de tabelas e colunas
        print(f"\n--- Informações do banco '{DB_NAME_MYSQL}' (Execução direta) ---")
        main_cursor = db_conn.cursor()
        main_cursor.execute("SHOW TABLES")
        tables_direct = main_cursor.fetchall()
        for table_direct in tables_direct:
            table_name_direct = table_direct[0]
            print(f"\n--- Tabela: {table_name_direct} ---")
            main_cursor.execute(f"DESCRIBE `{table_name_direct}`")
            columns_direct = main_cursor.fetchall()
            print("Campos:")
            for column_direct in columns_direct:
                print(f"  - {column_direct[0]} {column_direct[1]} {column_direct[3]}")
        main_cursor.close()
        
        print("\n--- Schema Formatado para LLM (Execução direta) ---")
        schema_output = get_mysql_schema_as_string(db_conn)
        print(schema_output)
        
        db_conn.close()
        print("\nConexão MySQL fechada (execução direta).")
    else:
        print("Falha ao conectar ao MySQL para teste.")