#MySQL
import MySQLdb

DB_HOST_MYSQL = "localhost"
DB_USER_MYSQL = "root"
DB_PASSWORD_MYSQL = "Abcd1234!"
DB_NAME_MYSQL = "University"

def connectMysql(host=DB_HOST_MYSQL, user=DB_USER_MYSQL, password=DB_PASSWORD_MYSQL, db_name=DB_NAME_MYSQL):
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

def getMysqlSchema(db_connection):

    if not db_connection:
        return "Schema não disponível (falha na conexão MySQL)."

    cursor = db_connection.cursor()

    try:
        current_name = db_connection.db.decode()
    except AttributeError: 
        current_name = DB_NAME_MYSQL 

    schema_parts = [f"### Schema do Banco de Dados MySQL: {current_name}"]
    
    try:
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        for table_row in tables:
            table_name = table_row[0]
            schema_parts.append(f"\n-- Tabela: {table_name}")
            
            try:
                cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
                create_table_statement = cursor.fetchone()[1]
                schema_parts.append(f"{create_table_statement};")
            except MySQLdb.Error as e_create:
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