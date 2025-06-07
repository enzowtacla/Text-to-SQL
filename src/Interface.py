import streamlit as st
import pandas as pd
import ollama
import MySQLConnection as mysql
import PGConnection as pg

def get_schema(db_type, connection):
    if db_type == "MySQL":
        return mysql.get_mysql_schema(connection)
    elif db_type == "PostgreSQL":
        return pg.get_postgresql_schema(connection)
    return None

def generate_sql(schema, question, db_type):
    prompt = f"""
    Com base no schema do banco de dados {db_type} abaixo, gere uma consulta SQL que responda à seguinte pergunta.
    Responda APENAS com o código SQL, sem explicações, introduções ou formatação markdown.

    ### Schema:
    {schema}

    ### Pergunta:
    {question}

    ### SQL:
    """
    try:
        response = ollama.chat(
            model='llama3',
            messages=[{'role': 'user', 'content': prompt.strip()}],
            options={'temperature': 0.0}
        )
        sql_query = response['message']['content'].strip()
        return sql_query
    except Exception as e:
        st.error(f"Erro ao comunicar com o Ollama: {e}")
        return None

#Função para gerar a resposta em texto a partir dos dados
def generate_text_response(question, df):
    if df.empty:
        return "A consulta não retornou resultados."
        
    data_string = df.to_string(index=False)
    
    prompt = f"""
    Com base na pergunta original do usuário e nos dados retornados pela consulta, formule uma resposta amigável e direta em linguagem natural.
    Seja conciso e direto ao ponto.

    ### Pergunta Original do Usuário:
    "{question}"

    ### Dados Retornados pela Consulta:
    {data_string}

    ### Resposta em Texto:
    """
    try:
        response = ollama.chat(
            model='llama3',
            messages=[{'role': 'user', 'content': prompt.strip()}],
            options={'temperature': 0.5}
        )
        return response['message']['content'].strip()
    except Exception as e:
        st.error(f"Erro ao comunicar com o Ollama para gerar texto: {e}")
        return "Não foi possível gerar a resposta em texto."


#  configuração da página
st.set_page_config(page_title="Text to SQL", page_icon=":robot:", layout="wide")
st.title("Text to SQL")

#  gerenciamento de estado da sessão
if "connection" not in st.session_state:
    st.session_state.connection = None
    st.session_state.db_type = None
    st.session_state.schema = None

# barra lateral
with st.sidebar:
    st.header("Conexão com o Banco")
    db_choice = st.radio("Selecione o Banco de Dados:", ("MySQL", "PostgreSQL"), key="db_choice")

    if st.button("Conectar"):
        st.session_state.connection = None
        st.session_state.schema = None
        st.session_state.db_type = db_choice
        
        with st.spinner(f"Conectando ao {db_choice}..."):
            if db_choice == "MySQL":
                st.session_state.connection = mysql.connect_mysql()
            elif db_choice == "PostgreSQL":
                st.session_state.connection = pg.connect_postgresql()
        
        if st.session_state.connection:
            st.success(f"Conectado ao {db_choice}!")
            with st.spinner("Obtendo schema do banco..."):
                st.session_state.schema = get_schema(db_choice, st.session_state.connection)
        else:
            st.error(f"Falha na conexão com o {db_choice}.")
            st.session_state.db_type = None

    if st.session_state.connection:
        st.success(f"Conectado: {st.session_state.db_type}")
        with st.expander("Ver Schema do Banco"):
            st.code(st.session_state.schema, language='sql')

# Área de perguntas
if not st.session_state.connection:
    st.info("Conecte-se a um banco de dados na barra lateral para começar.")
    st.stop()

question = st.text_area("Faça sua pergunta em linguagem natural:", height=100)

if st.button("Gerar Resposta", type="primary"):
    if not question:
        st.warning("Insira uma pergunta.")
    else:
        with st.spinner("1/3 - Gerando consulta SQL..."):
            generated_sql = generate_sql(st.session_state.schema, question, st.session_state.db_type)

        if generated_sql:
            with st.spinner("2/3 - Executando consulta no banco de dados..."):
                try:
                    df_results = pd.read_sql_query(generated_sql, st.session_state.connection)
                except Exception as e:
                    st.error(f"Erro ao executar SQL: {e}")
                    st.code(generated_sql, language="sql")
                    st.stop()
            
            with st.spinner("3/3 - O Llama 3 está formulando a resposta em texto..."):
                text_response = generate_text_response(question, df_results)
            
            # NOVO: Exibe a resposta em texto primeiro, para destaque
            st.subheader("Resposta")
            st.markdown(f"> {text_response}")
            
            # Expander para os detalhes técnicos
            with st.expander("Ver detalhes técnicos (SQL e Tabela de Dados)"):
                st.subheader("SQL Gerado:")
                st.code(generated_sql, language="sql")
                
                st.subheader("Dados Retornados:")
                st.dataframe(df_results)