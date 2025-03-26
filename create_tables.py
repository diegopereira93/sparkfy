import time
from sql_queries import create_table_queries, drop_table_queries
from utils import (
    connect_to_redshift,
    execute_query
)


def drop_tables(cur, conn):
    """
    Exclui todas as tabelas definidas em drop_table_queries.
    
    Parameters:
        cur: cursor para execução de comandos SQL
        conn: conexão com o banco de dados
    """
    print("Excluindo tabelas existentes...")
    
    for i, query in enumerate(drop_table_queries, 1):
        table_name = query.split("DROP TABLE")[1].split(";")[0].strip() if "DROP TABLE" in query else f"Tabela {i}"
        print(f"  Excluindo {table_name}...")
        execute_query(cur, conn, query, f"Excluindo {table_name}")


def create_tables(cur, conn):
    """
    Cria todas as tabelas definidas em create_table_queries.
    
    Parameters:
        cur: cursor para execução de comandos SQL
        conn: conexão com o banco de dados
    """
    print("\nCriando tabelas...")
    
    for i, query in enumerate(create_table_queries, 1):
        table_name = query.split("CREATE TABLE")[1].split("(")[0].strip() if "CREATE TABLE" in query else f"Tabela {i}"
        print(f"  Criando {table_name}...")
        execute_query(cur, conn, query, f"Criando {table_name}")


def run_setup():
    """
    Função principal que configura as tabelas do banco de dados.
    """
    print("\n" + "=" * 80)
    print("CONFIGURAÇÃO DE TABELAS DO SPARKIFY")
    print("=" * 80)
    
    start_time = time.time()
    
    conn = None
    cur = None
    
    try:
        # Conectar ao banco de dados
        conn, cur = connect_to_redshift()
        
        # Executar operações de banco de dados
        drop_tables(cur, conn)
        create_tables(cur, conn)
        
        # Calcular e exibir tempo total
        elapsed_time = time.time() - start_time
        print(f"\nConfiguração de tabelas concluída com sucesso em {elapsed_time:.2f} segundos!")
        
    except Exception as e:
        print(f"Erro durante a configuração de tabelas: {e}")
        raise
    finally:
        # Fechar conexões diretamente
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Conexão com o banco de dados fechada.")


if __name__ == "__main__":
    run_setup()