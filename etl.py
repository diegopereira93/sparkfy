import psycopg2
import time
from sql_queries import copy_table_queries, insert_table_queries
from utils import (
    get_config, 
    connect_to_redshift,
    execute_query
)


def load_staging_tables(cur, conn, config):
    """
    Load data from S3 into staging tables.
    
    Args:
        cur: Database cursor
        conn: Database connection
        config: Configuration parser with S3 paths
    """
    try:
        print("\n" + "=" * 80)
        print("INICIANDO CARREGAMENTO DE DADOS PARA STAGING")
        print("=" * 80)
        
        for i, query in enumerate(copy_table_queries):
            try:
                print(f"\nExecutando query de staging {i+1}/{len(copy_table_queries)}")
                
                # Identificar qual staging está sendo carregado
                if "staging_events" in query.lower():
                    print(f"Carregando staging_events...")
                elif "staging_songs" in query.lower():
                    print(f"Carregando staging_songs...")
                
                execute_query(cur, conn, query, f"Query de staging {i+1}")
                
            except Exception as e:
                print(f"Erro na query de staging {i+1}: {e}")
                conn.rollback()
                raise
        
        print("\nCarregamento de staging concluído com sucesso!")
        
    except Exception as e:
        print(f"Erro durante o carregamento de dados para staging: {e}")
        conn.rollback()
        raise


def insert_tables(cur, conn):
    """
    Insert data from staging tables into analytics tables.
    
    Args:
        cur: Database cursor
        conn: Database connection
    """
    print("\n" + "=" * 80)
    print("INICIANDO INSERÇÃO NAS TABELAS ANALÍTICAS")
    print("=" * 80)
    
    for i, query in enumerate(insert_table_queries):
        try:
            table_name = query.split("INSERT INTO ")[1].split(" ")[0] if "INSERT INTO " in query else f"tabela {i+1}"
            print(f"Populando {table_name}...")
            
            execute_query(cur, conn, query, f"Povoamento de {table_name}")
            
        except Exception as e:
            print(f"Erro ao popular {table_name}: {e}")
            conn.rollback()
            raise
    
    print("\nInserção nas tabelas analíticas concluída com sucesso!")


def run_etl():
    """
    Função principal para executar o processo ETL completo.
    """
    print("\n" + "=" * 80)
    print("SPARKIFY ETL")
    print("=" * 80)
    
    start_time = time.time()
    
    conn = None
    cur = None
    
    try:
        # Carregar configuração
        config = get_config()
        
        # Conectar ao banco de dados
        conn, cur = connect_to_redshift()
        
        # Carregar dados para as tabelas de staging
        load_staging_tables(cur, conn, config)
        
        # Inserir dados nas tabelas analíticas
        insert_tables(cur, conn)
        
        # Exibir resumo
        total_time = time.time() - start_time
        print("\n" + "=" * 80)
        print("RESUMO DO PROCESSAMENTO ETL")
        print("=" * 80)
        
        print(f"Tempo total de execução: {total_time:.2f} segundos")
        print("\nProcesso ETL concluído com sucesso!")
        
    except Exception as e:
        print(f"Erro durante o processo ETL: {e}")
        raise
    finally:
        # Fechar conexões
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Conexão com o banco de dados fechada.")


if __name__ == "__main__":
    run_etl()