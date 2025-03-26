import pandas as pd
from sql_queries import analytics_queries
from utils import (
    connect_to_redshift,
    format_query_results
)


def execute_analytics_queries(conn, cur):
    """
    Executa as consultas analíticas predefinidas.
    
    Args:
        conn: Conexão com o banco de dados
        cur: Cursor do banco de dados
        
    Returns:
        list: Lista de resultados formatados
    """
    results = []
    
    for query_name, query in analytics_queries.items():
        try:
            print(f"Executando consulta: {query_name}...")
            cur.execute(query)
            rows = cur.fetchall()
            
            # Formatar resultados usando a função utilitária
            formatted_result = format_query_results(cur, rows, query_name)
            results.append(formatted_result)
            
        except Exception as e:
            print(f"Erro ao executar consulta '{query_name}': {e}")
            results.append(f"\n=== {query_name} ===\nERRO: {e}")
    
    return results


def run_analytics():
    """
    Função principal que executa análises no banco de dados.
    """
    print("\n" + "=" * 80)
    print("ANÁLISES SPARKIFY")
    print("=" * 80)
    
    conn = None
    cur = None
    
    try:
        # Conectar ao banco de dados
        conn, cur = connect_to_redshift()
        
        # Executar consultas analíticas
        results = execute_analytics_queries(conn, cur)
        
        # Exibir todos os resultados
        for result in results:
            print(result)
            
        print("\n" + "=" * 80)
        print("ANÁLISES CONCLUÍDAS")
        print("=" * 80)
        
    except Exception as e:
        print(f"Erro ao executar análises: {e}")
        raise
    finally:
        # Fechar conexões diretamente
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Conexão com o banco de dados fechada.")


if __name__ == "__main__":
    run_analytics() 