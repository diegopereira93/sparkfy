"""
Módulo de utilitários para o projeto Sparkify Data Warehouse.
Centraliza funções essenciais para o ETL.
"""
import configparser
import time
import psycopg2


def get_config(config_path='dwh.cfg'):
    """
    Carrega o arquivo de configuração.
    
    Args:
        config_path (str): Caminho para o arquivo de configuração
        
    Returns:
        configparser.ConfigParser: Objeto de configuração
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def connect_to_redshift(config=None, max_attempts=3, wait_time=30):
    """
    Estabelece conexão com o cluster Redshift com tentativas de reconexão.
    
    Args:
        config (configparser.ConfigParser, optional): Configuração já carregada
        max_attempts (int): Número máximo de tentativas de conexão
        wait_time (int): Tempo de espera entre tentativas em segundos
        
    Returns:
        tuple: (conexão, cursor) para o banco de dados
    """
    if config is None:
        config = get_config()
    
    print("Conectando ao cluster Redshift...")
    
    for attempt in range(max_attempts):
        try:
            print(f"Tentativa de conexão {attempt+1}...")
            conn = psycopg2.connect(
                host=config.get('CLUSTER', 'HOST'),
                dbname=config.get('CLUSTER', 'DB_NAME'),
                user=config.get('CLUSTER', 'DB_USER'),
                password=config.get('CLUSTER', 'DB_PASSWORD'),
                port=config.get('CLUSTER', 'DB_PORT')
            )
            conn.autocommit = False  # Controle explícito de transações
            cur = conn.cursor()
            print("Conexão estabelecida com sucesso!")
            return conn, cur
            
        except psycopg2.OperationalError as e:
            print(f"Tentativa {attempt+1} falhou: {e}")
            if attempt < max_attempts - 1:
                print(f"Tentando novamente em {wait_time} segundos...")
                time.sleep(wait_time)
            else:
                print(f"Todas as {max_attempts} tentativas de conexão falharam.")
                print("Verifique se o cluster Redshift está disponível e se a configuração está correta.")
                raise


def execute_query(cursor, conn, query, query_name=None):
    """
    Executa uma query SQL com medição de tempo e tratamento de erros.
    
    Args:
        cursor: Cursor do banco de dados
        conn: Conexão com o banco de dados
        query (str): Query SQL a ser executada
        query_name (str, optional): Nome da query para logs
        
    Returns:
        float: Tempo de execução em segundos
    """
    query_desc = query_name or f"Query #{id(query)}"
    print(f"Executando {query_desc}...")
    
    start_time = time.time()
    
    try:
        cursor.execute(query)
        conn.commit()
        
        elapsed_time = time.time() - start_time
        print(f"{query_desc} concluída em {elapsed_time:.2f} segundos")
        
        return elapsed_time
    
    except Exception as e:
        print(f"Erro ao executar {query_desc}: {e}")
        conn.rollback()
        raise 