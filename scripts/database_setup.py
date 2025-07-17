import os
import re
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import logging
from sqlalchemy import create_engine, text

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
load_dotenv()

def clean_column_name(name: str) -> str:
    """Simplifica nomes de coluna para evitar duplicatas"""
    # Remove tudo que não for letra, número ou underscore
    name = re.sub(r'[^\w]', '_', name)
    # Converte para minúsculas e limita o tamanho
    return name.lower()[:50]

def create_database():
    """Cria o banco de dados se não existir"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT'),
            dbname='postgres'
        )
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (os.getenv('DB_NAME'),))
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {os.getenv('DB_NAME')}")
            logger.info(f"Banco {os.getenv('DB_NAME')} criado com sucesso.")
        
        return True
    except Exception as e:
        logger.error(f"Falha ao criar banco: {e}", exc_info=True)
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def load_data():
    """Carrega dados do CSV para o PostgreSQL"""
    try:
        # Caminho do arquivo
        csv_path = os.path.join('data', 'raw', 'base_feirante.csv')
        
        # Leitura do CSV
        df = pd.read_csv(csv_path)
        
        # Limpeza dos nomes das colunas
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Verifica colunas duplicadas
        duplicates = df.columns[df.columns.duplicated()]
        if len(duplicates) > 0:
            logger.warning(f"Colunas duplicadas após limpeza: {duplicates.tolist()}")
            # Adiciona sufixo numérico para colunas duplicadas
            df.columns = [f'{col}_{i}' if col in duplicates[:i] else col 
                         for i, col in enumerate(df.columns)]
        
        # Conexão com o banco
        engine = create_engine(
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        
        # Carga dos dados
        with engine.begin() as conn:
            # Remove tabela existente
            conn.execute(text("DROP TABLE IF EXISTS feirantes"))
            
            # Cria nova tabela com os dados
            df.to_sql(
                name='feirantes',
                con=conn,
                if_exists='append',
                index=False,
                chunksize=1000
            )
        
        logger.info(f"Dados carregados com sucesso! {len(df)} registros.")
        return True
    except Exception as e:
        logger.error(f"Falha na carga: {e}", exc_info=True)
        return False
    finally:
        if 'engine' in locals():
            engine.dispose()

if __name__ == "__main__":
    logger.info("Iniciando setup do banco de dados...")
    
    # Verifica variáveis de ambiente
    required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_PORT', 'DB_NAME']
    if not all([os.getenv(var) for var in required_vars]):
        logger.error("Variáveis de ambiente faltando!")
        exit(1)
    
    # Executa o processo
    if create_database() and load_data():
        logger.info("Processo concluído com sucesso!")
        exit(0)
    else:
        logger.error("Processo falhou")
        exit(1)
