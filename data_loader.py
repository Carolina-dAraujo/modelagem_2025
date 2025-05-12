import csv
import logging
from datetime import datetime
import mysql.connector
from mysql.connector import errorcode
from typing import Dict, List, Optional

# Configurações (ajuste conforme necessário)
CONFIG = {
    'db': {
        'host': 'localhost',
        'database': 'modelagem',
        'user': 'seu_usuario',
        'password': 'sua_senha',
        'port': 3306
    },
    'csv_folder': 'dados',
    'batch_size': 10000,
    'encoding': 'utf-8',
    'log_file': 'data_loader.log'
}

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(CONFIG['log_file']),
        logging.StreamHandler()
    ]
)

class MySQLDataLoader:
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.conn = None
    
    def __enter__(self):
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            logging.info("Conexão com o banco de dados estabelecida")
            return self
        except mysql.connector.Error as err:
            logging.error(f"Erro ao conectar ao banco de dados: {err}")
            raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type is None:
                self.conn.commit()
                logging.info("Transação concluída com sucesso")
            else:
                self.conn.rollback()
                logging.error(f"Erro durante a carga. Rollback executado: {exc_val}")
            self.conn.close()
            logging.info("Conexão com o banco de dados encerrada")
    
    def load_table(self, table_name: str, csv_file: str, columns: List[str]):
        """Carrega dados de um CSV para uma tabela do banco de dados"""
        csv_path = f"{CONFIG['csv_folder']}/{csv_file}"
        logging.info(f"Iniciando carga da tabela {table_name} a partir de {csv_path}")
        
        try:
            cursor = self.conn.cursor()
            
            # Contador de linhas processadas
            total_rows = 0
            batch_rows = []
            
            with open(csv_path, 'r', encoding=CONFIG['encoding']) as file:
                reader = csv.DictReader(file, delimiter=',')
                
                for row in reader:
                    # Prepara os dados para inserção
                    processed_row = []
                    for col in columns:
                        value = row.get(col)
                        
                        # Tratamento especial para campos YEAR
                        if 'ano' in col.lower() and value:
                            value = int(value) if value else None
                        # Tratamento para campos DECIMAL
                        elif any(keyword in col.lower() for keyword in ['decimal', 'valor', 'despesa', 'receita', 'investimento']):
                            value = float(value) if value else None
                        # Tratamento para campos INT
                        elif any(keyword in col.lower() for keyword in ['populacao', 'quantidade', 'id_']):
                            value = int(value) if value else None
                        
                        processed_row.append(value)
                    
                    batch_rows.append(processed_row)
                    
                    # Insere em lotes
                    if len(batch_rows) >= CONFIG['batch_size']:
                        self._insert_batch(cursor, table_name, columns, batch_rows)
                        total_rows += len(batch_rows)
                        batch_rows = []
                        logging.info(f"{total_rows} linhas processadas...")
                
                # Insere o último lote (se houver)
                if batch_rows:
                    self._insert_batch(cursor, table_name, columns, batch_rows)
                    total_rows += len(batch_rows)
            
            logging.info(f"Carga concluída para {table_name}. Total de linhas: {total_rows}")
        
        except Exception as e:
            logging.error(f"Erro durante a carga de {table_name}: {e}")
            raise
    
    def _insert_batch(self, cursor, table_name: str, columns: List[str], batch_rows: List[List]):
        """Insere um lote de registros na tabela"""
        try:
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join(columns)
            query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            
            cursor.executemany(query, batch_rows)
            self.conn.commit()
        
        except mysql.connector.Error as err:
            logging.error(f"Erro ao inserir lote na tabela {table_name}: {err}")
            raise

def main():
    # Ordem de carga respeitando as relações de chave estrangeira
    tables_to_load = [
        {
            'table': 'modelagem.municipio',
            'csv': 'municipio.csv',
            'columns': ['id_municipio', 'sigla_uf']
        },
        {
            'table': 'modelagem.demografia',
            'csv': 'demografia.csv',
            'columns': [
                'ano', 'id_municipio', 'populacao_urbana', 'populacao_urbana_residente_agua',
                'populacao_urbana_atendida_agua', 'populacao_urbana_residente_esgoto',
                'populacao_urbana_atendida_esgoto'
            ]
        },
        {
            'table': 'modelagem.despesas',
            'csv': 'despesas.csv',
            'columns': [
                'ano', 'id_municipio', 'despesa_pessoal', 'despesa_produto_quimico',
                'despesa_energia', 'despesa_servico_terceiro', 'despesa_exploracao',
                'despesas_juros_divida', 'despesa_total_servico', 'despesa_ativo',
                'despesa_agua_importada', 'despesa_fiscal', 'despesa_fiscal_nao_computada',
                'despesa_exploracao_outro', 'despesa_servico_outro', 'despesa_amortizacao_divida',
                'despesas_juros_divida_excecao', 'despesa_divida_variacao', 'despesa_divida_total',
                'despesa_esgoto_exportado', 'despesa_capitalizavel_municipio',
                'despesa_capitalizavel_estado', 'despesa_capitalizavel_prestador'
            ]
        },
        {
            'table': 'modelagem.financeiro_geral',
            'csv': 'financeiro_geral.csv',
            'columns': [
                'ano', 'id_municipio', 'receita_operacional_direta',
                'receita_operacional_direta_agua', 'receita_operacional_direta_esgoto',
                'receita_operacional_indireta', 'receita_operacional_direta_agua_exportada',
                'receita_operacional', 'receita_operacional_direta_esgoto_importado'
            ]
        },
        {
            'table': 'modelagem.indices',
            'csv': 'indices.csv',
            'columns': [
                'ano', 'id_municipio', 'indice_agua_ligacao', 'indice_hidrometracao',
                'indice_macromedicao', 'indice_perda_faturamento', 'indice_coleta_esgoto',
                'indice_tratamento_esgoto', 'indice_consumo_agua', 'indice_fluoretacao_agua'
            ]
        },
        {
            'table': 'modelagem.infraestrutura',
            'csv': 'infraestrutura.csv',
            'columns': [
                'ano', 'id_municipio', 'extensao_rede_agua', 'extensao_rede_esgoto',
                'quantidade_sede_municipal_agua', 'quantidade_sede_municipal_esgoto',
                'quantidade_localidade_agua', 'quantidade_localidade_esgoto',
                'quantidade_ligacao_ativa_agua', 'quantidade_ligacao_ativa_esgoto',
                'volume_agua_produzido', 'volume_esgoto_tratado'
            ]
        },
        {
            'table': 'modelagem.investimentos',
            'csv': 'investimentos.csv',
            'columns': [
                'ano', 'id_municipio', 'investimento_agua_prestador', 'investimento_esgoto_prestador',
                'investimento_outro_prestador', 'investimento_recurso_proprio_prestador',
                'investimento_recurso_oneroso_prestador', 'investimento_recurso_nao_oneroso_prestador',
                'investimento_total_prestador', 'investimento_agua_municipio',
                'investimento_esgoto_municipio', 'investimento_outro_municipio',
                'investimento_recurso_proprio_municipio', 'investimento_recurso_oneroso_municipio',
                'investimento_recurso_nao_oneroso_municipio', 'investimento_total_municipio',
                'investimento_agua_estado', 'investimento_esgoto_estado', 'investimento_outro_estado',
                'investimento_recurso_proprio_estado', 'investimento_recurso_oneroso_estado',
                'investimento_recurso_nao_oneroso_estado', 'investimento_total_estado'
            ]
        }
    ]

    try:
        with MySQLDataLoader(CONFIG['db']) as loader:
            for table_config in tables_to_load:
                start_time = datetime.now()
                logging.info(f"Iniciando carga para {table_config['table']}...")
                
                loader.load_table(
                    table_name=table_config['table'],
                    csv_file=table_config['csv'],
                    columns=table_config['columns']
                )
                
                duration = datetime.now() - start_time
                logging.info(f"Carga de {table_config['table']} concluída em {duration}")
    
    except Exception as e:
        logging.error(f"Erro fatal durante a execução: {e}")
        raise

if __name__ == "__main__":
    logging.info("Iniciando processo de carga de dados")
    main()
    logging.info("Processo de carga de dados concluído")