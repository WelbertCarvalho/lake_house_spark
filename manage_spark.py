from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import asc, sum

from config import conn, JDBC_DRIVER, PATH
from loading import DataLoader

class ManageSpark():
    def __init__(self, app_name: str, jdbc_driver_path: str):
        self._started_in = datetime.now().strftime('%y-%m-%d %H:%M:%S')
        self._app_name = app_name
        self._jdbc_driver = jdbc_driver_path
    
    def __str__(self) -> str:
        return f'App name: {self._app_name} \nStarted in: {self._started_in}'

    def start_spark(self) -> SparkSession:
        '''
        Este método inicia uma nova sessão Spark.
        '''
        spark = (
            SparkSession
                .builder
                .master('local[*]')
                .appName(self._app_name)
                .config('spark.jars', self._jdbc_driver)
                .getOrCreate()
        )

        return spark

    def get_table(self, spark_session: SparkSession, user: str, password: str, host: str, database: str, table: str) -> DataFrame:
        jdbc_url = f'jdbc:postgresql://{host}/{database}?ssl=true&sslmode=require'
        table = table
        properties = {
            'user': user,
            'password': password,
            'driver': 'org.postgresql.Driver',
            'ssl':'true',
            'sslmode':'require'
        }

        spark_df = spark_session.read.jdbc(
            url = jdbc_url, 
            table = table, 
            properties = properties
        )

        return spark_df

    def stop_spark(self, spark_session: SparkSession) -> None:
        '''
        Este método encerra uma sessão Spark existente.
        '''
        spark_session.stop()
        print('------- A sessão Spark foi encerrada -------')
        return None

if __name__ == '__main__':
    instancia_gerencia_spark = ManageSpark(
        app_name = 'arbo',
        jdbc_driver_path = JDBC_DRIVER
    
    )
    print(instancia_gerencia_spark)

    spark = instancia_gerencia_spark.start_spark()

    # Condominios
    condominios = instancia_gerencia_spark.get_table(
        spark_session = spark,
        user = conn['user'],
        password = conn['password'],
        host = conn['host'],
        database = conn['database'],
        table = 'condominios'
    )

    print('Condomínios -----------------------------------------------------------------------')
    print(condominios.show())

    # Imóveis
    imoveis = instancia_gerencia_spark.get_table(
        spark_session = spark,
        user = conn['user'],
        password = conn['password'],
        host = conn['host'],
        database = conn['database'],
        table = 'imoveis'
    )

    print('Imoveis -----------------------------------------------------------------------')
    print(imoveis.show())

    # Moradores
    moradores = instancia_gerencia_spark.get_table(
        spark_session = spark,
        user = conn['user'],
        password = conn['password'],
        host = conn['host'],
        database = conn['database'],
        table = 'moradores'
    )

    print('Moradores -----------------------------------------------------------------------')
    print(moradores.show())

    # Transações
    transacoes = instancia_gerencia_spark.get_table(
        spark_session = spark,
        user = conn['user'],
        password = conn['password'],
        host = conn['host'],
        database = conn['database'],
        table = 'transacoes'
    )

    print('Transações -----------------------------------------------------------------------')
    print(transacoes.show())


    # Salvando os dados na camada bronze (Sem realizar nenhum particionamento)
    path_bronze = f'{PATH}/bronze'
    
    dl_bronze_condominios = DataLoader(path_to_save = path_bronze, table_name = 'condominios')
    tb_bronze_condominios = dl_bronze_condominios.create_table(dataframe_to_save = condominios, format = 'parquet')

    dl_bronze_imoveis = DataLoader(path_to_save = path_bronze, table_name = 'imoveis')
    tb_bronze_imoveis = dl_bronze_imoveis.create_table(dataframe_to_save = imoveis, format = 'parquet')

    dl_bronze_moradores = DataLoader(path_to_save = path_bronze, table_name = 'moradores')
    tb_bronze_moradores = dl_bronze_moradores.create_table(dataframe_to_save = moradores, format = 'parquet')
    
    dl_bronze_transacoes = DataLoader(path_to_save = path_bronze, table_name = 'transacoes')
    tb_bronze_transacoes = dl_bronze_transacoes.create_table(dataframe_to_save = transacoes, format = 'parquet')


    # Salvando dados na camada silver (Aplicando particionamentos coerentes ao negócio)
    path_silver = f'{PATH}/silver'

    dl_silver_condominios = DataLoader(path_to_save = path_silver, table_name = 'condominios')
    tb_silver_condominios = dl_silver_condominios.create_table(dataframe_to_save = condominios, format = 'parquet')

    dl_silver_imoveis = DataLoader(path_to_save = path_silver, table_name = 'imoveis')
    tb_silver_imoveis = dl_silver_imoveis.create_table(dataframe_to_save = imoveis, format = 'parquet', partition = 'tipo')

    dl_silver_moradores = DataLoader(path_to_save = path_silver, table_name = 'moradores')
    tb_silver_moradores = dl_silver_moradores.create_table(dataframe_to_save = moradores, format = 'parquet', partition = 'data_registro')
    
    dl_silver_transacoes = DataLoader(path_to_save = path_silver, table_name = 'transacoes')
    tb_silver_transacoes = dl_silver_transacoes.create_table(dataframe_to_save = transacoes, format = 'parquet', partition = 'data_transacao')

    # Transformando os dados da gold (Retornando os dados refinados conforme as perguntas de negócio)
    print('Transformando os dados da gold --------------------------------------------')
    
    # 1 - Total transações por condomínio
    print('1 - Total transações por condomínio ---------------------------------------')
    transac_por_cond = (
        transacoes
            .join(other = moradores, on = transacoes['morador_id'] == moradores['morador_id'], how = 'inner')
            .join(other = condominios, on = moradores['condominio_id'] == condominios['condominio_id'])
            .select(condominios['nome'], condominios['condominio_id'])
            .groupBy('nome', 'condominio_id')
            .count()
            .orderBy(asc('condominio_id'))
    )

    print(transac_por_cond.show())

    # 2 - Valor total de transações por morador
    print('2 - Valor total de transações por morador ---------------------------------------')
    tot_transac_por_morador = (
        transacoes
            .join(other = moradores, on = transacoes['morador_id'] == moradores['morador_id'], how = 'inner')
            .select(moradores['morador_id'], moradores['nome'], transacoes['valor_transacao'])
            .groupBy('morador_id', 'nome')
            .agg(sum(transacoes['valor_transacao']))
            .orderBy(asc('nome'))
    )

    print(tot_transac_por_morador.show())

    # 3 - Agregar as transações diárias por tipo de imóvel
    print('3 - Agregar as transações diárias por tipo de imóvel ---------------------------------------')
    transc_dia_tipo = (
        transacoes
            .join(other = imoveis, on = transacoes['imovel_id'] == imoveis['imovel_id'], how = 'inner')
            .select(transacoes['data_transacao'], imoveis['tipo'])
            .groupBy('data_transacao', 'tipo')
            .count()
            .orderBy(asc('data_transacao'), asc('tipo'))
    )

    print(transc_dia_tipo.show())


    # Salvando dados na camada gold
    path_gold = f'{PATH}/gold'

    dl_transac_por_cond = DataLoader(path_to_save = path_gold, table_name = 'transac_por_cond')
    tb_transac_por_cond = dl_transac_por_cond.create_table(dataframe_to_save = transac_por_cond, format = 'parquet')

    dl_tot_transac_por_morador = DataLoader(path_to_save = path_gold, table_name = 'tot_transac_por_morador')
    tb_tot_transac_por_morador = dl_tot_transac_por_morador.create_table(dataframe_to_save = tot_transac_por_morador, format = 'parquet')

    dl_transc_dia_tipo = DataLoader(path_to_save = path_gold, table_name = 'transc_dia_tipo')
    tb_transc_dia_tipo = dl_transc_dia_tipo.create_table(dataframe_to_save = transc_dia_tipo, format = 'parquet')


    instancia_gerencia_spark.stop_spark(spark_session = spark)
    
