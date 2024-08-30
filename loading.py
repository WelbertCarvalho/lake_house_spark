from pyspark.sql import DataFrame

class DataLoader():
    def __init__(self, path_to_save: str, table_name: str):
        self._path_to_save = path_to_save
        self._table_name = table_name

    def __str__(self) -> str:
        return f'Caminho para salvar: {self._path_to_save} \nNome da tabela: {self._table_name}'

    def _path_to_files(self) -> str:
        '''
        Este método cria a string que provê o caminho completo para salvar arquivos.
        '''
        complete_path = f'{self._path_to_save}/{self._table_name}' 
        return complete_path
    
    def create_table(self, dataframe_to_save: DataFrame, format: str, partition: str = None) -> None:
        '''
        Este método cria uma tabela delta utilizando um dataframe como fonte de dados.
        '''
        path_and_name_of_table = self._path_to_files()

        if partition != None:
            (dataframe_to_save
                    .write
                    .format(format)
                    .mode('overwrite')
                    .partitionBy(partition)
                    .save(path_and_name_of_table))
            print(f'Tabela: {self._table_name} | Partição: {partition} | Salva em: {path_and_name_of_table}')
        else:
            (dataframe_to_save
                    .write
                    .format(format)
                    .mode('overwrite')
                    .save(path_and_name_of_table))
            print(f'Tabela: {self._table_name} | Salva em: {path_and_name_of_table}')
       

        return None
