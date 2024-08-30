# Projeto pyspark utilizando orientação a objetos com arquitetura de medalhão para lakehouse

## Estrutura do projeto
- Orientação a objetos.
- Foi criado um ambiente virtual chamado 'venv' para conter todas as libs necessárias para o projeto.
- Foram criadas 2 clases diferentes para conter os códigos por finalidade. 
- Por razões de viabilidade, vou evitado o carregamento de alguns arquivos neste repositório. A título de exemplo, são arquivos contendo configurações de acesso ao banco postgres, utilizado localmente como fonte de dados, constantes e etc.
- Na estrutura estão descritos todos os arquivos pertencentes ao projeto que é executado localmente. Uma demonstração poderá ser feita para visualizar o projeto rodando.

### Camadas
- Na camada bronze são armazenados os dados sem que seja realizada nenhuma alteração e sem particionamento.
- Na camada silver são armazenados os dados com particionamento e eventuais tratamentos de transformação de tados como por exemplo tipos.
- Na camada gold são armazenados os dados já agregados e enriquecidos conforme as necessidade de negócio.

  
## Estrutura

```
proj_arbo/
    ├───__pycache__/
    ├───lakehouse/      
    |     ├──bronze/
    |     |       ├──condominios/
    |     |       ├──imoveis/
    |     |       ├──moradores/
    |     |       └──transacoes/
    |     ├──silver/
    |     |       ├──condominios/
    |     |       ├──imoveis/
    |     |       ├──moradores/
    |     |       └──transacoes/
    |     └──gold/
    |             ├──tot_transac_por_morador/
    |             ├──transac_por_cond/
    |             └──transc_dia_tipo/ 
    ├───venv/
    ├───.gitignore/
    ├───config.py/
    ├───loading.py/ 
    ├───manage_spark.py/
    ├───postgresql-42.5.1.jar/
    ├───README.md/
    └───requirements.txt
```

## Explanation:

- **lakehouse**: Contém todos os arquivos que compôe o lakehouse.
- **loading.py**: Contém a chase que carrega os dados após as transformações.  
- **manage_spark.py**: Contpem a classe de inicialização e configuração do spark além de métodos de criação de tabela e encerramento de sessão spark. Neste aruqivo também está a execução da aplicação.


## Dependências

### requirements.txt
Para instalar localmente as libs execute: `pip install -r requirements.txt`

### Banco postgreSQL
Foi utilizado um banco postgreSQL como fonte de dados seguindo os DMLs informados no desafio para a criação das tabelas.

## Monitoramento/Alertas
Para executar o pipeline de dados deverá ser utilizada uma instância do Airflow. No conjunto de tabelas nativas do airflow existe uma tabela chamada 'dag_run'. 
Nesta tabela podemos verificar se a DAG responsável pelo processo de carregamento foi executada com sucesso. Além disso é possívl visualizar informações substanciais de logs de erronas execuções das DAGs.
