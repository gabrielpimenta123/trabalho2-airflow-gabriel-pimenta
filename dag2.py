import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta


#DAG 2
# Ler a tabela única de indicadores feitos na Dag1 (/tmp/tabela_unica.csv)
# Produzir médias para cada indicador considerando o total
# Printar a tabela nos logs
# Escrever o resultado em um arquivo csv local no container (/tmp/resultados.csv)

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args = {
    'owner': "Ney",
    "depends_on_past": False,
    'start_date': datetime (2022, 10, 13)
}

@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['TaskflowAPI'])
def trab2_titanic_2():

    @task
    def dag_2():
        print('CHEGOU ATÉ AQUI')

    #@task
    #def ind_passageiros(nome_do_arquivo):
    #    NOME_TABELA = "/tmp/passageiros_por_sexo_classe.csv"
    #    df = pd.read_csv(nome_do_arquivo, sep=";") 
    #    res = df.groupby(['Sex', 'Pclass']).agg({
    #        "PassengerId": "count"
    #    }).reset_index()
    #    print(res)
    #    res.to_csv(NOME_TABELA, index=False, sep=";")
    #    return NOME_TABELA

    fim = DummyOperator(task_id="fim")

    #ing = ingestao()
    #indicador = ind_passageiros(ing)

    #ing >> fim


execucao = trab2_titanic_2()
