import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
#from airflow.operators.python import BranchPythonOperator
#from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta


#DAG 1
# OK Ler os dados e escrever localmente dentro do container numa pasta /tmp
#Processar os seguintes indicadores: 
# OK Quantidade de passageiros por sexo e classe (produzir e escrever)
# OK Preço médio da tarifa pago por sexo e classe (produzir e escrever)
# OK Quantidade total de SibSp + Parch (tudo junto) por sexo e classe (produzir e escrever)
# OK Juntar todos os indicadores criados em um único dataset (produzir o dataset e escrever) /tmp/tabela_unica.csv
# OK Printar a tabela nos logs
#Triggar a Dag2

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args = {
    'owner': "Ney",
    "depends_on_past": False,
    'start_date': datetime (2022, 10, 13)
}

@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['TaskflowAPI'])
def trab2_titanic():

    @task
    def ingestao():
        NOME_DO_ARQUIVO = "/tmp/titanic.csv"
        df = pd.read_csv(URL, sep=';')
        df.to_csv(NOME_DO_ARQUIVO, index=False, sep=";")
        #pandas.core.frame.DataFrame type of df
        print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAB")
        print("Quantidade de passageiros por sexo e classe = ",df.groupby(['Sex','Pclass'])['PassengerId'].count())
        print("Preço médio da tarifa pago por sexo e classe = ",df.groupby(['Sex','Pclass'])['Fare'].mean())
        column_names = ['SibSp','Parch']
        df['sum_family']= df[column_names].sum(axis=1)
        print("Quantidade total de SibSp + Parch por sexo e classe = ",df.groupby(['Sex','Pclass'])['sum_family'].sum())
        #print (df.columns) #Index(['PassengerId', 'Survived', 'Pclass', 'Name', 'Sex', 'Age', 'SibSp','Parch', 'Ticket', 'Fare', 'Cabin', 'Embarked']
        df2 = pd.DataFrame(columns = column_names)
        #print(df2)
        df2['qtd_pass'] = df.groupby(['Sex','Pclass'])['PassengerId'].count()
        #print('count pass',df2)
        df2['mean_price'] = df.groupby(['Sex','Pclass'])['Fare'].mean()
        #print(df2)
        df2['sum_sum_family'] = df.groupby(['Sex','Pclass'])['sum_family'].sum()
        NOME_DO_ARQUIVO_INDICADORES = "/tmp/tabela_unica.csv"
        df2.to_csv(NOME_DO_ARQUIVO_INDICADORES, index=False, sep=";") 
        print('tabela de indicadores:\n',df2)


    fim = DummyOperator(task_id="fim")

    ing = ingestao()
    #indicador = ind_passageiros(ing)

    ing >> fim


execucao = trab2_titanic()
