## LIBS 
from datetime import datetime as dt, timedelta
from dotenv import load_dotenv # QUANDO NOS TIVERMOS O COFRE DE SEMHAS TROCAREMOS ESSA LIB PELA LIB DE CONSUMO DE CHAVES DA AWS
from bs4 import BeautifulSoup
from app_s3 import S3 # LIB AWS

import pandas as pd
import datetime
import requests
import os

## CLASSE PRINCIAL BI
class BI:
    def __init__(self):
        ### CREDENCIAIS
        dotenv_path = os.path.join('.env')
        load_dotenv(dotenv_path)
        
        # COSTUMER_BRUTO
        self.usr_costomer = os.getenv("USR_COSTOMER")
        self.pwd_costomer = os.getenv("PWD_COSTOMER")

        # MARGE_BRUTO
        self.username_marge = os.getenv("username_marge")
        self.password_marge = os.getenv("password_marge")
        self.password_marge_by_bills = os.getenv("aut_marge_by_bills")

        # IZZY PLAN
        self.izzy_plan_email = os.getenv("izzy_plan_email")
        self.izzy_plan_password = os.getenv("izzy_plan_password")
        self.izzy_plan_token = os.getenv("izzy_plan_token")
        pass
    
    # FUNCAO GENERICA QUE EXPLODE COLUNAS
    def explode_and_normalize(self, df, explode_columns):

        df_result = df.copy()

        for col in explode_columns:
            df_exploded = df_result.explode(col)
            df_final = pd.json_normalize(df_exploded[col])

            l_colunas = [f'{col}_{u}' for u in list(df_final.columns)]
            df_final.rename(columns=dict(zip(df_final.columns, l_colunas)), inplace=True)

            df_result = pd.concat([df_exploded.reset_index(drop=True), df_final], axis=1).drop(columns=[col])

        return df_result
                   
    # CALENDÁRIO DE OBRAS DAYS OFF
    def days_off(self):
        # NOME DO CAMINHO
        nm_path = 'days-off'

        # NOME DO ARQUIVO
        nm_arq = 'days-off'

        url = "https://api.sienge.com.br/labutare/public/api/v1/building-projects/109/calendar/days-off"

        username = "labutare-bi"
        password = "toYqCZweFTzX8AikcPAu4IspSxrsZtdN"

        params = {
            "startDate": "2024-10-01",
            "endDate": format(dt.now()- timedelta(1), '%Y-%m-%d'),
            "selectionType": "I",
            "correctionIndexerId": "1",
            "correctionDate": "2025-01-31",
            "billsIds": "20"
        }

        response = requests.get(url, auth=(username, password), params=params if "bulk-data" in url else None)

        if response.status_code == 200:
                    data = response.json()['results']

                    df = pd.json_normalize(data)

                    ### EXPORTAR PARA O BUCKET
                    S3().overwrite_dataframes_bucket_bruto(df, nm_arq)
        else:
            raise Exception(f'Erro - {nm_arq} - response status: {response.status_code}')

    # ITENS DE ORÇAMENTO DE OBRA
    def building_cost_estimation_items(self):

        nm_path = 'building_cost_estimation_items'

        # NOME DO ARQUIVO
        nm_arq = 'building_cost_estimation_items'

        ### RESQUEST DA API
        api_url = "https://api.sienge.com.br/labutare/public/api/bulk-data/v1/building-cost-estimation-items"

        df_now = format(dt.now(), '%Y-%m-%d')


        headers = {
            "Content-Type": "application/json"
        }

        response = requests.get(api_url, auth=(self.username_marge, self.password_marge), headers=headers)

        if response.status_code == 200:
            data = response.json()['data']

            df = pd.json_normalize(data)
            
            ### EXPORTAR PARA O BUCKET
            S3().overwrite_dataframes_bucket_bruto(df, nm_arq)
            
            # df.to_csv('00_bronze-local/marge_income.csv', sep=';', index=False) # DADOS BRUTOS NAO SAO TRADOS
            # df.to_parquet('00_bronze-local/marge_income.parquet', engine='fastparquet', index=False) # DADOS BRUTOS NAO SAO TRADOS
        else:
             raise Exception(f'Erro - {nm_arq} - response status: {response.status_code}')
        
    # EMPREENDIMENTOS OBRAS
    def enterprises(self):        
        # NOME DO ARQUIVO
        nm_arq = 'enterprises'

        url = "https://api.sienge.com.br/labutare/public/api/v1/enterprises"

        username = "labutare-bi"
        password = "toYqCZweFTzX8AikcPAu4IspSxrsZtdN"

        params = {
            "startDate": "2024-10-01",
            "endDate": "2025-01-31",
            "selectionType": "I",
            "correctionIndexerId": "1",
            "correctionDate": "2025-01-31",
            "billsIds": "20"
        }

        response = requests.get(url, auth=(username, password), params=params if "bulk-data" in url else None)

        if response.status_code == 200:
                    data = response.json()['results']

                    df = pd.json_normalize(data)

                    ### EXPORTAR PARA O BUCKET
                    S3().overwrite_dataframes_bucket_bruto(df, nm_arq)
        else:
            raise Exception(f'Erro - {nm_arq} - response status: {response.status_code}')   

    # EMPRESA
    def companies(self):
        # NOME DO CAMINHO
        nm_path = 'companies'
        
        # NOME DO ARQUIVO
        nm_arq = 'companies'

        url = "https://api.sienge.com.br/labutare/public/api/v1/companies"

        payload = {}
        headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic bGFidXRhcmUtYmk6dG9ZcUNad2VGVHpYOEFpa2NQQXU0SXNwU3hyc1p0ZE4=',
        'Cookie': 'JSESSIONID=68F40513CB3E8656B10E79F4046C723B'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        if response.status_code == 200:
                    data = response.json()['results']

                    df = pd.json_normalize(data)

                    ### EXPORTAR PARA O BUCKET
                    S3().overwrite_dataframes_bucket_bruto(df, nm_arq)
        else:
            raise Exception(f'Erro - {nm_arq} - response status: {response.status_code}')

    # PARCELAS CONTAS A RECEBER
    def income(self):
        # NOME DO CAMINHO
        nm_path = 'income'
        
        # NOME DO ARQUIVO
        nm_arq = 'income'

        url = "https://api.sienge.com.br/labutare/public/api/bulk-data/v1/income"

        username = "labutare-bi"
        password = "toYqCZweFTzX8AikcPAu4IspSxrsZtdN"

        params = {
            "startDate": "2024-10-01",
            "endDate": "2025-01-31",
            "selectionType": "I",
            "correctionIndexerId": "1",
            "correctionDate": "2025-01-31",
            "billsIds": "20"
        }

        response = requests.get(url, auth=(username, password), params=params if "bulk-data" in url else None)

        if response.status_code == 200:
            data = response.json()['data']

            df = pd.json_normalize(data)

            ### EXPORTAR PARA O BUCKET
            S3().overwrite_dataframes_bucket_bruto(df, nm_arq)
        else:
            raise Exception(f'Erro - {nm_arq} - response status: {response.status_code}')

    def outcome(self):
        # NOME DO ARQUIVO
        nm_arq = 'outcome'

        # Credenciais de autenticação
        username = "labutare-bi"
        password = "toYqCZweFTzX8AikcPAu4IspSxrsZtdN"

        # Lista de endpoints
        url = "https://api.sienge.com.br/labutare/public/api/bulk-data/v1/outcome"

        # Parâmetros para endpoints que exigem filtros
        params = {
            "startDate": "2020-01-01",
            "endDate": format(dt.now(), '%Y-%m-%d'),
            "selectionType": "I",
            "correctionIndexerId": "1",
            "correctionDate": format(dt.now(), '%Y-%m-%d'),
            "billsIds": "20"
        }

        # Loop sobre os endpoints
        response = requests.get(url, auth=(username, password), params=params if "bulk-data" in url else None)

        if response.status_code == 200:
            data = response.json()['data']

            df = pd.json_normalize(data)

            explode_columns = ["paymentsCategories", "departamentsCosts", "buildingsCosts", "payments", "authorizations",
                    "payments_bankMovements","payments_bankMovements_paymentCategories"]
            df_geral = self.explode_and_normalize(df, explode_columns)

            # Salvar no S3
            S3().overwrite_dataframes_bucket_bruto(df_geral, nm_arq)
        else:
            raise Exception(f'Erro - {nm_arq} - response status: {response.status_code}')

    # CALENDÁRIO DE OBRAS
    def calendar(self):

        username = "labutare-bi"
        password = "toYqCZweFTzX8AikcPAu4IspSxrsZtdN"
    

        # NOME DO CAMINHO
        nm_path = 'calendar'

        # NOME DO ARQUIVO
        nm_arq = 'calendar'

        url = "https://api.sienge.com.br/labutare/public/api/v1/building-projects/109/calendar"

        username = "labutare-bi"
        password = "toYqCZweFTzX8AikcPAu4IspSxrsZtdN"

        params = {
           # "startDate": "2024-10-01",
            #"endDate": "2025-01-31",
            #"selectionType": "I",
            #"correctionIndexerId": "1",
            #"correctionDate": "2025-01-31",
            #"billsIds": "20"
        }

        response = requests.get(url, auth=(username, password), params=params if "bulk-data" in url else None)

        if response.status_code == 200:
                    data = response.json()['results']

                    df = pd.json_normalize(data)

                    ### EXPORTAR PARA O BUCKET
                    S3().overwrite_dataframes_bucket_bruto(df, nm_arq)
        else:
            raise Exception(f'Erro - {nm_arq} - response status: {response.status_code}')

    def izzy_plain_project(self):
        def clean_html(text):
            return BeautifulSoup(text, "html.parser").get_text() if isinstance(text, str) else text


        # NOME DO ARQUIVO
        nm_arq = 'izzy_project'

        url = "https://izzyplan.com/api/v1/projects"

        payload = {}
        headers = {
             'Accept': 'application/json',
                'Content-Type': 'application/json',
                'api-token': self.izzy_plan_token,
                'email': self.izzy_plan_email,
                'password': self.izzy_plan_password
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        
        if response.status_code == 200:
             data = response.json()['data']

             df = pd.json_normalize(data)

             if 'description' in df.columns:
                  df["description"] = df["description"].str.replace(r"\n", " ", regex=True)
                  
             if 'description' in df.columns:
                  df["description"] = df["description"].apply(lambda x: BeautifulSoup(x, "html.parser").get_text() if isinstance(x, str) else x)
             
             ### EXPORTAR PARA O BUCKET
             S3().overwrite_dataframes_bucket_bruto(df, nm_arq)
        else:
            raise Exception(f'Erro - {nm_arq} - response status: {response.status_code}')

    def izzy_plain_workflows(self):
        nm_path = 'project_workflow'

        # NOME DO ARQUIVO
        nm_arq = 'project_workflow'
        
        url = "https://izzyplan.com/api/v1/workflows"

        payload = {}
        headers = {
             'Accept': 'application/json',
                'Content-Type': 'application/json',
                'api-token': self.izzy_plan_token,
                'email': self.izzy_plan_email,
                'password': self.izzy_plan_password
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        
        if response.status_code == 200:
             data = response.json()['data']

             df = pd.json_normalize(data)
             
             ### EXPORTAR PARA O BUCKET
             S3().overwrite_dataframes_bucket_bruto(df, nm_arq)
        else:
            raise Exception(f'Erro - {nm_arq} - response status: {response.status_code}')

    def izzy_plain_tasklogs(self):
        # NOME DO ARQUIVO
        nm_arq = 'project_tasklogs'

        url = "https://izzyplan.com/api/v1/task-logs"

        payload = {}
        headers = {
             'Accept': 'application/json',
                'Content-Type': 'application/json',
                'api-token': self.izzy_plan_token,
                'email': self.izzy_plan_email,
                'password': self.izzy_plan_password
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        
        if response.status_code == 200:
             data = response.json()['data']

             df = pd.json_normalize(data)

             df.columns = [i.replace('.','_') for i in df.columns]
             
             ### EXPORTAR PARA O BUCKET
             S3().overwrite_dataframes_bucket_bruto(df, nm_arq)
        else:
            raise Exception(f'Erro - {nm_arq} - response status: {response.status_code}')
 
    def marge_income_bruto(self):
        # NOME DO ARQUIVO
        nm_arq = 'marge_income'

        ### RESQUEST DA API
        api_url = "https://api.sienge.com.br/labutare/public/api/bulk-data/v1/income"

        df_now = format(dt.now(), '%Y-%m-%d')

        params = {
            "startDate": "2021-01-01",
            "endDate": f"{df_now}",
            "selectionType": "I",
            "correctionIndexerId": "1",
            "correctionDate": f"{df_now}"
        }

        headers = {
            "Content-Type": "application/json"
        }

        response = requests.get(api_url, auth=(self.username_marge, self.password_marge), headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()['data']

            df = pd.json_normalize(data)
            
            ### EXPORTAR PARA O BUCKET
            S3().overwrite_dataframes_bucket_bruto(df, nm_arq)
            
            # df.to_csv('00_bronze-local/marge_income.csv', sep=';', index=False) # DADOS BRUTOS NAO SAO TRADOS
            # df.to_parquet('00_bronze-local/marge_income.parquet', engine='fastparquet', index=False) # DADOS BRUTOS NAO SAO TRADOS
        else:
             raise Exception(f'Erro - {nm_arq} - response status: {response.status_code}')

    def marge_defaulters_receivable_bills_bruto(self):
        # NOME DO ARQUIVO
        nm_arq = 'marge_defaulters_receivable_bills'
        companyId = 1
        url = f"https://api.sienge.com.br/labutare/public/api/bulk-data/v1//defaulters-receivable-bills?companyId={companyId}"

        payload = {}
        headers = {
            'Content-Type': 'application/json',
            'Authorization': self.password_marge_by_bills
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        if response.status_code == 200:
                    data = response.json()['data']

                    df = pd.json_normalize(data)
                    
                    ### EXPORTAR PARA O BUCKET
                    S3().overwrite_dataframes_bucket_bruto(df, nm_arq)

                    # df.to_csv('00_bronze-local/marge_defaulters_receivable_bills.csv', sep=';', index=False) # DADOS BRUTOS NAO SAO TRADOS
                    # df.to_parquet('00_bronze-local/marge_defaulters_receivable_bills.parquet', engine='fastparquet', index=False) # DADOS BRUTOS NAO SAO TRADOS
        else:
             raise Exception(f'Erro - {nm_arq} - response status: {response.status_code}')
    
    def marge_income_bruto(self):
        # NOME DO ARQUIVO
        nm_arq = 'invoice-itens'

        ### RESQUEST DA API
        api_url = "https://api.sienge.com.br/labutare/public/api/bulk-data/v1/invoice-itens"

        df_now = format(dt.now(), '%Y-%m-%d')

        params = {
            "companyId":"1",
            "startDate": "2021-01-01",
            "endDate": f"{df_now}"

        }

        headers = {
            "Content-Type": "application/json"
        }

        response = requests.get(api_url, auth=(self.username_marge, self.password_marge), headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()['data']

            df = pd.json_normalize(data)
            
            ### EXPORTAR PARA O BUCKET
            S3().overwrite_dataframes_bucket_bruto(df, nm_arq)
            
            # df.to_csv('00_bronze-local/marge_income.csv', sep=';', index=False) # DADOS BRUTOS NAO SAO TRADOS
            # df.to_parquet('00_bronze-local/marge_income.parquet', engine='fastparquet', index=False) # DADOS BRUTOS NAO SAO TRADOS
        else:
             raise Exception(f'Erro - {nm_arq} - response status: {response.status_code}')
    
    def titulos_inadimplentes(self):
        nm_path = 'od17titulos_inadimplentes'
        # NOME DO ARQUIVO
        nm_arq = 'od17titulos_inadimplentes'

        url = "https://api.sienge.com.br/labutare/public/api/bulk-data/v1//defaulters-receivable-bills?companyId=1&startDate=2024-10-01&endDate=2025-01-31"

        payload = {}
        headers = {
             'Accept': 'application/json',
             'Content-Type': 'application/json',
             'Authorization': 'Basic bGFidXRhcmUtYmk6dG9ZcUNad2VGVHpYOEFpa2NQQXU0SXNwU3hyc1p0ZE4=',
  'Cookie': 'JSESSIONID=4B065D389121F9EB682DBCFB4F674019'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        
        if response.status_code == 200:
             data = response.json()['data']

             df = pd.json_normalize(data)
             
             ### EXPORTAR PARA O BUCKET
             S3().overwrite_dataframes_bucket_bruto(df, nm_arq)
        else:
            raise Exception(f'Erro - {nm_arq} - response status: {response.status_code}')
 
    def titulos_inadimplentes2(self):
        nm_path = 'od17titulos_inadimplentes2'
        # NOME DO ARQUIVO
        nm_arq = 'od17titulos_inadimplentes2'

        url = "https://api.sienge.com.br/labutare/public/api/bulk-data/v1/defaulters-receivable-bills/by-aging?companyId=1&startDate=2024-10-01&endDate=2025-01-31"

        payload = {}
        headers = {
             'Accept': 'application/json',
             'Content-Type': 'application/json',
             'Authorization': 'Basic bGFidXRhcmUtYmk6dG9ZcUNad2VGVHpYOEFpa2NQQXU0SXNwU3hyc1p0ZE4=',
  'Cookie': 'JSESSIONID=4B065D389121F9EB682DBCFB4F674019'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        
        if response.status_code == 200:
             data = response.json()['data']

             df = pd.json_normalize(data)
             
             ### EXPORTAR PARA O BUCKET
             S3().overwrite_dataframes_bucket_bruto(df, nm_arq)
        else:
            raise Exception(f'Erro - {nm_arq} - response status: {response.status_code}')
 






