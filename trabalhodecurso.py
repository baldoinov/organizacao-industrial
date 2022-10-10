import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import requests
import os
from pathlib import Path

from brfinance import CVMAsyncBackend
from datetime import datetime, date
from unidecode import unidecode
from bs4 import BeautifulSoup

def download_docs(start_date: date, end_date: date, cvm_codes: list, category: list, report: int) -> pd.DataFrame:

    cvm_client = CVMAsyncBackend()
    
    docs: list = ['Balanço Patrimonial Ativo',
                  'Balanço Patrimonial Passivo',
                  'Demonstração do Resultado Abrangente', 
                  'Demonstração do Fluxo de Caixa'][report]
    # 'Demonstração das Mutações do Patrimônio Líquido'

    search = cvm_client.get_consulta_externa_cvm_results(
        cod_cvm=cvm_codes,
        start_date=start_date,
        end_date=end_date,
        last_ref_date=False,
        category=category
        )

    search = search.loc[search['categoria'] == 'DFP - Demonstrações Financeiras Padronizadas']
    search = search.loc[pd.to_numeric(search['numero_seq_documento'], errors='coerce').notnull()]
    search = search.sort_values(by='version', ascending=False)
    search = search.drop_duplicates(subset=['ref_date', 'empresa'], keep='first')

    df_consolidado = None

    for index, row in search.iterrows():

        reports = cvm_client.get_report(row["numero_seq_documento"], row["codigo_tipo_instituicao"], reports_list=[docs])
        
        for i in reports:
            
            df = reports[docs]
            df['Descrição'] = list(map(unidecode, df['Descrição'].str.lower()))
            df = df.set_index('Descrição')
            df = df.dropna(axis=0, subset='Valor')
            df = df.groupby(by=['Descrição'], axis=0).sum()
            df = df.transpose()
            
            df['Ano'] = row['ref_date']
            df['Empresa'] = row['empresa']
            
            df = df.reset_index(drop=True)

            df_consolidado = pd.concat([df_consolidado, df], axis=0, ignore_index=True)


def et_demonstrativos(path: Path) -> pd.DataFrame:

    files = os.listdir(path)
    df_consolidado = None

    for file in files:
        df = pd.read_excel(path / file)
        
        nome = df.columns[0]
        indx = list(map(lambda x: str(x).strip().lower(), list(df[nome])))
        
        df = df.set_index(nome)
        df.index = indx
        
        df = df.loc[['consolidado', 'ativo total', 'ativo circulante', 'ativo nao circulante', 
                     'imobilizado', 'passivo e patrimonio liq', 'passivo circulante', 'passivo nao circulante', 
                     'patrim liq consolidado', 'lucros acumulados', '+receita liquida operac', '-custo produtos vendidos', 
                     '=lucro bruto', '+despesas com vendas', '=lucro liquido', '+receita bruta']]
        
        df = df[~df.index.duplicated(keep='first')]
        df.columns = df.loc['consolidado'].astype(str).str.slice(start=0, stop=10)
        df = df.drop('consolidado')
        
        df = df.T
        df['empresa'] = nome
        df = df.reset_index()

        df_consolidado = pd.concat([df_consolidado, df]).reset_index(drop=True)
    
    return df_consolidado


def et_dados_mercado(path: Path) -> pd.DataFrame:

    files = os.listdir(path)
    df_consolidado = None

    for file in files:
        df = pd.read_excel(path / file, skiprows=[1, 2])
        
        nome = df.columns[0]
        indx = list(map(lambda x: str(x).strip().lower(), list(df[nome])))
        
        df = df.set_index(nome)
        df.index = indx
        
        df.columns = df.loc['consolidado'].astype(str).str.slice(start=0, stop=10)
        df = df.drop('consolidado')
        
        df = df.T
        df['empresa'] = nome
        df = df.reset_index()

        df_consolidado = pd.concat([df_consolidado, df]).reset_index(drop=True)
    
    return df_consolidado


def et_financeiros(path: Path) -> pd.DataFrame:

    files = os.listdir(path)
    df_consolidado = None

    for file in files:
        df = pd.read_excel(path / file, skiprows=[1, 2])
        
        nome = df.columns[0]
        indx = list(map(lambda x: str(x).strip().lower(), list(df[nome])))
        
        df = df.set_index(nome)
        df.index = indx
        
        df.columns = df.loc['consolidado'].astype(str).str.slice(start=0, stop=10)
        df = df.drop('consolidado')
        
        df = df.T
        df['empresa'] = nome
        df = df.reset_index()

        df_consolidado = pd.concat([df_consolidado, df]).reset_index(drop=True)
    
    return df_consolidado


def et_acionistas():

    return None

    def et_acionistas_01(path: Path) -> pd.DataFrame:

        files = os.listdir(path)
        df_consolidado = None

        for file in files:
            df = pd.read_excel(path / file)
            
            nome = df.columns[1]
            indx = ['nomes'] + list(map(lambda x: str(x).strip().lower(), list(df.iloc[:, 2])))
            
            df = df.set_index(2)
            df.index = indx
            
            df.columns = df.loc['nomes'].astype(str).str.slice(start=0, stop=10)
            df = df.drop('consolidado')
            
            df = df.T
            df['empresa'] = nome
            df = df.reset_index()

            df_consolidado = pd.concat([df_consolidado, df]).reset_index(drop=True)
        
        return df_consolidado

    def et_acionistas_02(path: Path) -> pd.DataFrame:

        files = os.listdir(path)
        df_consolidado = None

        for file in files:
            df = pd.read_excel(path / file)
            
            nome = df.columns[0]
            indx = list(map(lambda x: str(x).strip().lower(), list(df[nome])))
            
            df = df.set_index(nome)
            df.index = indx
            
            df.columns = df.loc['consolidado'].astype(str).str.slice(start=0, stop=10)
            
            df = df.T
            df['empresa'] = nome
            df = df.reset_index()

            df_consolidado = pd.concat([df_consolidado, df]).reset_index(drop=True)
        
        return df_consolidado

    tipo1 = ['dados/dados-acionistas/cea_composição_acionistas.xlsx',
        'dados/dados-acionistas/riachuelo_composicao_acionistas.xlsx',
        'dados/dados-acionistas/marisa_composicao_acionistas.xlsx']

    tipo2 = ['dados/dados-acionistas/arezzo_composicao_acionistas.xlsx',
            'dados/dados-acionistas/renner_composicao_acionistas.xlsx',
            'dados/dados-acionistas/soma_composicao_acionistas.xlsx',
            'dados/dados-acionistas/le_lis_blanc_composicao_acionistas.xlsx']



if __name__ == '__main__':
    
    # Arquivos da CVM
    cvm_reference_codes : dict = CVMAsyncBackend().get_cvm_codes()
    start_date : date = date(2010, 1, 1)
    end_date   : date = date.today()
    cvm_codes  : list = ['022055', '024848', '004669', '021440', '008133', '025011', '022349']
    category   : list = ["EST_4"]

    #df_consolidado = download_docs(start_date, end_date, cvm_codes, category, report=0)
    #df_consolidado.to_excel('dados/balanco_patrimonial_ativo.xlsx')

    # Consolidacao arquivos do Economatica

    path = Path('dados/demonstrativos/')
    df = et_demonstrativos(path)
    df.to_excel('dados/demonstrativos.xlsx', index=False)


    #path = Path('dados/dados-mercado/')
    #df = et_dados_mercado(path)
    #df.to_excel('dados/indicadores-mercado.xlsx', index=False)

    
    #path = Path('dados/indicadores-financeiros/')
    #df = et_financeiros(path)
    #df.to_excel('dados/indicadores-financeiros.xlsx', index=False)