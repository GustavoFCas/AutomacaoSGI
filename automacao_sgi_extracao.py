from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
import time
import os
import pandas as pd
from sqlalchemy import create_engine
import schedule
from datetime import datetime

def run_script():
    servico = Service(ChromeDriverManager().install())
    navegador = webdriver.Chrome(service=servico)

    try:
        navegador.get('https://sgi.slu.df.gov.br/login')
        navegador.find_element('xpath', '//*[@id="wrapper"]/div[2]/div/div/div/div[2]/div[2]/form/div[1]/div/input').send_keys('SGI_USERNAME')
        navegador.find_element('xpath', '//*[@id="wrapper"]/div[2]/div/div/div/div[2]/div[2]/form/div[2]/div/input').send_keys('SGI_SENHA')
        navegador.find_element('xpath', '//*[@id="wrapper"]/div[2]/div/div/div/div[2]/div[2]/form/div[4]/div[1]/button').click()
        time.sleep(5)

        navegador.find_element('xpath', '//*[@id="wrapper"]/div[2]/div[2]/div[1]/div/div[3]/div/div/a/button').click()
        time.sleep(5)
        navegador.find_element('xpath', '//*[@id="wrapper"]/div[2]/div[2]/div[2]/div[4]/a/button').click()
        time.sleep(1)
        navegador.find_element('xpath', '//*[@id="wrapper"]/div[2]/div[1]/div/span[2]/button[2]').click()
        time.sleep(1)
        navegador.find_element('xpath', '//*[@id="pesagem-filtro"]/div[1]/div/div[3]/div/button/span[1]').click()
        time.sleep(1)
        navegador.find_element('xpath', '//*[@id="pesagem-filtro"]/div[1]/div/div[3]/div/div/div/input').send_keys('SUMA BRASIL - SERVIÇOS URBANOS E MEIO AMBIENTE', Keys.ENTER)
        time.sleep(1)
        navegador.find_element('xpath', '//*[@id="pesagem-filtro"]/div[1]/div/div[8]/div/input[1]').send_keys('01072024')
        time.sleep(5)
        navegador.find_element('xpath', '//*[@id="pesagem-filtro"]/div[3]/button').click()
        time.sleep(10)
        navegador.find_element('xpath', '//*[@id="wrapper"]/div[2]/div[1]/div/span[2]/button[1]').click()
        time.sleep(15)

        navegador.quit()

        diretorio = 'C:/Users/freit/Downloads'
        arquivos = os.listdir(diretorio)
        arquivos_excel = [arquivo for arquivo in arquivos if arquivo.endswith('.xlsx')]

        if arquivos_excel:
            arquivo_mais_recente = max(arquivos_excel, key=lambda arquivo: os.path.getmtime(os.path.join(diretorio, arquivo)))
            caminho_arquivo = os.path.join(diretorio, arquivo_mais_recente)
            df = pd.read_excel(caminho_arquivo)

            df.rename(columns={
                'Tíquete': 'tiquete',
                'Alterado': 'alterado',
                'Núcleo': 'nucleo',
                'Empresa': 'empresa',
                'Veículo': 'veiculo',
                'Operação': 'operacao',
                'Peso de Entrada': 'peso_entrada',
                'Peso de Saída': 'peso_saida',
                'Peso Líquido': 'peso_liquido',
                'Data Entrada': 'data_entrada',
                'Data Saída': 'data_saida',
                'Data/Hora de Entrada': 'hora_entrada',
                'Data/Hora de Saída': 'hora_saida',
                'Situação': 'situacao',
                'Origem/Destino': 'origem_destino',
                'Produto': 'produto',
                'Observação': 'observacao'
            }, inplace=True)

            df_finalizado = df[df['situacao'] == 'finalizado']

            if not df_finalizado.empty:
                df_finalizado['data_entrada'] = pd.to_datetime(df_finalizado['data_entrada'], dayfirst=True).dt.date
                df_finalizado['data_saida'] = pd.to_datetime(df_finalizado['data_saida'], dayfirst=True).dt.date
                df_finalizado['hora_entrada'] = pd.to_datetime(df_finalizado['hora_entrada'], dayfirst=True)
                df_finalizado['hora_saida'] = pd.to_datetime(df_finalizado['hora_saida'], dayfirst=True)

                endpoint = 'DB_ENDPOINT'
                port = 'DB_PORTA'
                name = 'DB_NOME'
                user = 'DB_USER'
                password = 'DB_SENHA'

                url = f'postgresql://{user}:{password}@{endpoint}:{port}/{name}'
                engine = create_engine(url)

                nome_tabela = 'dados_sgi'

                with engine.connect() as connection:
                    existing_data = pd.read_sql(f'SELECT "tiquete" FROM {nome_tabela}', con=connection)
                    existing_tiquetes = existing_data['tiquete'].tolist()

                new_data_finalizado = df_finalizado[~df_finalizado["tiquete"].isin(existing_tiquetes)]

                if not new_data_finalizado.empty:
                    with engine.connect() as connection:
                        new_data_finalizado.to_sql(nome_tabela, con=connection, if_exists='append', index=False)
                        print("Novos dados 'finalizados' inseridos com sucesso.")
                else:
                    print("Não há novos dados 'finalizados' para inserir.")
            else:
                print("Não há dados 'finalizados' para inserir.")
        else:
            print("Nenhum arquivo Excel encontrado no diretório.")

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        navegador.quit()

schedule.every(15).minutes.do(run_script)

while True:
    schedule.run_pending()
    time.sleep(1)
