'''
==========================================================================================
ETL e Datamart AIVI
Planejamento e projeto: Fabrício Pinheiro Souza/ Analista Sênior/ Vibra Energia S.A.
Data revisão: 24/01/2024
==========================================================================================
'''

import pandas as pd
import os
from datetime import datetime
from pathlib import Path

# Definições de PATH
INPATH = r'C:\Users\fpsou\OneDrive - VIBRA\NCMV - Indicador\Dados'
OUTPATH1 = r'C:\Users\fpsou\OneDrive - VIBRA\NCMV - Indicador\BI-StageArea\AIVI\csv'
OUTPATH2 = r'C:\Users\fpsou\OneDrive - VIBRA\NCMV - Indicador\BI-StageArea\AIVI\txt'
OUTPATH3 = r'C:\Users\fpsou\OneDrive - VIBRA\NCMV - Indicador\BI-StageArea\AIVI\xlsx'
OUTPATH = OUTPATH1
caminho_arquivos_excel = INPATH
caminho_arquivo_csv = os.path.join(OUTPATH, 'df_aivi.csv')

# Função para limpar e preparar o dataframe
def limpar_e_preparar_dataframe(df, nome_arquivo):
    df = df.dropna(how='all', axis=0)
    df = df.dropna(how='all', axis=1)
    df.columns = df.iloc[0]
    df = df[1:]
    df = df.drop_duplicates()
    df.loc[:, 'Nome_Arquivo'] = nome_arquivo
    df.loc[:, 'Data_Adicao'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return df

# Função para ler e concatenar arquivos
def ler_e_concatenar(arquivos, caminho):
    lista_dfs = []
    for arquivo in arquivos:
        caminho_completo = os.path.join(caminho, arquivo)
        df = pd.read_excel(caminho_completo, thousands='.', decimal=',')
        df_limpo = limpar_e_preparar_dataframe(df, arquivo)
        lista_dfs.append(df_limpo)
    return pd.concat(lista_dfs, ignore_index=True)

# Listar os arquivos Excel e concatenar dados
arquivos_excel = [arquivo for arquivo in os.listdir(INPATH) if arquivo.endswith('.xlsx')]
df_existente = pd.DataFrame()
for arquivo in arquivos_excel:
    df_novo = ler_e_concatenar([arquivo], caminho_arquivos_excel)
    df_existente = pd.concat([df_existente, df_novo], ignore_index=True)

# Removendo duplicatas no df_existente
df_existente = df_existente.drop_duplicates()

# Função para carregar ou criar índice
def carregar_ou_criar_indice(nome_dimensao, df_existente=None):
    caminho_indice = os.path.join(OUTPATH, f'indice_{nome_dimensao}.csv')
    if os.path.exists(caminho_indice):
        with open(caminho_indice, 'r') as file:
            ultimo_indice = int(file.read().strip())
    else:
        ultimo_indice = 0

    if df_existente is not None and not df_existente.empty:
        if nome_dimensao + 'ID' in df_existente.columns:
            maior_indice_existente = df_existente[nome_dimensao + 'ID'].max()
            novo_indice_inicio = maior_indice_existente + 1
        else:
            novo_indice_inicio = 1
    else:
        novo_indice_inicio = ultimo_indice + 1

    return novo_indice_inicio

# Função para atualizar índice
def atualizar_indice(nome_dimensao, ultimo_indice):
    caminho_indice = os.path.join(OUTPATH, f'indice_{nome_dimensao}.csv')
    with open(caminho_indice, 'w') as file:
        file.write(str(ultimo_indice))

# Função para salvar DataFrames em CSV, TXT e XLSX
def salvar_dataframe(df, nome_arquivo_base, nome_dimensao=None, with_index=False):
    if with_index:
        indice = carregar_ou_criar_indice(nome_dimensao)
        df[nome_dimensao + 'ID'] = range(indice, indice + len(df))
        atualizar_indice(nome_dimensao, df[nome_dimensao + 'ID'].iloc[-1])

    df.to_csv(os.path.join(OUTPATH1, nome_arquivo_base + '.csv'), index=False, sep=';', encoding='utf-8-sig')
    df.to_csv(os.path.join(OUTPATH2, nome_arquivo_base + '.txt'), index=False, sep=';', encoding='utf-8-sig')
    df.to_excel(os.path.join(OUTPATH3, nome_arquivo_base + '.xlsx'), index=False)

# Criar DimProduto
dim_produto = df_existente[['Cód Grupo de produto', 'Desc. Grupo de Produto']].drop_duplicates()
indice_produto = carregar_ou_criar_indice('Produto', dim_produto)
dim_produto['ProdutoID'] = list(range(indice_produto, indice_produto + len(dim_produto)))
atualizar_indice('Produto', dim_produto['ProdutoID'].iloc[-1])
salvar_dataframe(dim_produto, 'DimProduto')
df_existente = df_existente.merge(dim_produto, on=['Cód Grupo de produto', 'Desc. Grupo de Produto'], how='left')

# DimCentro
dim_centro = df_existente[['Centro', 'Nome']].drop_duplicates()
indice_centro = carregar_ou_criar_indice('Centro', dim_centro)
dim_centro['CentroID'] = list(range(indice_centro, indice_centro + len(dim_centro)))
atualizar_indice('Centro', dim_centro['CentroID'].max())
salvar_dataframe(dim_centro, 'DimCentro')
df_existente = df_existente.merge(dim_centro, on=['Centro', 'Nome'], how='left')

# Criar DimLimites (ajustado para criar 'LimiteID')
dim_limites = df_existente[['Nome do set', 'Cód Grupo de produto', 'Centro', 'Nome', 'Limite Inferior', 'Histórico', 'Limite Su']].drop_duplicates()
indice_limites = carregar_ou_criar_indice('Limites', dim_limites)
dim_limites['LimiteID'] = list(range(indice_limites, indice_limites + len(dim_limites)))
atualizar_indice('Limites', dim_limites['LimiteID'].max())
salvar_dataframe(dim_limites, 'DimLimites')
df_existente = df_existente.merge(dim_limites, on=['Nome do set', 'Cód Grupo de produto', 'Centro', 'Nome', 'Limite Inferior', 'Histórico', 'Limite Su'], how='left')

# DimStatus
dim_status = df_existente[['Status de Homologação', 'Desc  Status', 'Status']].drop_duplicates()
indice_status = carregar_ou_criar_indice('Status', dim_status)
dim_status['StatusID'] = list(range(indice_status, indice_status + len(dim_status)))
atualizar_indice('Status', dim_status['StatusID'].max())
salvar_dataframe(dim_status, 'DimStatus')
df_existente = df_existente.merge(dim_status, on=['Status de Homologação', 'Desc  Status', 'Status'], how='left')

# DimCompetencia
dim_competencia = df_existente[['Competência']].drop_duplicates()
indice_competencia = carregar_ou_criar_indice('Competencia', dim_competencia)
dim_competencia['CompetenciaID'] = list(range(indice_competencia, indice_competencia + len(dim_competencia)))
atualizar_indice('Competencia', dim_competencia['CompetenciaID'].max())
salvar_dataframe(dim_competencia, 'DimCompetencia')
df_existente = df_existente.merge(dim_competencia, on='Competência', how='left')

# DimTempo
anos = df_existente['Ano do documento do material'].unique()
meses = range(1, 13)
dim_tempo = pd.DataFrame([(ano, mes) for ano in anos for mes in meses], columns=['Ano', 'Mes'])
dim_tempo['AnoMes'] = dim_tempo['Ano'].astype(str) + dim_tempo['Mes'].astype(str).str.zfill(2)
indice_tempo = carregar_ou_criar_indice('Tempo', dim_tempo)
dim_tempo['TempoID'] = list(range(indice_tempo, indice_tempo + len(dim_tempo)))
atualizar_indice('Tempo', dim_tempo['TempoID'].max())
salvar_dataframe(dim_tempo, 'DimTempo')
df_existente['AnoMes'] = df_existente['Ano do documento do material'].astype(str) + df_existente['Mês do exercício'].astype(str).str.zfill(2)
df_existente = df_existente.merge(dim_tempo, on='AnoMes', how='left')

# Criar e salvar o Fato
fato = df_existente.drop(['Nome_Arquivo', 'Data_Adicao'], axis=1)
salvar_dataframe(fato, 'Fato', 'Fato', with_index=True)

# Verificar se o arquivo df_aivi.csv existe
if os.path.exists(caminho_arquivo_csv):
    # Carregar o dataframe existente
    df_aivi_existente = pd.read_csv(caminho_arquivo_csv, sep=';', encoding='utf-8-sig')
    # Concatenar com o novo dataframe e remover duplicatas
    df_aivi = pd.concat([df_aivi_existente, df_existente], ignore_index=True).drop_duplicates()
else:
    # Se não existir, usar o dataframe atual como df_aivi
    df_aivi = df_existente

# Salvar o dataframe final como df_aivi.csv
df_aivi.to_csv(caminho_arquivo_csv, index=False, sep=';', encoding='utf-8-sig')

if not os.path.exists(os.path.join(OUTPATH2, 'df_aivi.txt')):
    df_aivi.to_csv(os.path.join(OUTPATH2, 'df_aivi.txt'), index=False, sep=';', encoding='utf-8-sig')

if not os.path.exists(os.path.join(OUTPATH3, 'df_aivi.xlsx')):
    df_aivi.to_excel(os.path.join(OUTPATH3, 'df_aivi.xlsx'), index=False)

print("Processo concluído com sucesso. DataFrames criados e salvos.")