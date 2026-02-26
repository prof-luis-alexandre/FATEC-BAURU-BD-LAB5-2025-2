import pandas as pd
from sqlalchemy import create_engine
import os 
import warnings 

warnings.filterwarnings('ignore', category=FutureWarning) 

DATABASE_URL = 'postgresql+psycopg2://postgres:admin@localhost:5432/DATA_MART_MATRICULAS' 
CAMINHO_RAIZ = r'C:\Users\Pichau\Desktop\patricia_etl' 

ARQUIVO_MATRICULAS = os.path.join(CAMINHO_RAIZ, 'matriculas_inep2019a2023.csv') 
ARQUIVO_IBGE = os.path.join(CAMINHO_RAIZ, 'RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.xlsx') 
ARQUIVO_IES = os.path.join(CAMINHO_RAIZ, 'ies_inep2019a2023.csv') 

engine = create_engine(DATABASE_URL)

def main():
    print(f"1. Lendo e configurando os arquivos...")
    
    try:
        df_ibge = pd.read_excel(ARQUIVO_IBGE, dtype='str', header=0)
        df_ibge.columns = df_ibge.columns.str.strip() 
    except FileNotFoundError:
        print(f"ERRO: Arquivo IBGE não encontrado em: {ARQUIVO_IBGE}")
        return
    
    try:
        df_ies = pd.read_csv(ARQUIVO_IES, sep=';', dtype='str', encoding='utf-8', encoding_errors='ignore')
    except FileNotFoundError:
        print(f"ERRO: Arquivo IES não encontrado em: {ARQUIVO_IES}")
        return

    try:
        df_matriculas = pd.read_csv(ARQUIVO_MATRICULAS, sep=';', dtype='str', encoding='utf-8', encoding_errors='ignore')
        print(f"Total de registros de Matrículas lidos: {len(df_matriculas)}")
    except FileNotFoundError:
        print(f"ERRO: Arquivo de Matrículas não encontrado em: {ARQUIVO_MATRICULAS}")
        return
        
    print("\n2. Processando e Carregando Dimensões...")

    try:
        df_dim_municipio = df_ibge[
            df_ibge['UF'] == '35' 
        ][['Código Município Completo', 'Nome_Município']].rename(columns={
            'Código Município Completo': 'co_municipio', 
            'Nome_Município': 'no_municipio'
        }).drop_duplicates(subset=['co_municipio'])
        
        with engine.begin() as conn:
            df_dim_municipio.to_sql('dim_municipio', conn, schema='public', if_exists='replace', index=False)
            print(f"   -> dim_municipio OK: {len(df_dim_municipio)} municípios de SP carregados.")
    except KeyError as e:
        print(f"ERRO: Coluna do IBGE não encontrada: {e}. Verifique nomes.")
        return

    df_dim_ies = df_ies.rename(columns={
        'co_ies': 'co_ies', 
        'no_ies': 'no_ies'
    })[['co_ies', 'no_ies']].drop_duplicates(subset=['co_ies'])
    
    with engine.begin() as conn:
        df_dim_ies.to_sql('dim_ies', conn, schema='public', if_exists='replace', index=False)
        print(f"   -> dim_ies OK: {len(df_dim_ies)} IES carregadas.")
    
    print("\n3. Processando e Carregando FATO_MATRICULAS...")

    df_fato = df_matriculas.rename(columns={
        'nu_ano_censo': 'ano', 
        'n_matricula': 'qt_matricula',
        'co_ies': 'fk_co_ies', 
        'tp_modalidade_ensino': 'modalidade', 
        'grau_academico': 'grau_academico', 
        'tp_rede': 'rede',
        'cod_ibge': 'fk_co_municipio',
        'sexo': 'sexo' 
    })[['ano', 'qt_matricula', 'fk_co_ies', 'modalidade', 'grau_academico', 'rede', 'fk_co_municipio', 'sexo']]
    
    df_fato_sp = pd.merge(
        df_fato, 
        df_dim_municipio[['co_municipio']], 
        left_on='fk_co_municipio', 
        right_on='co_municipio', 
        how='inner'
    )
    df_fato_sp = df_fato_sp.drop(columns=['co_municipio']) 
    print(f"Registros de SP (após filtro): {len(df_fato_sp)}")
    
    df_fato_sp['qt_matricula'] = pd.to_numeric(df_fato_sp['qt_matricula'], errors='coerce')
    df_fato_sp = df_fato_sp.dropna(subset=['qt_matricula', 'fk_co_municipio'])
    
    try:
        with engine.begin() as conn:
            df_fato_sp.to_sql('fato_matriculas', conn, schema='public', if_exists='replace', index=False)
            print(f"   -> fato_matriculas OK: {len(df_fato_sp)} linhas carregadas.")

    except Exception as e:
        print(f"\n!!! ERRO CRITICO NA CARGA DA FATO: {e}")
        return

    print("\nCARGA ETL CONCLUIDA COM SUCESSO (3 TABELAS CARREGADAS).")

if __name__ == '__main__':
    main()