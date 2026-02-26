
import pandas as pd  # type: ignore

caminho_dados_brutos = r'C:\Users\gabri\Desktop\Censo\microdados_censo_escolar_2024\dados\microdados_ed_basica_2024.csv'

print("Iniciando a extração dos dados...")
try:

    df_bruto = pd.read_csv(caminho_dados_brutos, encoding='utf-8', sep=';', low_memory=False)
    print("Dados extraídos com sucesso usando UTF-8!")

except UnicodeDecodeError:

    print("Falha ao ler com UTF-8. Tentando com Latin-1...")
    df_bruto = pd.read_csv(caminho_dados_brutos, encoding='latin-1', sep=';', low_memory=False)
    print("Dados extraídos com sucesso usando Latin-1!")

except FileNotFoundError:
    print(f"ERRO: O arquivo não foi encontrado no caminho: {caminho_dados_brutos}")
    exit() 
except Exception as e:
    print(f"Ocorreu um erro inesperado durante a leitura do arquivo: {e}")
    exit() 

print(f"O arquivo original contém {df_bruto.shape[0]} linhas e {df_bruto.shape[1]} colunas.")

print("\nIniciando a fase de Transformação...")


colunas_selecionadas = [
    'NU_ANO_CENSO', 'NO_ENTIDADE', 'CO_UF', 'NO_UF', 'CO_MUNICIPIO', 'NO_MUNICIPIO',
    'TP_DEPENDENCIA', 'TP_LOCALIZACAO', 'IN_LABORATORIO_INFORMATICA', 'IN_BIBLIOTECA',
    'IN_QUADRA_ESPORTES', 'IN_INTERNET', 'IN_BANDA_LARGA', 'QT_SALAS_UTILIZADAS',
    'QT_DESKTOP_ALUNO', 'QT_COMP_PORTATIL_ALUNO', 'QT_TABLET_ALUNO'
]
df_selecionado = df_bruto[colunas_selecionadas].copy()
print("-> Colunas selecionadas.")


mapa_nomes = {
    'NU_ANO_CENSO': 'ano_censo', 'NO_ENTIDADE': 'nome_escola', 'CO_UF': 'codigo_uf',
    'NO_UF': 'estado', 'CO_MUNICIPIO': 'codigo_municipio', 'NO_MUNICIPIO': 'municipio',
    'TP_DEPENDENCIA': 'dependencia_adm', 'TP_LOCALIZACAO': 'localizacao',
    'IN_LABORATORIO_INFORMATICA': 'possui_lab_informatica', 'IN_BIBLIOTECA': 'possui_biblioteca',
    'IN_QUADRA_ESPORTES': 'possui_quadra_esportes', 'IN_INTERNET': 'possui_internet',
    'IN_BANDA_LARGA': 'possui_banda_larga', 'QT_SALAS_UTILIZADAS': 'qtd_salas_utilizadas',
    'QT_DESKTOP_ALUNO': 'qtd_desktops_alunos',
    'QT_COMP_PORTATIL_ALUNO': 'qtd_notebooks_alunos',
    'QT_TABLET_ALUNO': 'qtd_tablets_alunos'
}
df_renomeado = df_selecionado.rename(columns=mapa_nomes)
print("-> Colunas renomeadas.")


df_tratado = df_renomeado.copy()

mapa_dependencia = {1: 'Federal', 2: 'Estadual', 3: 'Municipal', 4: 'Privada'}
mapa_localizacao = {1: 'Urbana', 2: 'Rural'}
mapa_booleano = {0: 'Não', 1: 'Sim'}

df_tratado['dependencia_adm'] = df_tratado['dependencia_adm'].map(mapa_dependencia)
df_tratado['localizacao'] = df_tratado['localizacao'].map(mapa_localizacao)
df_tratado['possui_lab_informatica'] = df_tratado['possui_lab_informatica'].map(mapa_booleano)
df_tratado['possui_biblioteca'] = df_tratado['possui_biblioteca'].map(mapa_booleano)
df_tratado['possui_quadra_esportes'] = df_tratado['possui_quadra_esportes'].map(mapa_booleano)
df_tratado['possui_internet'] = df_tratado['possui_internet'].map(mapa_booleano)
df_tratado['possui_banda_larga'] = df_tratado['possui_banda_larga'].map(mapa_booleano)

df_tratado['qtd_salas_utilizadas'] = df_tratado['qtd_salas_utilizadas'].fillna(0)
df_tratado['qtd_desktops_alunos'] = df_tratado['qtd_desktops_alunos'].fillna(0)
df_tratado['qtd_notebooks_alunos'] = df_tratado['qtd_notebooks_alunos'].fillna(0)
df_tratado['qtd_tablets_alunos'] = df_tratado['qtd_tablets_alunos'].fillna(0)

df_tratado['qtd_total_computadores_alunos'] = (df_tratado['qtd_desktops_alunos'] + 
                                               df_tratado['qtd_notebooks_alunos'] + 
                                               df_tratado['qtd_tablets_alunos'])

df_tratado.fillna('Não informado', inplace=True)
print("-> Dados limpos e tratados.")

print("\nTransformação concluída! Amostra dos dados finais:")
print(df_tratado.head())


caminho_dados_tratados = r'C:\Users\gabri\Desktop\dados_censo_escolar_tratados.csv'

print(f"\nSalvando o arquivo tratado em: {caminho_dados_tratados}")
try:

    df_tratado.to_csv(caminho_dados_tratados, index=False, encoding='utf-8')
    print("PROCESSO CONCLUÍDO! Arquivo final salvo com sucesso!")

except Exception as e:
    print(f"Ocorreu um erro ao salvar o arquivo: {e}")