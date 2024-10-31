#!/usr/bin/env python
# coding: utf-8

# In[13]:


import pandas as pd

def patient_subtype_division(csv_path: str) -> (list, list, list, list, list):
    """
    Divide os pacientes de acordo com os subtipos de câncer gástrico presentes no arquivo CSV.

    Esta função lê um arquivo CSV contendo dados de pacientes com câncer gástrico e classifica os pacientes 
    em cinco subtipos diferentes: STAD_CIN, STAD_EBV, STAD_GS, STAD_MSI, e STAD_POLE. Para cada subtipo, a função
    cria uma lista contendo os IDs dos pacientes pertencentes àquele subtipo.

    Parâmetros:
    -----------
    csv_path : str
        Caminho para o arquivo CSV contendo os dados dos pacientes. O arquivo deve ter colunas 'PATIENT ID' e 'Subtype'.

    Retorna:
    --------
    tuple:
        Retorna cinco listas, uma para cada subtipo:
        - CIN_list: Lista de IDs dos pacientes com o subtipo STAD_CIN.
        - EBV_list: Lista de IDs dos pacientes com o subtipo STAD_EBV.
        - GS_list: Lista de IDs dos pacientes com o subtipo STAD_GS.
        - MSI_list: Lista de IDs dos pacientes com o subtipo STAD_MSI.
        - POLE_list: Lista de IDs dos pacientes com o subtipo STAD_POLE.
    """
    
    # Carrega os dados do arquivo CSV
    STAD_data = pd.read_csv(csv_path)
    
    # Remove as linhas onde o subtipo não está definido (valores NaN)
    df_subtype = STAD_data.dropna(subset=['Subtype'])
    
    # Filtra os pacientes do subtipo 'STAD_CIN' e armazena seus IDs em uma lista
    CIN_list = list(df_subtype[df_subtype['Subtype'] == 'STAD_CIN']['PATIENT ID'])
    print(f'Quantidade de casos CIN: {len(CIN_list)}')
    
    # Filtra os pacientes do subtipo 'STAD_EBV' e armazena seus IDs em uma lista
    EBV_list = list(df_subtype[df_subtype['Subtype'] == 'STAD_EBV']['PATIENT ID'])
    print(f'Quantidade de casos EBV: {len(EBV_list)}')
    
    # Filtra os pacientes do subtipo 'STAD_GS' e armazena seus IDs em uma lista
    GS_list = list(df_subtype[df_subtype['Subtype'] == 'STAD_GS']['PATIENT ID'])
    print(f'Quantidade de casos GS: {len(GS_list)}')
    
    # Filtra os pacientes do subtipo 'STAD_MSI' e armazena seus IDs em uma lista
    MSI_list = list(df_subtype[df_subtype['Subtype'] == 'STAD_MSI']['PATIENT ID'])
    print(f'Quantidade de casos MSI: {len(MSI_list)}')
    
    # Filtra os pacientes do subtipo 'STAD_POLE' e armazena seus IDs em uma lista
    POLE_list = list(df_subtype[df_subtype['Subtype'] == 'STAD_POLE']['PATIENT ID'])
    print(f'Quantidade de casos POLE: {len(POLE_list)}')
    
    # Retorna as listas com os IDs dos pacientes de cada subtipo
    return CIN_list, EBV_list, GS_list, MSI_list, POLE_list

def patient_subtype_binary(csv_path: str, subtype: str) -> (list, list):
    """
    Divide os pacientes em dois grupos: aqueles pertencentes a um subtipo específico e os demais.

    Esta função lê um arquivo CSV contendo dados de pacientes com câncer gástrico e classifica os pacientes
    em dois grupos: os pacientes que pertencem ao subtipo especificado e todos os outros pacientes.

    Parâmetros:
    -----------
    csv_path : str
        Caminho para o arquivo CSV contendo os dados dos pacientes. O arquivo deve ter colunas 'PATIENT ID' e 'Subtype'.
    
    subtype : str
        O subtipo de câncer gástrico que será usado como alvo. O subtipo deve ser fornecido sem o prefixo 'STAD_', 
        por exemplo, se o subtipo for 'CIN', deve-se passar 'CIN' como argumento.

    Retorna:
    --------
    tuple:
        Retorna duas listas:
        - target_subtype_list: Lista de IDs dos pacientes pertencentes ao subtipo alvo (por exemplo, 'STAD_CIN').
        - others_list: Lista de IDs dos pacientes pertencentes a outros subtipos.
    """
    
    # Carrega os dados do arquivo CSV
    STAD_data = pd.read_csv(csv_path)
    
    # Remove as linhas onde o subtipo não está definido (valores NaN)
    df_subtype = STAD_data.dropna(subset=['Subtype'])
    
    # Filtra os pacientes do subtipo alvo e armazena seus IDs em uma lista
    target_subtype_list = list(df_subtype[df_subtype['Subtype'] == f'STAD_{subtype}']['PATIENT ID'])
    print(f'Quantidade de casos alvo: {len(target_subtype_list)}')
    
    # Filtra os pacientes de outros subtipos e armazena seus IDs em uma lista
    others_list = list(df_subtype[df_subtype['Subtype'] != f'STAD_{subtype}']['PATIENT ID'])
    print(f'Quantidade de outros casos: {len(others_list)}')
    
    # Retorna as listas com os IDs dos pacientes do subtipo alvo e dos outros subtipos
    return target_subtype_list, others_list

