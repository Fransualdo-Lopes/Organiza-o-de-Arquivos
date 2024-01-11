import os
import shutil
import pandas as pd
from tqdm import tqdm

# Função para carregar dados de um arquivo CSV
def carregar_dados(nome_arquivo, dtype=None):
    return pd.read_csv(nome_arquivo, dtype=dtype)

# Especificando o tipo de dados para todas as colunas como string
dtype = {str(i): str for i in range(10)}  # Substitua 'total_colunas' pelo número total de colunas
arquivos = carregar_dados('arquivos.csv', dtype=dtype) # Em arquivos.csv tem 10 colunas.

# Função para organizar os arquivos em pastas por cliente, contrato, plano e atendimento
#def organizar_arquivos(clientes, contratos, atendimentos, arquivos, pasta_origem, pasta_destino, log):
    # Iterar sobre os contratos
    for _, contrato in tqdm(contratos.iterrows(), total=len(contratos), desc="Contratos Processados"):
        cliente_id = contrato['Id_cli']
        contrato_numero = contrato['Numero_contrat']
        plano = contrato['Plano']

        # Filtrar atendimentos para o cliente e contrato
        atendimentos_contrato = atendimentos[(atendimentos['Id_cli'] == cliente_id) & (atendimentos['Contrato'] == contrato_numero)]

        for _, atendimento in tqdm(atendimentos_contrato.iterrows(), total=len(atendimentos_contrato), desc="Atendimentos Processados", leave=False):
            protocolo = atendimento['Protocolo']
            atendimento_numero = atendimento['Iten']

            # Filtrar arquivos para o atendimento
            arquivos_atendimento = arquivos[arquivos['Atendimento'] == atendimento_numero]

            # Obter o nome do cliente, se o cliente existir
            if cliente_id in clientes['Id_cli'].values:
                nome_cliente = clientes[clientes['Id_cli'] == cliente_id]['Nome'].values[0]

                # Criar estrutura de diretórios
                pasta_cliente = os.path.join(pasta_destino, f"{cliente_id}_{nome_cliente}")
                pasta_contrato = os.path.join(pasta_cliente, f"Contrato_{contrato_numero}")
                pasta_plano = os.path.join(pasta_contrato, f"Plano_{plano}")
                pasta_atendimento = os.path.join(pasta_plano, f"Atendimento_{atendimento_numero}")
                
                # Verificar se há arquivos para este atendimento
                if not arquivos_atendimento.empty:
                    # Criar diretório para o atendimento
                    os.makedirs(pasta_atendimento, exist_ok=True)

                    # Copiar os arquivos para a pasta de destino
                    for _, arquivo in tqdm(arquivos_atendimento.iterrows(), total=len(arquivos_atendimento), desc=f"Copiando arquivos para {pasta_atendimento}", leave=False):
                        caminho_arquivo_origem = os.path.join(pasta_origem, arquivo['NomeArquivo'])
                        caminho_arquivo_destino = os.path.join(pasta_atendimento, os.path.basename(arquivo['NomeArquivo']))

                        try:
                            shutil.copy2(caminho_arquivo_origem, caminho_arquivo_destino)
                        except FileNotFoundError:
                            log.write(f"Arquivo não encontrado: {caminho_arquivo_origem}\n")
                        except Exception as e:
                            log.write(f"Erro ao copiar arquivo {caminho_arquivo_origem}: {str(e)}\n")

            else:
                log.write(f"Cliente ID {cliente_id} não encontrado nos dados de clientes pode está faltando informaçoes no dataframe: clientes.csv\n")

    print("Organizando arquivos do  tipo Cliente 'C' não associados a um atendimento")

#------------------Se corresponde ao número de atendimento ou id do cliente Incluindo arquivos do tipo 'C'

def organizar_arquivos_tipo_c(clientes, arquivos, pasta_origem, pasta_destino, log):
    # Filtrar apenas os arquivos do tipo 'C'
    arquivos_sem_atendimento = arquivos[arquivos['Tipo'] == 'C']

    # Agrupar arquivos sem atendimento pelo ID do atendimento
    grupos_atendimento = arquivos_sem_atendimento.groupby('Atendimento')

    for atendimento, grupo_arquivos in tqdm(grupos_atendimento, total=len(grupos_atendimento), desc="Arquivos 'C' sem Atendimento", leave=False):
        # Iterar sobre os arquivos do mesmo atendimento
        for _, arquivo in tqdm(grupo_arquivos.iterrows(), total=len(grupo_arquivos), desc=f"Atendimento {atendimento}", leave=False):
            cliente_id = arquivo['Atendimento']

            if cliente_id in clientes['Id_cli'].values:
                nome_cliente = clientes[clientes['Id_cli'] == cliente_id]['Nome'].values[0]

                pasta_cliente = os.path.join(pasta_destino, f"{cliente_id}_{nome_cliente}")

                os.makedirs(pasta_cliente, exist_ok=True)

                # Verificar se 'NomeArquivo' está vazio e usar 'Arquivo' em vez disso
                nome_arquivo = str(arquivo['NomeArquivo']) if pd.notna(arquivo['NomeArquivo']) and arquivo['NomeArquivo'] != '' else str(arquivo['Arquivo'])

                caminho_arquivo_origem = os.path.join(pasta_origem, nome_arquivo)
                caminho_arquivo_destino = os.path.join(pasta_cliente, os.path.basename(nome_arquivo))

                try:
                    shutil.copy2(caminho_arquivo_origem, caminho_arquivo_destino)
                    print(f"Arquivo copiado para {pasta_cliente}: {os.path.basename(nome_arquivo)}")
                except FileNotFoundError:
                    log.write(f"Arquivo não encontrado: {caminho_arquivo_origem}\n")
                    print("Caiu no except: Arquivo não encontrado")
                except Exception as e:
                    log.write(f"Erro ao copiar arquivo {caminho_arquivo_origem}: {str(e)}\n")
                    print("Caiu no except2: Erro ao copiar o arquivo do tipo C")
            else:
                log.write(f"Cliente ID {cliente_id} não encontrado nos dados de clientes. Pode estar faltando informações no dataframe: clientes.csv\n")

#------------------organizar_arquivos_atendimento_zero ou id zerado
def organizar_arquivos_atendimento_zero(arquivos, pasta_origem, pasta_destino, log):
    # Filtrar arquivos com 'Atendimento' igual a 0
    arquivos_atendimento_zero = arquivos[arquivos['Atendimento'] == 0]

    # Verificar se há arquivos para esse caso
    if not arquivos_atendimento_zero.empty:
        # Criar diretório para arquivos com 'Atendimento' igual a 0
        pasta_atendimento_zero = os.path.join(pasta_destino, '0_ArqSEM-id_e-atend')
        os.makedirs(pasta_atendimento_zero, exist_ok=True)

        # Copiar os arquivos para a pasta de destino
        for _, arquivo in tqdm(arquivos_atendimento_zero.iterrows(), total=len(arquivos_atendimento_zero), desc="Copiando arquivos para 'Atendimento' igual a 0", leave=False):
            nome_arquivo_origem = str(arquivo['NomeArquivo']) or str(arquivo['Arquivo'])
            caminho_arquivo_origem = os.path.join(pasta_origem, nome_arquivo_origem)
            caminho_arquivo_destino = os.path.join(pasta_atendimento_zero, os.path.basename(nome_arquivo_origem))

            try:
                shutil.copy2(caminho_arquivo_origem, caminho_arquivo_destino)
                print(f"Arquivo copiado para {pasta_atendimento_zero}: {os.path.basename(nome_arquivo_origem)}")
            except FileNotFoundError:
                log.write(f"Arquivo não encontrado: {caminho_arquivo_origem}\n")
                print("Caiu no except: Arquivo não encontrado")
            except Exception as e:
                log.write(f"Erro ao copiar arquivo {caminho_arquivo_origem}: {str(e)}\n")
                print("Caiu no except: Erro ao copiar o arquivo 'Atendimento' igual a 0")

#-----------------------------
# Nomes dos arquivos CSV
clientes_csv = 'clientes.csv'
contratos_csv = 'contratos.csv'
arquivos_csv = 'arquivos.csv'
atendimentos_csv = 'Atendimentos.csv'

# Pasta de origem dos arquivos
pasta_origem = 'Geral'

# Pasta de destino para os arquivos por cliente
pasta_destino = 'FOLDER'

# Caminho do arquivo de log
log_path = 'erros.log'

# Carrega os dados
clientes = carregar_dados(clientes_csv)
contratos = carregar_dados(contratos_csv)
arquivos = carregar_dados(arquivos_csv, dtype={'NomeArquivo': str})
atendimentos = carregar_dados(atendimentos_csv)

# Criar o log
log = open(log_path, 'w')

#----------------
# Verificar se há clientes
if not clientes.empty:
    # Chamar a função para organizar os arquivos
    organizar_arquivos(clientes, contratos, atendimentos, arquivos, pasta_origem, pasta_destino, log)

    # Chamar a função para organizar os arquivos do tipo 'C'
    organizar_arquivos_tipo_c(clientes, arquivos, pasta_origem, pasta_destino, log)

    # Adicionar chamada para atendimentos com valor zero
    organizar_arquivos_atendimento_zero(arquivos, pasta_origem, pasta_destino, log)
else:
    log.write("Nenhum cliente encontrado nos dados.\n")

# Fechar o log
log.close()

print("Processo concluído. Arquivos associados e copiados.")

