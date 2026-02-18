# Databricks notebook source
# MAGIC %md
# MAGIC # Relatório de Tempo de Atividade por Usuário (Refatorado)
# MAGIC
# MAGIC Este notebook gera o relatório final de tempo de atividade por usuário, aplicando filtros dinâmicos, pivotação de atributos customizados e formatação de saída, seguindo as melhores práticas e requisitos do cliente.
# MAGIC
# MAGIC ## Lógica
# MAGIC 1. **Carregar a Tabela Gold**: Inicia lendo a tabela `prod.gold.relatorio_tempo_atividade_usuario` pré-processada.
# MAGIC 2. **Receber Parâmetros**: Obtém os filtros da execução via `dbutils.widgets`, padronizados em inglês e camelCase.
# MAGIC 3. **Aplicar Filtros**: Filtra a tabela gold com base nos parâmetros, incluindo segurança de administrador e filtros de data.
# MAGIC 4. **Pivotação de Atributos**: Transforma as linhas de atributos customizados de usuário em colunas. Lida com nomes de atributos que contêm caracteres inválidos para o Spark e garante que o script não falhe se não houver atributos.
# MAGIC 5. **Formatar Saída**: Garante a presença de colunas de atributos obrigatórios, formata datas e prepara o DataFrame para a saída.
# MAGIC 6. **Gerar JSON**: Coleta o resultado no driver para gerar um único arquivo JSON contendo um array de objetos, que é salvo no Data Lake.

# COMMAND ----------

# MAGIC %run ../05_utils/utils

# COMMAND ----------

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window
import json
import uuid
import re
from datetime import datetime

# COMMAND ----------

# MAGIC %md # Autenticação e Configuração
# MAGIC Configura o acesso de escrita ao Azure Data Lake Storage.

# COMMAND ----------

OUTPUT_BASE_PATH = "abfss://analytics@adlsdbanalyticsprod.dfs.core.windows.net/relatorios"

# Este SAS Token deve ter permissões de Leitura, Escrita e Criação (rwc)
write_sas_token = "sv=2024-11-04&ss=bf&srt=co&sp=rwdlacyx&se=2026-12-31T08:24:50Z&st=2025-12-12T00:09:50Z&spr=https&sig=o3Z4JWJ3i5DlTAfQ7g1zTFoYe%2BZYbKmumYpJbpUHBRc%3D"

storage_account_name = "adlsdbanalyticsprod"
container_name = "analytics"

spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(
    f"fs.azure.sas.{container_name}.{storage_account_name}.dfs.core.windows.net", write_sas_token)

print(
    f"Configuração de acesso de escrita para o container '{container_name}' via SAS Token foi definida.")

# COMMAND ----------

# MAGIC %md # 1. Definição de Parâmetros (Widgets)
# MAGIC Parâmetros de entrada do relatório, padronizados para camelCase em inglês.

# COMMAND ----------

# -- IDs e Filtros de Texto --
dbutils.widgets.text("adminId", "")
dbutils.widgets.text("customerId", "")
dbutils.widgets.text("userStatus", "")
dbutils.widgets.text("profileTrackIds", "")
dbutils.widgets.text("competitionId", "")
dbutils.widgets.text("trackId", "")
dbutils.widgets.text("roundId", "")
dbutils.widgets.text("activityId", "")
dbutils.widgets.text("activityType", "")
dbutils.widgets.text("userId", "")
dbutils.widgets.text("groupId", "")
dbutils.widgets.text("activityCompletionStatus", "")

# -- Filtros de Data (Formato String: 'YYYY-MM-DD HH:MM:SS' ou 'YYYY-MM-DD') --
dbutils.widgets.text("firstAccessToActivityDateStartDate", "")
dbutils.widgets.text("firstAccessToActivityDateEndDate", "")
dbutils.widgets.text("lastActivityAccessDateStartDate", "")
dbutils.widgets.text("lastActivityAccessDateEndDate", "")
dbutils.widgets.text("activityCompletionDateStartDate", "")
dbutils.widgets.text("activityCompletionDateEndDate", "")

# -- Flags Booleanos --
dbutils.widgets.text("showInactiveCompetition", "false")
dbutils.widgets.text("showInactiveTrack", "false")
dbutils.widgets.text("showInactiveRound", "false")
dbutils.widgets.text("showInactiveActivity", "false")
dbutils.widgets.text("showEmptyDates", "false")

# COMMAND ----------

# MAGIC %md # 2. Função de Geração do Relatório
# MAGIC
# MAGIC Encapsula toda a lógica de negócio, incluindo a conversão de parâmetros, para maior robustez e testabilidade.
# MAGIC

# COMMAND ----------


def engagesp_relatorio_tempo_atividade_usuario(
        adminId, customerId, userStatus, profileTrackIds, competitionId, trackId,
        roundId, activityId, activityType, userId, groupId, activityCompletionStatus,
        firstAccessToActivityDateStartDate, firstAccessToActivityDateEndDate,
        lastActivityAccessDateStartDate, lastActivityAccessDateEndDate,
        activityCompletionDateStartDate, activityCompletionDateEndDate,
        showInactiveCompetition, showInactiveTrack, showInactiveRound,
        showInactiveActivity, showEmptyDates
) -> DataFrame:
    
    # Mantém compatibilidade com a lógica interna que usa clienteID
    clienteID = customerId

    # --- Funções de Conversão Internas ---
    def parse_date_to_timestamp(date_str: str) -> datetime:
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            # Tenta formato ISO 8601 completo (ex: 2025-01-01T13:40:42.000Z)
            return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            try:
                # Tenta formato padrão SQL (ex: 2025-01-01 13:40:42)
                return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    # Tenta apenas data (ex: 2025-01-01)
                    return datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    return None

    def get_boolean_from_str(bool_str) -> bool:
        if isinstance(bool_str, bool):
            return bool_str
        return str(bool_str).lower() in ['true', '1']

    # --- Conversão de Parâmetros ---
    adminId_int = int(adminId) if adminId and str(adminId).isdigit() else None
    start_date_1 = parse_date_to_timestamp(firstAccessToActivityDateStartDate)
    end_date_1 = parse_date_to_timestamp(firstAccessToActivityDateEndDate)
    start_date_2 = parse_date_to_timestamp(lastActivityAccessDateStartDate)
    end_date_2 = parse_date_to_timestamp(lastActivityAccessDateEndDate)
    start_date_3 = parse_date_to_timestamp(activityCompletionDateStartDate)
    end_date_3 = parse_date_to_timestamp(activityCompletionDateEndDate)
    show_inactive_competition_bool = get_boolean_from_str(showInactiveCompetition)
    show_inactive_track_bool = get_boolean_from_str(showInactiveTrack)
    show_inactive_round_bool = get_boolean_from_str(showInactiveRound)
    show_inactive_activity_bool = get_boolean_from_str(showInactiveActivity)
    show_empty_dates_bool = get_boolean_from_str(showEmptyDates)

    spark.catalog.clearCache()
    utils = EngageUtils(spark)

    colunas_atributos_json = [
        "desc._cargo", "desc._uni._org.", "chefe_imediato", "regime_contratual",
        "liderança", "área", "mod_ou_moi", "preposto_ou_gestor", "e-mail_do_preposto_ou_gestor"
    ]

    try:
        gold_df = spark.table("prod.gold.relatorio_tempo_atividade_usuario")
    except Exception as e:
        return spark.createDataFrame([{"error": f"Tabela gold 'prod.gold.relatorio_tempo_atividade_usuario' não encontrada: {e}"}])

    for col_name in ["DataPrimeiroAcessoNaAtividade", "DataUltimoAcessoNaAtividade", "DataConclusaoAtividade"]:
        gold_df = gold_df.withColumn(f"{col_name}_ts", F.col(col_name))

    if not clienteID:
        return spark.createDataFrame([{"error": "O parâmetro 'customerId' é obrigatório."}])
    gold_df = gold_df.where(F.col("clienteID") == clienteID)

    if adminId_int:
        entidades_editaveis_df = utils.fnt_entidades_editaveis(
            adminId_int, clienteID, 'US', None).alias("ee")
        
        join_condition = (gold_df["UsuarioID"] == F.col("ee.EntidadeId")) & (gold_df["clienteID"] == F.col("ee.clienteID"))
        
        cols_to_select = [gold_df[c] for c in gold_df.columns]
        
        gold_df = gold_df.join(entidades_editaveis_df, join_condition, "inner").select(*cols_to_select)

    if userStatus:
        status_map = {'1': 'Ativo', '0': 'Inativo'}
        mapped_status = [s for s in [status_map.get(
            s.strip()) for s in userStatus.split(',')] if s]
        if mapped_status:
            gold_df = gold_df.where(F.col("StatusUsuario").isin(mapped_status))

    if profileTrackIds:
        perfil_map = {'1': 'Obrigatório', '2': 'Participa', '3': 'Gestor'}
        mapped_perfis = [p for p in [perfil_map.get(
            p.strip()) for p in profileTrackIds.split(',')] if p]
        if mapped_perfis:
            gold_df = gold_df.where(F.col("PerfilNaTrilha").isin(mapped_perfis))

    for col_name, param_val in [("AmbienteID", competitionId), ("TrilhaId", trackId),
                                ("ModuloID", roundId), ("AtividadeId", activityId),
                                ("UsuarioID", userId)]:
        if param_val:
            id_list = [int(i) for i in str(param_val).split(',')
                       if i.strip().isdigit()]
            if id_list:
                gold_df = gold_df.where(F.col(col_name).isin(id_list))

    if activityType:
        gold_df = gold_df.where(
            F.col("AtividadeTipoId").isin(activityType.split(',')))

    if activityCompletionStatus:
        # Mapeia valores de entrada para valores do banco
        status_map = {
            'in_progress': 'Em Andamento',
            'completed': 'Concluído', 
            'not_started': 'Não Iniciado',
            'expired': 'Expirado',
            'dispensed': 'Dispensado',
            'awaiting_correction': 'Aguardando Correção',
            'not_released': 'Não Liberado'
        }
        status_list = []
        for s in activityCompletionStatus.split(','):
            s_stripped = s.strip()
            s_lower = s_stripped.lower()
            # Tenta mapear, ou usa o valor original (pode já estar em português)
            mapped_status = status_map.get(s_lower, s_stripped)
            status_list.append(mapped_status)
        
        if status_list:
            gold_df = gold_df.where(F.col("StatusUsuarioAtividade").isin(status_list))

    # Aplicar filtros de data seguindo a lógica do SQL (3-way OR):
    # 1. Se datas não fornecidas: retorna todas as linhas
    # 2. Se datas fornecidas: retorna linhas no intervalo
    # 3. Se showEmptyDates=true: também inclui linhas com data null
    date_filters = [
        ("DataPrimeiroAcessoNaAtividade_ts", start_date_1, end_date_1),
        ("DataUltimoAcessoNaAtividade_ts", start_date_2, end_date_2),
        ("DataConclusaoAtividade_ts", start_date_3, end_date_3)
    ]
    for col_name, start_date, end_date in date_filters:
        # Se ambas as datas são fornecidas, aplicar filtro
        if start_date and end_date:
            # Condição base: valor está no intervalo de datas
            condition = F.col(col_name).between(start_date, end_date)
            # Se showEmptyDates é true, também incluir registros com data null
            if show_empty_dates_bool:
                condition = condition | F.col(col_name).isNull()
            gold_df = gold_df.where(condition)
        # Se datas não são fornecidas, não aplicar filtro (SQL retorna tudo neste caso)
        # Isso já está correto - não fazemos nada

    if not show_inactive_competition_bool:
        gold_df = gold_df.where(F.col("AmbienteAtivo") == True)
    if not show_inactive_track_bool:
        gold_df = gold_df.where(F.col("TrilhaAtiva") == True)
    if not show_inactive_round_bool:
        gold_df = gold_df.where(F.col("RodadaAtiva") == True)
    if not show_inactive_activity_bool:
        gold_df = gold_df.where(F.col("AtividadeAtiva") == True)

    usuarios_df = gold_df.select("UsuarioID", F.col("clienteID").alias("ClienteID")).distinct()
    grupos_usuario_df = utils.fnt_todos_grupos_usuarios(
        usuarios_df, groupId).alias("grupos")
    gold_df = gold_df.join(
        grupos_usuario_df,
        ["UsuarioID", "clienteID"],
        "inner"
    )

    attribute_names = utils.get_attribute_names(clienteID)
    sanitized_attribute_map = {
        attr: f"attr_{re.sub(r'[^a-zA-Z0-9_]', '_', attr)}" for attr in attribute_names}

    if attribute_names:
        atributos_usuarios_df = utils.fnt_atributos_usuarios(
            clienteID).alias("attr")
        map_expr = F.create_map(
            [F.lit(item) for pair in sanitized_attribute_map.items() for item in pair])
        atributos_usuarios_df = atributos_usuarios_df.withColumn(
            "AtributoSanitizado", map_expr[F.col("attr.Atributo")])

        df_para_pivotar = gold_df.join(
            atributos_usuarios_df, gold_df["UsuarioID"] == F.col("attr.UsuarioId"), "left"
        ).select(
            *[gold_df[c] for c in gold_df.columns],
            F.col("AtributoSanitizado"),
            F.col("attr.AtributoValor")
        )
        grouping_cols = [c for c in gold_df.columns]
        final_df = df_para_pivotar.groupBy(*grouping_cols).pivot(
            "AtributoSanitizado", list(sanitized_attribute_map.values())
        ).agg(F.first("AtributoValor"))

        # Renomeia as colunas sanitizadas de volta para os nomes originais
        for original_name, sanitized_name in sanitized_attribute_map.items():
            if sanitized_name in final_df.columns:
                final_df = final_df.withColumnRenamed(sanitized_name, original_name)
        
        colunas_adicionais = attribute_names
    else:
        # Se não houver atributos para pivotar, adicionamos as colunas padrão como nulas
        final_df = gold_df
        for attr_col in colunas_atributos_json:
            final_df = final_df.withColumn(f"`{attr_col}`", F.lit(None).cast("string"))
        
        colunas_adicionais = colunas_atributos_json

    for col_name in ["DataPrimeiroAcessoNaAtividade", "DataUltimoAcessoNaAtividade", "DataConclusaoAtividade"]:
        final_df = final_df.withColumn(
            col_name,
            F.coalesce(F.date_format(
                F.col(f"{col_name}_ts"), "dd/MM/yyyy HH:mm"), F.lit(""))
        )

    final_df = final_df.withColumn("AcessouReuniaoZoom", F.lit(""))

    colunas_base = [
        "UsuarioID", "NomeUsuario", "EmailUsuario", "LoginUsuario", "StatusUsuario", "GrupoPaiId", "NomeGrupoPai",
        "GrupoFilhoId", "NomeGrupoFilho", "TodosGruposUsuario", "AmbienteID", "NomeAmbiente", "TrilhaID", "NomeTrilha", "ModuloID",
        "NomeModulo", "AtividadeId", "NomeAtividade", "TentativaID", "CargaHorariaAtividade", "TipoAtividades",
        "TempoAcessoTotal", "QtdAcessosNaAtividade", "DataPrimeiroAcessoNaAtividade", "DataUltimoAcessoNaAtividade",
        "DataConclusaoAtividade", "AproveitamentoAtividade", "EnunciadoAtividade", "RespostaUsuario",
        "PesoAtividade", "FeedbackAtividade", "StatusUsuarioAtividade", "PerfilNaTrilha", "AcessouReuniaoZoom"
    ]
    colunas_finais = colunas_base + colunas_adicionais
    
    final_df = final_df.orderBy("NomeUsuario", "NomeTrilha", "NomeModulo", "NomeAtividade")
    
    # Select columns safely, adding backticks if needed for attribute columns with dots
    select_exprs = []
    current_columns = final_df.columns
    for c in colunas_finais:
        if c in current_columns:
            if "." in c:
                select_exprs.append(F.col(f"`{c}`"))
            else:
                select_exprs.append(F.col(c))
                
    return final_df.select(*select_exprs)


# COMMAND ----------

# MAGIC
# MAGIC %md # 3. Chamada da Função e Geração do JSON
# MAGIC
# MAGIC Executa a função principal e salva o resultado no formato JSON de array.
# MAGIC

# COMMAND ----------

import json

# Define se o notebook está em modo de teste (True) ou produção (False)
debug = True

# --- 1. Definição dos Parâmetros ---
if debug:
    print("--- MODO DEBUG ATIVO: Usando parâmetros fixos ---")
    params = {
        "adminId": 865140,
        "customerId": "arcelormittal",
        "userStatus": "1",
        "profileTrackIds": "1,2",
        "competitionId": "2442",
        "trackId": "",
        "roundId": "",
        "activityId": "",
        "activityType": "",
        "userId": "",
        "groupId": "",
        "activityCompletionStatus": "",
        "firstAccessToActivityDateStartDate": "",
        "firstAccessToActivityDateEndDate": "",
        "lastActivityAccessDateStartDate": "",
        "lastActivityAccessDateEndDate": "",
        "activityCompletionDateStartDate": "",
        "activityCompletionDateEndDate": "",
        "showInactiveCompetition": "",
        "showInactiveTrack": "False",
        "showInactiveRound": "False",
        "showInactiveActivity": "False",
        "showEmptyDates": "False"
    }
else:
    # Captura explícita de parâmetros dos Widgets (Modo Produção)
    if __name__ == "__main__":
        params = {
            "adminId": dbutils.widgets.get("adminId"),
            "customerId": dbutils.widgets.get("customerId"),
            "userStatus": dbutils.widgets.get("userStatus"),
            "profileTrackIds": dbutils.widgets.get("profileTrackIds"),
            "competitionId": dbutils.widgets.get("competitionId"),
            "trackId": dbutils.widgets.get("trackId"),
            "roundId": dbutils.widgets.get("roundId"),
            "activityId": dbutils.widgets.get("activityId"),
            "activityType": dbutils.widgets.get("activityType"),
            "userId": dbutils.widgets.get("userId"),
            "groupId": dbutils.widgets.get("groupId"),
            "activityCompletionStatus": dbutils.widgets.get("activityCompletionStatus"),
            "firstAccessToActivityDateStartDate": dbutils.widgets.get("firstAccessToActivityDateStartDate"),
            "firstAccessToActivityDateEndDate": dbutils.widgets.get("firstAccessToActivityDateEndDate"),
            "lastActivityAccessDateStartDate": dbutils.widgets.get("lastActivityAccessDateStartDate"),
            "lastActivityAccessDateEndDate": dbutils.widgets.get("lastActivityAccessDateEndDate"),
            "activityCompletionDateStartDate": dbutils.widgets.get("activityCompletionDateStartDate"),
            "activityCompletionDateEndDate": dbutils.widgets.get("activityCompletionDateEndDate"),
            "showInactiveCompetition": dbutils.widgets.get("showInactiveCompetition"),
            "showInactiveTrack": dbutils.widgets.get("showInactiveTrack"),
            "showInactiveRound": dbutils.widgets.get("showInactiveRound"),
            "showInactiveActivity": dbutils.widgets.get("showInactiveActivity"),
            "showEmptyDates": dbutils.widgets.get("showEmptyDates")
        }

        # Validação de Segurança para Produção
        if not params.get("customerId"):
             error_msg = {"error": "O parâmetro 'customerId' (Widget) está vazio. Preencha o widget no topo do notebook ou ative o modo debug=True."}
             dbutils.notebook.exit(json.dumps(error_msg, ensure_ascii=False))

# --- 2. Execução e Salvamento (Comum) ---
print("--- Parâmetros Utilizados ---")
print(json.dumps(params, indent=2, ensure_ascii=False))
print("-----------------------------")

final_df = engagesp_relatorio_tempo_atividade_usuario(**params)

if hasattr(final_df, "columns") and "error" not in final_df.columns:
    report_name = "relatorio-tempo-atividade-usuario"
    
    # Define o identificador do usuário para o nome do arquivo
    user_id = params.get("userId")
    admin_id = params.get("adminId")
    # Garante que seja string para evitar erro no format
    user_identifier = str(user_id) if user_id and str(user_id).strip() != "" else str(admin_id)
    
    datetime_str = datetime.now().strftime("%Y%m%d%H%M%S")
    random_id = uuid.uuid4().hex[:8]
    file_name = f"{report_name}-user{user_identifier}-{datetime_str}-{random_id}.json"
    output_path = f"{OUTPUT_BASE_PATH}/{report_name}/{file_name}"

    # Coleta os dados para geração do JSON (Array de Objetos)
    # Se for debug, imprime uma amostra antes de salvar
    if debug:
        print("\n--- Amostra do Resultado (Top 5) ---")
        if hasattr(final_df, "limit"):
             print(json.dumps(final_df.limit(5).toPandas().to_dict(orient="records"), indent=2, ensure_ascii=False))
    
    collected_data = [row.asDict(recursive=True) for row in final_df.collect()]
    json_output = json.dumps(collected_data, indent=2, ensure_ascii=False)

    # Salva o arquivo no Data Lake
    print(f"\nSalvando arquivo em: {output_path}")
    dbutils.fs.put(output_path, json_output, overwrite=True)

    # Libera memória e encerra o notebook retornando o caminho do arquivo
    if hasattr(final_df, "unpersist"):
        final_df.unpersist()
    
    dbutils.notebook.exit(output_path)

else:  # Caso a função retorne um DataFrame de erro ou erro crítico
    if hasattr(final_df, "first"):
        row = final_df.first()
        error_message = row.asDict(recursive=True) if row else {"error": "DataFrame de erro vazio."}
    else:
        error_message = final_df
    
    print("\n--- Erro na Execução ---")
    print(error_message)
    dbutils.notebook.exit(json.dumps(error_message, ensure_ascii=False, default=str))