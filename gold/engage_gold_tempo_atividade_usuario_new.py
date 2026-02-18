# Databricks notebook source
# MAGIC %md
# MAGIC Gold Layer: Tempo de Atividade por Usuário
# MAGIC
# MAGIC Este notebook processa os dados brutos (bronze/silver) e aplica as transformações e joins necessários para criar uma tabela gold consolidada. Esta tabela servirá como a fonte principal para o relatório snapshot `engagesp_Relatorio_De_Tempo_De_Atividade_Por_Usuario`.
# MAGIC
# MAGIC ## Lógica de Negócio
# MAGIC
# MAGIC A lógica deste notebook replica as seguintes etapas da stored procedure original:
# MAGIC 1. **Identificação de Entidades**: Carrega as tabelas de competições, trilhas, rodadas, atividades e usuários.
# MAGIC 2. **Plano de Usuário**: Cruza as informações para determinar quais atividades cada usuário tem acesso.
# MAGIC 3. **Dados de Interação**: Junta os dados de tentativas, acessos, respostas e dispensas.
# MAGIC 4. **Cálculo de Status**: Aplica as regras de negócio para determinar o status de cada atividade para cada usuário (e.g., 'Concluído', 'Em Andamento', 'Expirado').
# MAGIC 5. **Enriquecimento de Dados**: Adiciona informações de grupos, atributos de usuário e outros metadados.
# MAGIC 6. **Salvamento**: Salva o DataFrame resultante como uma tabela Delta na camada Gold.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../05_utils/utils

# COMMAND ----------

# Definição das tabelas que serão utilizadas
tabelas = [
    "atividade", "rodada_atividade", "rodada", "trilha", 
    "competicao_trilha", "competicao", "usuario_perfil_entidade",
    "usuario", "atividade_tipo", "rodada_atividade_usuario", "tentativa",
    "acesso_atividade", "resposta", "dispensa_rodada", "usuario_grupo",
    "grupo", "competicao_grupo", "atividade_alternativa", 
    "resposta_alternativa", "pergunta_formulario", 
    "resposta_atividade_formulario", "alternativa_formulario",
    "resposta_alternativa_formulario", "resposta_minigame",
    "resposta_alternativa_minigame", "alternativa_minigame"
]

# Dicionário para armazenar os DataFrames
dfs = {t: spark.table(f"prod.silver.{t}") for t in tabelas}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Construção da Base da Trilha (Equivalente a #UsuarioTrilha)
# MAGIC
# MAGIC A primeira etapa é juntar as tabelas hierárquicas para criar uma visão completa de todas as atividades dentro de suas respectivas rodadas, trilhas e competições.

# COMMAND ----------

# CTE Atividades - Filtra apenas atividades dos tipos padrao_sou e scorm (conforme SQL procedure linha 102)
atividades_df = dfs["atividade"].where(F.col("id_tipo").isin('padrao_sou', 'scorm'))

# Construção da base hierárquica
base_trilha_df = atividades_df.alias("Atividade") \
    .join(
        dfs["rodada_atividade"].alias("RA"),
        F.col("Atividade.id") == F.col("RA.id_atividade"),
        "inner"
    ) \
    .join(
        dfs["rodada"].alias("Rodada"),
        F.col("RA.id_rodada") == F.col("Rodada.id"),
        "inner"
    ) \
    .join(
        dfs["trilha"].alias("Trilha"),
        F.col("Rodada.id_trilha") == F.col("Trilha.id"),
        "inner"
    ) \
    .join(
        dfs["competicao_trilha"].alias("CT"),
        F.col("Trilha.id") == F.col("CT.id_trilha"),
        "inner"
    ) \
    .join(
        dfs["competicao"].alias("Competicao"),
        F.col("CT.id_competicao") == F.col("Competicao.id"),
        "inner"
    ) \
    .select(
        F.col("Competicao.id").alias("CompeticaoId"),
        F.col("Competicao.id_cliente").alias("ClienteId"),
        F.col("Competicao.texto_nome").alias("Competicao"),
        F.col("Competicao.flag_disponivel").alias("AmbienteAtivo"),
        F.col("Trilha.id").alias("TrilhaId"),
        F.col("Trilha.texto_descricao").alias("Trilha"),
        F.col("Trilha.flag_status").alias("TrilhaAtiva"),
        F.col("Rodada.id").alias("RodadaId"),
        F.col("Rodada.texto_nome").alias("Rodada"),
        F.col("Rodada.flag_status").alias("RodadaAtiva"),
        F.col("Atividade.id").alias("AtividadeId"),
        F.col("Atividade.id_tipo").alias("AtividadeTipoId"),
        F.col("Atividade.texto_titulo").alias("Titulo"),
        F.col("Atividade.texto_enunciado").alias("Enunciado"),
        F.col("Atividade.flag_status").alias("AtividadeAtiva")
    ).distinct()

# Exibir o resultado para verificação
# display(base_trilha_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Junção com Dados do Usuário
# MAGIC
# MAGIC Agora, cruzamos a `base_trilha_df` com as permissões de usuário (`usuario_perfil_entidade`) e os dados cadastrais do usuário (`usuario`) para criar a base do relatório.

# COMMAND ----------

# Carrega a tabela de rótulos para buscar o nome do tipo da atividade
rotulo_idioma_df = spark.table("prod.silver.rotulo_idioma").where(lower(F.col("id_idioma")) == "pt-br")

resultado_parcial_df = base_trilha_df.alias("UsuarioTrilha") \
    .join(
        dfs["usuario_perfil_entidade"].alias("UPE"),
        (F.col("UPE.id_entidade") == F.col("UsuarioTrilha.RodadaId")) &
        (F.col("UPE.id_cliente") == F.col("UsuarioTrilha.ClienteId")) &
        (F.trim(F.col("UPE.id_entidade_tipo")) == "RD") &
        (F.col("UPE.id_perfil_jogo").isin([1, 2, 3])),
        "inner"
    ) \
    .join(
        dfs["usuario"].alias("Usuario"),
        (F.col("Usuario.id") == F.col("UPE.id_usuario")) &
        (F.col("Usuario.id_cliente") == F.col("UPE.id_cliente")) &
        (F.col("Usuario.data_exclusao").isNull()) &
        (F.col("Usuario.flag_padrao") == False),
        "inner"
    ) \
    .join(
        dfs["atividade_tipo"].alias("AT"),
        F.col("AT.id") == F.col("UsuarioTrilha.AtividadeTipoId"),
        "left"
    ) \
    .join(
        rotulo_idioma_df.alias("RotuloAtividade"),
        F.col("AT.id_rotulo_nome") == F.col("RotuloAtividade.id_rotulo"),
        "left"
    ) \
    .select(
        F.col("Usuario.id").alias("UsuarioID"),
        F.col("Usuario.texto_nome_completo").alias("NomeUsuario"),
        F.col("Usuario.texto_email").alias("EmailUsuario"),
        F.col("Usuario.texto_login").alias("LoginUsuario"),
        F.when(F.col("Usuario.flag_status") == 1, "Ativo").otherwise("Inativo").alias("StatusUsuario"),
        
        F.col("UsuarioTrilha.CompeticaoId").alias("AmbienteID"),
        F.col("UsuarioTrilha.Competicao").alias("NomeAmbiente"),
        F.col("UsuarioTrilha.AmbienteAtivo"),
        F.col("UsuarioTrilha.TrilhaId").alias("TrilhaID"),
        F.col("UsuarioTrilha.Trilha").alias("NomeTrilha"),
        F.col("UsuarioTrilha.TrilhaAtiva"),
        F.col("UsuarioTrilha.RodadaId").alias("ModuloID"),
        F.col("UsuarioTrilha.Rodada").alias("NomeModulo"),
        F.col("UsuarioTrilha.RodadaAtiva"),
        F.col("UsuarioTrilha.AtividadeId"),
        F.col("UsuarioTrilha.Titulo").alias("NomeAtividade"),
        F.col("UsuarioTrilha.AtividadeAtiva"),
        F.col("UsuarioTrilha.AtividadeTipoId"),
        F.col("RotuloAtividade.texto_texto").alias("TipoAtividades"),
        F.col("UsuarioTrilha.Enunciado").alias("EnunciadoAtividade"),

        F.col("UPE.id_cliente").alias("ClienteId"),
        F.when(F.col("UPE.id_perfil_jogo") == 1, "Obrigatório")
         .when(F.col("UPE.id_perfil_jogo") == 2, "Participa")
         .when(F.col("UPE.id_perfil_jogo") == 3, "Gestor")
         .otherwise("Não Participa").alias("PerfilNaTrilha")
    )

# display(resultado_parcial_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Junção com Dados de Interação (Tentativas, Acessos e Respostas)
# MAGIC
# MAGIC O passo seguinte é encontrar a **última tentativa válida** para cada usuário em cada atividade e, a partir dela, buscar as informações de acesso, tempo gasto e respostas.

# COMMAND ----------

# Janela para encontrar a última tentativa de um usuário em uma rodada
window_ultima_tentativa = Window.partitionBy("id_usuario", "id_cliente", "id_rodada").orderBy(F.col("id").desc())

# Seleciona a última tentativa que não foi cancelada
ultima_tentativa_df = dfs["tentativa"] \
    .where(F.col("id_status") != "cancelado") \
    .withColumn("rn", F.row_number().over(window_ultima_tentativa)) \
    .where(F.col("rn") == 1) \
    .select(
        F.col("id").alias("TentativaID"),
        F.col("id_usuario"),
        F.col("id_cliente"),
        F.col("id_rodada"),
        F.col("id_status").alias("StatusTentativa")
    )

# Agregar informações de TODOS os acessos à atividade para calcular TempoAcessoTotal e QtdAcessosNaAtividade
acessos_agregados_df = dfs["acesso_atividade"].groupBy(
    "id_tentativa", "id_cliente", "id_atividade"
).agg(
    F.sum("numero_tempo_em_segundos").alias("TempoAcessoTotalEmSegundos"),
    F.count("*").alias("QtdAcessosNaAtividade"),
    F.min("data_cadastro").alias("DataPrimeiroAcessoNaAtividade"),
    F.max("data_ultima_atualizacao").alias("DataUltimoAcessoNaAtividade"),
    F.max("numero_tempo_em_segundos").alias("TempoAcessoNaAtividadeEmSegundos")  # Mantém para compatibilidade
)

# Juntando o resultado parcial com a última tentativa
resultado_com_tentativa_df = resultado_parcial_df.alias("RP") \
    .join(
        ultima_tentativa_df.alias("UT"),
        (F.col("RP.UsuarioID") == F.col("UT.id_usuario")) &
        (F.col("RP.ClienteId") == F.col("UT.id_cliente")) &
        (F.col("RP.ModuloID") == F.col("UT.id_rodada")),
        "left"
    ) \
    .join( # Left Join com AcessosAgregados (soma de todos os acessos)
        acessos_agregados_df.alias("AcessoAtividade"),
        (F.col("AcessoAtividade.id_tentativa") == F.col("UT.TentativaID")) &
        (F.col("AcessoAtividade.id_cliente") == F.col("UT.id_cliente")) &
        (F.col("AcessoAtividade.id_atividade") == F.col("RP.AtividadeId")),
        "left"
    ) \
    .join( # Left Join com Resposta
        dfs["resposta"].alias("Resposta"),
        (F.col("Resposta.id_tentativa") == F.col("UT.TentativaID")) &
        (F.col("Resposta.id_cliente") == F.col("UT.id_cliente")) &
        (F.col("Resposta.id_atividade") == F.col("RP.AtividadeId")) &
        (F.col("Resposta.id_usuario") == F.col("UT.id_usuario")) &
        (F.col("Resposta.id_competicao") == F.col("RP.AmbienteID")),
        "left"
    ) \
    .select(
        "RP.*",
        "UT.TentativaID",
        "UT.StatusTentativa",
        F.col("AcessoAtividade.TempoAcessoNaAtividadeEmSegundos"),
        F.col("AcessoAtividade.TempoAcessoTotalEmSegundos"),
        F.col("AcessoAtividade.QtdAcessosNaAtividade"),
        F.col("AcessoAtividade.DataPrimeiroAcessoNaAtividade"),
        F.col("AcessoAtividade.DataUltimoAcessoNaAtividade"),
        F.col("Resposta.data_resposta").alias("DataConclusaoAtividade"),
        F.col("Resposta.numero_porcentagem_acertos").alias("AproveitamentoAtividade"),
        F.col("Resposta.texto_resposta_dissertativa"),
        F.col("Resposta.texto_feedback").alias("FeedbackAtividade"),
        F.col("Resposta.flag_corrigida").alias("RespostaCorrigida")
    )

# display(resultado_com_tentativa_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Junção com Grupos do Usuário
# MAGIC
# MAGIC Identificamos o grupo e o grupo pai do usuário no contexto de cada competição (ambiente). Na procedure, isso é feito com `CROSS APPLY`. Aqui, usamos uma `Window Function` para selecionar o primeiro grupo associado.

# COMMAND ----------

# Junção para encontrar o grupo do usuário na competição
grupos_usuario_df = dfs["usuario_grupo"].alias("UsuarioGrupo") \
    .join(
        dfs["grupo"].alias("Grupo"),
        (F.col("Grupo.id") == F.col("UsuarioGrupo.id_grupo")) &
        (F.col("Grupo.id_cliente") == F.col("UsuarioGrupo.id_cliente")) &
        (F.col("Grupo.flag_status") == 1),
        "inner"
    ) \
    .join(
        dfs["competicao_grupo"].alias("CompeticaoGrupo"),
        (F.col("CompeticaoGrupo.id_grupo") == F.col("Grupo.id")) &
        (F.col("CompeticaoGrupo.id_cliente") == F.col("Grupo.id_cliente")),
        "inner"
    ) \
    .join(
        dfs["grupo"].alias("GrupoPai"),
        (F.col("GrupoPai.id") == F.col("Grupo.id_grupo_pai")) &
        (F.col("GrupoPai.id_cliente") == F.col("Grupo.id_cliente")),
        "left"
    ) \
    .select(
        "UsuarioGrupo.id_usuario",
        F.col("UsuarioGrupo.id_cliente"),
        F.col("CompeticaoGrupo.id_competicao"),
        F.col("Grupo.id").alias("GrupoFilhoId"),
        F.col("Grupo.texto_nome").alias("NomeGrupoFilho"),
        F.col("GrupoPai.id").alias("GrupoPaiId"),
        F.col("GrupoPai.texto_nome").alias("NomeGrupoPai")
    )

# Janela para pegar o primeiro grupo, simulando o TOP 1 do CROSS APPLY
window_primeiro_grupo = Window.partitionBy("id_usuario", "id_cliente", "id_competicao").orderBy("GrupoFilhoId")

primeiro_grupo_usuario_df = grupos_usuario_df \
    .withColumn("rn", F.row_number().over(window_primeiro_grupo)) \
    .where(F.col("rn") == 1)

# Juntando o resultado com a informação do grupo
resultado_com_grupo_df = resultado_com_tentativa_df.alias("RTC") \
    .join(
        primeiro_grupo_usuario_df.alias("PGU"),
        (F.col("RTC.UsuarioID") == F.col("PGU.id_usuario")) &
        (F.col("RTC.ClienteId") == F.col("PGU.id_cliente")) &
        (F.col("RTC.AmbienteID") == F.col("PGU.id_competicao")),
        "left"
    ) \
    .select(
        "RTC.*",
        "PGU.GrupoPaiId",
        "PGU.NomeGrupoPai",
        "PGU.GrupoFilhoId",
        "PGU.NomeGrupoFilho"
    )

# display(resultado_com_grupo_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Agregação de Respostas
# MAGIC
# MAGIC Nesta seção, replicamos a lógica de `STRING_AGG` da procedure para consolidar as respostas dos usuários em uma única string, dependendo do tipo da atividade.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.1. Respostas de Múltipla Escolha

# COMMAND ----------

# Agrega respostas de múltipla escolha (inclui tipos como 'verdadeiro_falso', 'multiplas_respostas', etc.)
resposta_multipla_escolha_df = dfs["resposta_alternativa"].alias("RA") \
    .join(
        dfs["atividade_alternativa"].alias("AA"),
        (F.col("RA.id_alternativa") == F.col("AA.id")) &
        (F.col("RA.id_cliente") == F.col("AA.id_cliente")) &
        (F.col("AA.flag_status") == 1),
        "inner"
    ) \
    .where(F.col("RA.flag_correta") == 1) \
    .groupBy("RA.id_tentativa", "RA.id_cliente") \
    .agg(F.concat_ws(", ", F.collect_list(F.col("AA.texto_alternativa"))).alias("RespostaUsuarioMultiplaEscolha"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.2. Respostas de Formulário

# COMMAND ----------

# Agrega respostas de atividades do tipo formulário
# 1. Junta as respostas (texto, escala) com as alternativas selecionadas
respostas_formulario_base_df = dfs["pergunta_formulario"].alias("PF") \
    .join(
        dfs["resposta_atividade_formulario"].alias("RAF"),
        (F.col("PF.id") == F.col("RAF.id_pergunta")) & 
        (F.col("PF.id_cliente") == F.col("RAF.id_cliente")),
        "left"
    ) \
    .join(
        dfs["resposta_alternativa_formulario"].alias("RALF"),
        (F.col("PF.id") == F.col("RALF.id_pergunta_formulario")) &
        (F.col("RAF.id_tentativa") == F.col("RALF.id_tentativa")) &
        (F.col("RALF.flag_selecionada") == 1),
        "left"
    ) \
    .join(
        dfs["alternativa_formulario"].alias("AF"),
        (F.col("RALF.id_alternativa") == F.col("AF.id")),
        "left"
    ) \
    .where(F.col("PF.flag_status") == 1) \
    .select(
        F.col("RAF.id_tentativa"),
        F.col("RAF.id_cliente"),
        F.col("PF.id_atividade"),
        # Concatena as diferentes formas de resposta para uma única pergunta
        F.concat_ws(", ", 
            F.col("AF.texto_alternativa"), 
            F.col("RAF.texto_resposta_dissertativa"),
            F.col("RAF.numero_resposta_escala")
        ).alias("RespostaConsolidadaPergunta")
    )

# 2. Agrega todas as respostas de um formulário (tentativa) em uma única string
resposta_formulario_df = respostas_formulario_base_df \
    .groupBy("id_tentativa", "id_cliente") \
    .agg(F.concat_ws(", ", F.collect_list("RespostaConsolidadaPergunta")).alias("RespostaUsuarioFormulario"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.3. Respostas de Minigame

# COMMAND ----------

# Agrega respostas de atividades do tipo minigame
resposta_minigame_df = dfs["resposta_minigame"].alias("RM") \
    .join(
        dfs["resposta_alternativa_minigame"].alias("RALM"),
        (F.col("RM.id_tentativa") == F.col("RALM.id_tentativa")) &
        (F.col("RM.id_pergunta_minigame") == F.col("RALM.id_pergunta_minigame")) &
        (F.col("RALM.flag_selecionada") == 1),
        "inner"
    ) \
    .join(
        dfs["alternativa_minigame"].alias("AM"),
        (F.col("RALM.id_alternativa") == F.col("AM.id")) &
        (F.col("AM.flag_status") == 1),
        "inner"
    ) \
    .groupBy("RM.id_tentativa", "RM.id_cliente") \
    .agg(F.concat_ws(", ", F.collect_list("AM.texto_alternativa")).alias("RespostaUsuarioMinigame"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.4. Unificação das Respostas

# COMMAND ----------

# Juntando o resultado com todas as respostas agregadas
resultado_com_respostas_df = resultado_com_grupo_df.alias("RCG") \
    .join(
        resposta_multipla_escolha_df.alias("RME"),
        (F.col("RCG.TentativaID") == F.col("RME.id_tentativa")) &
        (F.col("RCG.ClienteId") == F.col("RME.id_cliente")),
        "left"
    ) \
    .join(
        resposta_formulario_df.alias("RF"),
        (F.col("RCG.TentativaID") == F.col("RF.id_tentativa")) &
        (F.col("RCG.ClienteId") == F.col("RF.id_cliente")),
        "left"
    ) \
    .join(
        resposta_minigame_df.alias("RMG"),
        (F.col("RCG.TentativaID") == F.col("RMG.id_tentativa")) &
        (F.col("RCG.ClienteId") == F.col("RMG.id_cliente")),
        "left"
    ) \
    .select(
        "RCG.*", 
        "RME.RespostaUsuarioMultiplaEscolha",
        "RF.RespostaUsuarioFormulario",
        "RMG.RespostaUsuarioMinigame"
    )

# Unifica as respostas em uma única coluna 'RespostaUsuario'
resultado_final_parcial_df = resultado_com_respostas_df.withColumn(
    "RespostaUsuario",
    F.when(
        F.col("AtividadeTipoId").isin('multipla_escolha', 'verdadeiro_falso', 'multiplas_respostas', 'pesquisaMultiplaEscolha', 'pesquisaMultiplasRespostas'),
        F.col("RespostaUsuarioMultiplaEscolha")
    ).when(
        F.col("AtividadeTipoId").isin('dissertativaPontuacao', 'dissertativa'),
        F.col("texto_resposta_dissertativa")
    ).when(
        F.col("AtividadeTipoId") == 'formulario',
        F.col("RespostaUsuarioFormulario")
    ).when(
        F.col("AtividadeTipoId") == 'minigame',
        F.col("RespostaUsuarioMinigame")
    )
).drop("RespostaUsuarioMultiplaEscolha", "RespostaUsuarioFormulario", "RespostaUsuarioMinigame") # Remove colunas temporárias

# display(resultado_final_parcial_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Cálculo do Status da Atividade
# MAGIC
# MAGIC A lógica final é determinar o status de conclusão de cada atividade para o usuário. Isso replica o complexo `CASE` statement da procedure.

# COMMAND ----------

# Adiciona informações de dispensa
resultado_com_dispensa_df = resultado_final_parcial_df.alias("RFP") \
    .join(
        dfs["dispensa_rodada"].alias("Dispensa"),
        (F.col("RFP.ModuloID") == F.col("Dispensa.id_rodada")) &
        (F.col("RFP.UsuarioID") == F.col("Dispensa.id_usuario")) &
        (F.col("RFP.ClienteId") == F.col("Dispensa.id_cliente")),
        "left"
    ) \
    .join( # Join com Rodada para pegar as datas de início e término
        dfs["rodada"].alias("Rodada"),
        (F.col("RFP.ModuloID") == F.col("Rodada.id")) &
        (F.col("RFP.ClienteId") == F.col("Rodada.id_cliente")),
        "left"
    ) \
    .join( # Join com AtividadeTipo para saber se requer correção
        dfs["atividade_tipo"].alias("AT"),
        F.col("RFP.AtividadeTipoId") == F.col("AT.id"),
        "left"
    ) \
    .select("RFP.*", "Dispensa.data_dispensa", "Rodada.data_inicio", "Rodada.data_termino", F.col("AT.flag_requer_correcao").alias("RequerCorrecao"))


# Aplica a lógica de status
agora = F.current_timestamp()

gold_df = resultado_com_dispensa_df.withColumn(
    "StatusUsuarioAtividade",
    F.when(F.col("data_dispensa").isNotNull(), "Dispensado")
     .when((F.col("DataConclusaoAtividade").isNull()) & (F.col("data_termino") < agora), "Expirado")
     .when((F.col("RespostaCorrigida") == 0) & (F.col("RequerCorrecao") == 1), "Aguardando Correção")
     .when((F.col("DataPrimeiroAcessoNaAtividade").isNotNull()) & (F.col("DataConclusaoAtividade").isNull()), "Em Andamento")
     .when((F.col("data_inicio") < agora) & (F.col("TentativaID").isNull()), "Não Iniciado")
     .when(F.col("data_inicio") > agora, "Não Liberado")
     .when(F.col("DataConclusaoAtividade").isNotNull(), "Concluído")
     .when(F.col("StatusTentativa").isin("aprovado", "concluido", "reprovado"), "Concluído")
     .otherwise("Não Iniciado")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Adicionar Carga Horária e Peso da Atividade

# COMMAND ----------

# Adiciona Carga Horária e Peso da Atividade
# Primeiro faz LEFT JOIN com RAU (usuário-específico)
gold_df_com_rau = gold_df.alias("G") \
    .join(
        dfs["rodada_atividade_usuario"].alias("RAU"),
        (F.col("G.ModuloID") == F.col("RAU.id_rodada")) &
        (F.col("G.ClienteId") == F.col("RAU.id_cliente")) &
        (F.col("G.AtividadeId") == F.col("RAU.id_atividade")) &
        (F.col("G.UsuarioID") == F.col("RAU.id_usuario")),
        "left"
    )

# Depois faz LEFT JOIN com RA (geral), mas só usa quando RAU não existe
gold_df_com_pesos = gold_df_com_rau \
    .join(
        dfs["rodada_atividade"].alias("RA"),
        (F.col("G.ModuloID") == F.col("RA.id_rodada")) &
        (F.col("G.ClienteId") == F.col("RA.id_cliente")) &
        (F.col("G.AtividadeId") == F.col("RA.id_atividade")),
        "left"
    ) \
    .join( # Join com Atividade para pegar a carga horária padrão
        dfs["atividade"].alias("Atividade"),
        F.col("G.AtividadeId") == F.col("Atividade.id"),
        "left"
    ) \
    .withColumn(
        "PesoAtividade", 
        # Usa RAU se existir, senão usa RA (conforme SQL linha 481: COALESCE(RAU.NU_PESO, RA.NU_PESO))
        F.when(F.col("RAU.numero_peso").isNotNull(), F.col("RAU.numero_peso"))
         .otherwise(F.col("RA.numero_peso"))
    ) \
    .withColumn(
        "CargaHorariaAtividade", F.col("Atividade.numero_carga_horaria")
    ) \
    .select("G.*", "PesoAtividade", "CargaHorariaAtividade")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Finalização e Salvamento
# MAGIC
# MAGIC Selecionamos e renomeamos as colunas finais para corresponder à saída da procedure e salvamos a tabela em formato Delta.

# COMMAND ----------

# Seleciona as colunas finais e formata alguns campos
gold_df_final = gold_df_com_pesos.select(
    "UsuarioID",
    "NomeUsuario",
    "EmailUsuario",
    "LoginUsuario",
    "StatusUsuario",
    "GrupoPaiId",
    "NomeGrupoPai",
    "GrupoFilhoId",
    "NomeGrupoFilho",
    #F.col("TodosGruposUsuario"), # Esta coluna será adicionada depois com a função do utils
    "AmbienteID",
    "NomeAmbiente",
    "TrilhaID",
    "NomeTrilha",
    "ModuloID",
    "NomeModulo",
    "AtividadeId",
    "NomeAtividade",
    F.when(F.col("StatusUsuarioAtividade") != 'Dispensado', F.col("TentativaID").cast("string")).otherwise(F.lit("")).alias("TentativaID"),
    F.when((F.col("StatusUsuarioAtividade") != 'Dispensado') & (F.col("CargaHorariaAtividade") > 0), F.col("CargaHorariaAtividade").cast("string")).otherwise(F.lit("")).alias("CargaHorariaAtividade"),
    "TipoAtividades",
    # TempoAcessoTotal - soma de todos os tempos de acesso (conforme esperado pelo cliente)
    F.when(
        (F.col("StatusUsuarioAtividade") != 'Dispensado') & (F.col("TempoAcessoTotalEmSegundos").isNotNull()), 
        F.concat(
            F.format_string("%02d", F.floor(F.col("TempoAcessoTotalEmSegundos") / 3600).cast("long")),
            F.lit(":"),
            F.format_string("%02d", F.floor((F.col("TempoAcessoTotalEmSegundos") % 3600) / 60).cast("long")),
            F.lit(":"),
            F.format_string("%02d", F.floor(F.col("TempoAcessoTotalEmSegundos") % 60).cast("long"))
        )
    ).otherwise(F.lit("")).alias("TempoAcessoTotal"),
    # QtdAcessosNaAtividade - contagem de acessos (conforme esperado pelo cliente)
    F.when(F.col("StatusUsuarioAtividade") != 'Dispensado', F.col("QtdAcessosNaAtividade").cast("string")).otherwise(F.lit("")).alias("QtdAcessosNaAtividade"),
    F.when(F.col("StatusUsuarioAtividade") != 'Dispensado', F.col("DataPrimeiroAcessoNaAtividade")).alias("DataPrimeiroAcessoNaAtividade"),
    F.when(F.col("StatusUsuarioAtividade") != 'Dispensado', F.col("DataUltimoAcessoNaAtividade")).alias("DataUltimoAcessoNaAtividade"),
    F.when(F.col("StatusUsuarioAtividade") != 'Dispensado', F.col("DataConclusaoAtividade")).alias("DataConclusaoAtividade"),
    F.when((F.col("StatusUsuarioAtividade") != 'Dispensado') & (F.col("AproveitamentoAtividade").isNotNull()), F.format_string("%.2f", (F.col("AproveitamentoAtividade") * 100).cast("double"))).otherwise(F.lit("")).alias("AproveitamentoAtividade"),
    "EnunciadoAtividade",
    F.when(F.col("StatusUsuarioAtividade") != 'Dispensado', F.col("RespostaUsuario")).otherwise(F.lit("")).alias("RespostaUsuario"),
    F.when(F.col("StatusUsuarioAtividade") != 'Dispensado', F.col("PesoAtividade").cast("string")).otherwise(F.lit("")).alias("PesoAtividade"),
    F.when(F.col("StatusUsuarioAtividade") != 'Dispensado', F.col("FeedbackAtividade")).otherwise(F.lit("")).alias("FeedbackAtividade"),
    "StatusUsuarioAtividade",
    "PerfilNaTrilha",
    "ClienteId", # Manter para filtros futuros
    "AtividadeTipoId", # Manter para lógica de respostas no snapshot
    "AmbienteAtivo",
    "TrilhaAtiva",
    "RodadaAtiva",
    "AtividadeAtiva",
    "TempoAcessoTotalEmSegundos"  # Manter para cálculos no analytics
).distinct()

# COMMAND ----------

# Salvar na tabela Gold
(gold_df_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ClienteId") # Particionar por ClienteID para performance
    .saveAsTable("prod.gold.relatorio_tempo_atividade_usuario"))

# COMMAND ----------

# display(gold_df_final)