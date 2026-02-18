# Databricks notebook source
# COMMAND ----------

from pyspark.sql.functions import col, when, lit, trim, regexp_replace, row_number, count, concat_ws, collect_list, max, udf, explode, split, first
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# COMMAND ----------

class EngageUtils:
    def __init__(self, spark):
        self.spark = spark

    def fnt_atributos_usuarios(self, clienteId: str):
        usuario = self.spark.table("prod.silver.usuario").alias("u")
        atributo = self.spark.table("prod.silver.atributo").alias("a")
        atributo_tipo = self.spark.table("prod.silver.atributo_tipo").alias("at")
        atributo_grupo_item = self.spark.table("prod.silver.atributo_grupo_item").alias("agi")
        entidade_atributo = self.spark.table("prod.silver.entidade_atributo").alias("ea")
        atributo_valor = self.spark.table("prod.silver.atributo_valor").alias("av")
        
        rotulo = self.spark.table("prod.silver.rotulo").alias("r")
        rotulo_idioma = self.spark.table("prod.silver.rotulo_idioma").alias("ri")
        rotulo_idioma_cliente = self.spark.table("prod.silver.rotulo_idioma_cliente").alias("ric")

        default_idioma_for_atributo_valor = 'pt-BR'

        atributo_valor_nome_lookup = (
            rotulo
            .join(rotulo_idioma, col("r.id") == col("ri.id_rotulo"))
            .join(
                rotulo_idioma_cliente,
                (col("r.id") == col("ric.id_rotulo"))
                & (col("ri.id_idioma") == col("ric.id_idioma"))
                & (col("ric.id_cliente") == clienteId),
                "left",
            )
            .withColumn(
                "TextoFinal",
                when(col("ric.texto_texto").isNotNull(), col("ric.texto_texto"))
                .otherwise(col("ri.texto_texto")),
            )
            .withColumn(
                "IdiomaFinal",
                when(col("ric.id_idioma").isNotNull(), col("ric.id_idioma"))
                .otherwise(col("ri.id_idioma")),
            )
            .withColumn(
                "rn_idioma",
                row_number().over(
                    Window.partitionBy(col("r.id")).orderBy(
                        when(col("IdiomaFinal") == default_idioma_for_atributo_valor, 1)
                        .when(col("IdiomaFinal").startswith(default_idioma_for_atributo_valor), 2)
                        .otherwise(4),
                        col("IdiomaFinal"),
                    )
                ),
            )
            .where(col("rn_idioma") == 1)
            .select(
                col("r.id").alias("ID_ROTULO_VALOR_lookup"),
                col("TextoFinal").alias("Texto_lookup"),
            )
        )

        window_spec = Window.partitionBy(
            col("ea.id_atributo"), col("ea.id_cliente"), col("ea.id_entidade"), col("ea.id_entidade_tipo")
        ).orderBy(col("ea.id").desc())

        cte = (
            usuario
            .join(
                entidade_atributo,
                (col("u.id") == col("ea.id_entidade"))
                & (col("u.id_cliente") == col("ea.id_cliente")),
                "left",
            )
            .join(
                atributo,
                (col("ea.id_atributo") == col("a.id"))
                & (col("ea.id_cliente") == col("a.id_cliente")),
                "left",
            )
            .join(
                atributo_tipo,
                col("a.id_atributo_tipo") == col("at.id"),
                "left",
            )
            .join(
                atributo_grupo_item,
                (col("a.id") == col("agi.id_atributo"))
                & (col("a.id_cliente") == col("agi.id_cliente")),
                "left",
            )
            .join(
                atributo_valor,
                (col("ea.id_atributo_valor") == col("av.id"))
                & (col("ea.id_cliente") == col("av.id_cliente")),
                "left",
            )
            .join(
                atributo_valor_nome_lookup,
                (col("av.id_rotulo_valor") == atributo_valor_nome_lookup.ID_ROTULO_VALOR_lookup),
                "left",
            )
            .where(
                (col("u.id_cliente") == clienteId)
                & (col("a.flag_status") == True)
                & (col("a.id_cliente") == clienteId)
                & (trim(col("ea.id_entidade_tipo")) == "US")
            )
            .withColumn("Atributo", trim(regexp_replace(col("a.texto_nome"), clienteId + "_", "")))
            .withColumn("RN", row_number().over(window_spec))
            .withColumn(
                "Valor",
                when(
                    col("at.texto_nome").isin("TEXT", "PHONE", "INTEGER", "DECIMAL"),
                    trim(col("ea.texto_valor").cast("string")),
                )
                .when(
                    col("at.texto_nome").isin("SELECT", "MULTIPLE"),
                    trim(col("Texto_lookup").cast("string")),
                )
                .when(col("at.texto_nome") == "DATE", col("ea.data_valor").cast("string")),
            )
            .select(
                col("u.id").alias("UsuarioId"),
                col("u.id_cliente").alias("ClienteId"),
                col("a.id").alias("AtributoId"),
                col("a.flag_status"),
                "Atributo",
                col("agi.numero_ordem").alias("NU_ORDEM"),
                "RN",
                "Valor",
                col("ea.id_atributo_valor")
            )
        )

        atributos_usuario = cte.where((col("RN") == 1) | col("Atributo").isNull())
        
        atributos_usuario_final = atributos_usuario.where(col("flag_status") == True) \
            .select(
                "UsuarioId",
                "ClienteId",
                "Atributo",
                col("Valor").alias("AtributoValor"),
                "AtributoId",
                "id_atributo_valor"
            )
            
        return atributos_usuario_final

    def fnt_configuracoes(self, clienteID: str):
        configuracao = self.spark.table("prod.silver.configuracao").alias("Configuracao")
        configuracao_cliente = self.spark.table("prod.silver.configuracao_cliente").alias("ConfiguracaoCliente")

        result = configuracao.join(
            configuracao_cliente,
            (configuracao.id == configuracao_cliente.id_configuracao) &
            (configuracao_cliente.id_cliente == clienteID),
            "left"
        ).select(
            col("Configuracao.id").alias("ConfiguracaoID"),
            when(col("ConfiguracaoCliente.id_cliente").isNull(), clienteID).otherwise(col("ConfiguracaoCliente.id_cliente")).alias("ClienteID"),
            col("Configuracao.texto_descricao").alias("Descricao"),
            when(col("ConfiguracaoCliente.texto_arquivo").isNull(), col("Configuracao.texto_arquivo")).otherwise(col("ConfiguracaoCliente.texto_arquivo")).alias("Arquivo"),
            when(col("ConfiguracaoCliente.flag_status").isNull(), col("Configuracao.flag_status")).otherwise(col("ConfiguracaoCliente.flag_status")).alias("Status"),
            when(col("ConfiguracaoCliente.flag_visivel").isNull(), col("Configuracao.flag_visivel")).otherwise(col("ConfiguracaoCliente.flag_visivel")).alias("Visivel"),
            when(col("ConfiguracaoCliente.id_configuracao").isNull(), lit(1)).otherwise(lit(0)).cast("boolean").alias("EstaUtilizandoConfiguracaoPadrao"),
            lit("").alias("TemaCliente"),
            col("Configuracao.texto_grupo").alias("RotuloGrupo"),
            col("Configuracao.numero_ordem").alias("OrdemConfiguracao"),
            col("Configuracao.texto_ajuda").alias("RotuloAjuda"),
            col("Configuracao.numero_ordem_grupo").alias("OrdemGrupo"),
            col("Configuracao.numero_ordem_configuracao").alias("OrdemConfig"),
            col("Configuracao.numero_ordem_grupo_configuracao").alias("OrdemGrupoConfig"),
            col("Configuracao.numero_tab_configuracao").alias("TabConfig"),
            col("Configuracao.texto_grupo_configuracao").alias("GrupoConfig")
        )
        return result

    def fnt_configuracoes_competicao(self, clienteID: str, competicaoID: int):
        configuracao_competicao = self.spark.table("prod.silver.configuracao_competicao").alias("ConfiguracaoCompeticao")
        configuracao_competicao_cliente = self.spark.table("prod.silver.configuracao_competicao_cliente").alias("ConfiguracaoCompeticaoCliente")

        result = configuracao_competicao.join(
            configuracao_competicao_cliente,
            (configuracao_competicao.id == configuracao_competicao_cliente.id_configuracao_competicao) &
            (configuracao_competicao_cliente.id_cliente == clienteID) &
            (configuracao_competicao_cliente.id_competicao == competicaoID),
            "left"
        ).select(
            col("ConfiguracaoCompeticao.id").alias("ConfiguracaoID"),
            when(col("ConfiguracaoCompeticaoCliente.id_cliente").isNull(), clienteID).otherwise(col("ConfiguracaoCompeticaoCliente.id_cliente")).alias("ClienteID"),
            when(col("ConfiguracaoCompeticaoCliente.id_competicao").isNull(), competicaoID).otherwise(col("ConfiguracaoCompeticaoCliente.id_competicao")).alias("CompeticaoID"),
            col("ConfiguracaoCompeticao.texto_descricao").alias("Descricao"),
            when(col("ConfiguracaoCompeticaoCliente.texto_arquivo").isNull(), col("ConfiguracaoCompeticao.texto_arquivo")).otherwise(col("ConfiguracaoCompeticaoCliente.texto_arquivo")).alias("Arquivo"),
            when(col("ConfiguracaoCompeticaoCliente.flag_status").isNull(), col("ConfiguracaoCompeticao.flag_status")).otherwise(col("ConfiguracaoCompeticaoCliente.flag_status")).alias("Status"),
            when(col("ConfiguracaoCompeticaoCliente.flag_visivel").isNull(), col("ConfiguracaoCompeticao.flag_visivel")).otherwise(col("ConfiguracaoCompeticaoCliente.flag_visivel")).alias("Visivel"),
            when(col("ConfiguracaoCompeticaoCliente.id_configuracao_competicao").isNull(), lit(1)).otherwise(lit(0)).cast("boolean").alias("EstaUtilizandoConfiguracaoPadrao"),
            col("ConfiguracaoCompeticao.texto_grupo").alias("RotuloGrupo"),
            col("ConfiguracaoCompeticao.numero_ordem").alias("OrdemConfiguracao"),
            col("ConfiguracaoCompeticao.texto_ajuda").alias("RotuloAjuda"),
            col("ConfiguracaoCompeticao.numero_ordem_grupo").alias("OrdemGrupo"),
            col("ConfiguracaoCompeticao.texto_grupo_configuracao"),
            col("ConfiguracaoCompeticao.numero_ordem_configuracao"),
            col("ConfiguracaoCompeticao.numero_ordem_grupo_configuracao")
        )
        return result

    def fnt_converter_filtros_para_tabela(self, inputString: str, tipo: str):
        if not inputString:
            # Return an empty DataFrame with the correct schema
            schema = "OriginalValue STRING, ConvertedValue STRING, Tipo STRING"
            return self.spark.createDataFrame([], schema)

        # Create a DataFrame from the input string
        df = self.spark.createDataFrame([(inputString,)], ["value"])
        # Explode the comma-separated string into multiple rows
        df = df.withColumn("value", explode(split(col("value"), ",")))
        
        # Initialize ConvertedValue with the original value
        df = df.withColumn("ConvertedValue", col("value"))

        if tipo == 'perfilUsuarioTrilhaIds':
            df = df.withColumn(
                "ConvertedValue",
                when(col("value") == "1", "Obrigatório")
                .when(col("value") == "2", "Participa")
                .when(col("value") == "3", "Gestor")
                .otherwise("Desconhecido"),
            )
        elif tipo == 'statusConclusaoCompeticao':
            df = df.withColumn(
                "ConvertedValue",
                when(trim(col("value")) == "finished", "Concluído")
                .when(trim(col("value")) == "expired", "Expirado (Não concluído)")
                .when(trim(col("value")) == "in_progress", "Em andamento")
                .when(trim(col("value")) == "not_started", "Não iniciado")
                .when(trim(col("value")) == "not_released", "Não liberado")
                .when(trim(col("value")) == "exempted", "Dispensado")
                .otherwise("Desconhecido"),
            )
        elif tipo == 'statusConclusaoRodada':
            df = df.withColumn(
                "ConvertedValue",
                when(trim(col("value")) == "approved", "Aprovado")
                .when(trim(col("value")) == "reproved", "Reprovado")
                .when(trim(col("value")) == "out_of_date", "Fora do Prazo")
                .when(trim(col("value")) == "expired", "Expirado (Não Realizado)")
                .when(trim(col("value")) == "awaiting_approval", "Aguardando Correção")
                .when(trim(col("value")) == "in_progress", "Em Andamento")
                .when(trim(col("value")) == "not_started", "Não Iniciado")
                .when(trim(col("value")) == "not_released", "Não Liberado")
                .when(trim(col("value")) == "exempted", "Dispensado")
                .when(trim(col("value")) == "invalid_certification", "Certificação Inválida")
                .when(trim(col("value")) == "valid_certification", "Certificação Válida")
                .otherwise("Desconhecido"),
            )
        elif tipo == 'statusConclusaoAtividade':
            df = df.withColumn(
                "ConvertedValue",
                when(trim(col("value")) == "finished", "Concluído")
                .when(trim(col("value")) == "expired", "Expirado")
                .when(trim(col("value")) == "awaiting_approval", "Aguardando Correção")
                .when(trim(col("value")) == "in_progress", "Em Andamento")
                .when(trim(col("value")) == "not_started", "Não Iniciado")
                .when(trim(col("value")) == "not_released", "Não Liberado")
                .when(trim(col("value")) == "exempted", "Dispensado")
                .otherwise("Desconhecido"),
            )
        elif tipo == 'statusConclusaoTrilha':
            df = df.withColumn(
                "ConvertedValue",
                when(trim(col("value")) == "finished", "Concluído")
                .when(trim(col("value")) == "expired", "Expirado (Não Concluído)")
                .when(trim(col("value")) == "in_progress", "Em Andamento")
                .when(trim(col("value")) == "not_started", "Não Iniciado")
                .when(trim(col("value")) == "not_released", "Não Liberado")
                .when(trim(col("value")) == "exempted", "Dispensado")
                .when(trim(col("value")) == "invalid_certification", "Certificação Inválida")
                .when(trim(col("value")) == "valid_certification", "Certificação Válida")
                .otherwise("Desconhecido"),
            )
        elif tipo == 'statusPergunta':
            df = df.withColumn(
                "ConvertedValue",
                when(trim(col("value")) == "pending", "Pendente")
                .when(trim(col("value")) == "answered", "Respondida")
                .otherwise("Desconhecido"),
            )
        elif tipo == 'statusEnvioNotificacao':
            df = df.withColumn(
                "ConvertedValue",
                when(trim(col("value")) == "summary", "Rascunho")
                .when(trim(col("value")) == "wating_for_sending", "Aguardando envio")
                .when(trim(col("value")) == "failed", "Falha")
                .when(trim(col("value")) == "sent", "Enviado")
                .otherwise("Desconhecido"),
            )
        elif tipo == 'tipoTentativa':
            df = df.withColumn(
                "ConvertedValue",
                when(trim(col("value")) == "recertification", "Recertificação")
                .when(trim(col("value")) == "default", "Padrão")
                .otherwise("Desconhecido"),
            )
        elif tipo == 'statusUsuario':
            df = df.withColumn(
                "ConvertedValue",
                when(col("value") == "1", "Ativo")
                .when(col("value") == "2", "Inativo")
                .otherwise("Desconhecido"),
            )
        
        df = df.withColumn("Tipo", lit(tipo))
        return df.select(col("value").alias("OriginalValue"), "ConvertedValue", "Tipo")

    def fnt_entidades_editaveis(self, adminId: int, clienteId: str, entidadeTipoID: str, grupos):
        perfil_admin_tbl = self.spark.table("prod.silver.perfil_admin")
        perfil_usuario_tbl = self.spark.table("prod.silver.perfil_usuario")
        entidade_grupo_tbl = self.spark.table("prod.silver.entidade_grupo")
        grupo_tbl = self.spark.table("prod.silver.grupo")
        cache_arvore_grupo_tbl = self.spark.table("prod.silver.cache_arvore_grupo")
        perfil_grupo_acesso_tbl = self.spark.table("prod.silver.perfil_grupo_acesso")
        usuario_perfil_entidade_tbl = self.spark.table("prod.silver.usuario_perfil_entidade")
        rodada_tbl = self.spark.table("prod.silver.rodada")
        competicao_trilha_tbl = self.spark.table("prod.silver.competicao_trilha")
        usuario_tbl = self.spark.table("prod.silver.usuario")
        vUsuarioGrupo_tbl = self.spark.table("prod.silver.usuario_grupo").select("id_usuario", "id_cliente", "id_grupo").distinct()
        competicao_tbl = self.spark.table("prod.silver.competicao")
        competicao_grupo_tbl = self.spark.table("prod.silver.competicao_grupo")
        atividade_tbl = self.spark.table("prod.silver.atividade")
        conquista_tbl = self.spark.table("prod.silver.conquista")

        perfil_usuario_base = perfil_admin_tbl.alias("Perfil") \
            .join(
                perfil_usuario_tbl.alias("PerfilUsuario"),
                (col("PerfilUsuario.id_perfil") == col("Perfil.id")) &
                (col("PerfilUsuario.id_cliente") == col("Perfil.id_cliente")),
            ) \
            .where(
                (col("Perfil.flag_status") == True) &
                (col("Perfil.id_cliente") == clienteId) &
                (col("PerfilUsuario.id_usuario") == adminId)
            )

        pode_visualizar_agg = perfil_usuario_base \
            .agg(max(col("Perfil.flag_permitir_visualizar_entidades_sem_grupo")).alias("PermitirVisualizarEntidadesSemGrupos")) \
            .first()
        
        permitir_sem_grupo = pode_visualizar_agg["PermitirVisualizarEntidadesSemGrupos"] if pode_visualizar_agg else 0
        
        perfil_usuario_logado = perfil_usuario_base \
            .withColumn("PermitirVisualizarEntidadesSemGrupos", lit(permitir_sem_grupo)) \
            .withColumn("order_col",
                when(col("Perfil.id_perfil_grupo_acesso_tipo") == 'any_group', 1)
                .when(col("Perfil.id_perfil_grupo_acesso_tipo") == 'group_structure_top_down', 2)
                .when(col("Perfil.id_perfil_grupo_acesso_tipo") == 'specific_groups', 3)
                .otherwise(4)
            ) \
            .orderBy("order_col") \
            .limit(1) \
            .select(
                col("Perfil.id").alias("PerfilId"),
                col("Perfil.id_perfil_grupo_acesso_tipo").alias("PerfilGrupoAcessoTipoId"),
                col("PermitirVisualizarEntidadesSemGrupos")
            ).cache()

        if perfil_usuario_logado.count() == 0:
            return self.spark.createDataFrame([], "EntidadeTipoId STRING, EntidadeId INT, ClienteID STRING, Vinculado BOOLEAN")

        perfil_logado_row = perfil_usuario_logado.first()
        perfil_id = perfil_logado_row["PerfilId"]
        perfil_grupo_acesso_tipo_id = perfil_logado_row["PerfilGrupoAcessoTipoId"]
        permitir_sem_grupo = perfil_logado_row["PermitirVisualizarEntidadesSemGrupos"]

        entidades_de_acesso_list = []

        if perfil_grupo_acesso_tipo_id == 'any_group':
            df = entidade_grupo_tbl.alias("eg") \
                .join(grupo_tbl.alias("g"), (col("g.id") == col("eg.id_grupo")) & (col("g.id_cliente") == col("eg.id_cliente")))\
                .where(
                    (trim(col("eg.id_entidade_tipo")) == entidadeTipoID) &
                    (col("eg.id_cliente") == clienteId) &
                    (col("g.flag_status") == True)
                ) \
                .select(
                    col("eg.id_entidade_tipo").alias("EntidadeTipoId"),
                    col("eg.id_entidade").alias("EntidadeId"),
                    col("eg.id_cliente").alias("ClienteID"),
                    lit(True).alias("Vinculado")
                ).distinct()
            entidades_de_acesso_list.append(df)

        if perfil_grupo_acesso_tipo_id == 'group_structure_top_down':
            admin_grupos = entidade_grupo_tbl \
                .where((col("id_entidade") == adminId) & (trim(col("id_entidade_tipo")) == 'US') & (col("id_cliente") == clienteId)) \
                .select("id_grupo")
            
            descendant_groups = cache_arvore_grupo_tbl.alias("cag") \
                .join(admin_grupos, col("cag.id_root") == admin_grupos.id_grupo, "inner") \
                .select("cag.id_grupo").distinct()

            df = entidade_grupo_tbl.alias("eg") \
                .join(descendant_groups, col("eg.id_grupo") == descendant_groups.id_grupo, "inner") \
                .join(grupo_tbl.alias("g"), (col("g.id") == col("eg.id_grupo")) & (col("g.id_cliente") == col("eg.id_cliente")))\
                .where(
                    (trim(col("eg.id_entidade_tipo")) == entidadeTipoID) &
                    (col("eg.id_cliente") == clienteId) &
                    (col("g.flag_status") == True)
                ) \
                .select(
                    col("eg.id_entidade_tipo").alias("EntidadeTipoId"),
                    col("eg.id_entidade").alias("EntidadeId"),
                    col("eg.id_cliente").alias("ClienteID"),
                    lit(True).alias("Vinculado")
                ).distinct()
            entidades_de_acesso_list.append(df)

        if perfil_grupo_acesso_tipo_id == 'specific_groups':
            perfil_grupos = perfil_grupo_acesso_tbl \
                .where((col("id_perfil") == perfil_id) & (col("id_cliente") == clienteId)) \
                .select("id_grupo")
            
            base_df = entidade_grupo_tbl.alias("eg") \
                .join(perfil_grupos, col("eg.id_grupo") == perfil_grupos.id_grupo, "inner") \
                .join(grupo_tbl.alias("g"), (col("g.id") == col("eg.id_grupo")) & (col("g.id_cliente") == col("eg.id_cliente")))\
                .where(
                    (trim(col("eg.id_entidade_tipo")) == entidadeTipoID) &
                    (col("eg.id_cliente") == clienteId) &
                    (col("g.flag_status") == True)
                )
            
            if entidadeTipoID == 'CP':
                usuario_perfil_trilha_df = usuario_perfil_entidade_tbl.alias("UPE") \
                    .join(rodada_tbl.alias("RD"), 
                        (col("UPE.id_cliente") == col("RD.id_cliente")) &
                        (col("UPE.id_entidade") == col("RD.id")) &
                        (trim(col("UPE.id_entidade_tipo")) == "RD") &
                        (col("UPE.id_perfil_jogo").isin([1, 2, 3])) &
                        (col("RD.flag_status") == True)) \
                    .where((col("UPE.id_usuario") == adminId) & (col("UPE.id_cliente") == clienteId)) \
                    .select(col("RD.id_trilha"), col("UPE.id_cliente"))

                competicao_trilha_filter_df = competicao_trilha_tbl.alias("CT") \
                    .join(usuario_perfil_trilha_df, 
                        (col("CT.id_trilha") == usuario_perfil_trilha_df.id_trilha) &
                        (col("CT.id_cliente") == usuario_perfil_trilha_df.id_cliente)) \
                    .select("id_competicao", "id_cliente")

                base_df = base_df.join(competicao_trilha_filter_df,
                    (col("eg.id_entidade") == competicao_trilha_filter_df.id_competicao) &
                    (col("eg.id_cliente") == competicao_trilha_filter_df.id_cliente), "semi")

            df = base_df.select(
                    col("eg.id_entidade_tipo").alias("EntidadeTipoId"),
                    col("eg.id_entidade").alias("EntidadeId"),
                    col("eg.id_cliente").alias("ClienteID"),
                    lit(True).alias("Vinculado")
                ).distinct()
            entidades_de_acesso_list.append(df)

        if permitir_sem_grupo == 1:
            if entidadeTipoID == "US":
                unique_users = usuario_tbl.select("id", "id_cliente", "flag_padrao").distinct()
                df = unique_users.alias("U") \
                    .join(vUsuarioGrupo_tbl.alias("VUG"),
                        (col("U.id") == col("VUG.id_usuario")) & (col("U.id_cliente") == col("VUG.id_cliente")),"left_anti") \
                    .where((col("U.id_cliente") == clienteId) & (col("U.flag_padrao") == 0)) \
                    .select(lit("US").alias("EntidadeTipoId"), col("U.id").alias("EntidadeId"), col("U.id_cliente").alias("ClienteID"), lit(False).alias("Vinculado")) \
                    .distinct()
                entidades_de_acesso_list.append(df)
            
            if entidadeTipoID == "CP":
                 df = competicao_tbl.alias("C") \
                    .join(competicao_grupo_tbl.alias("CG_sub") \
                    .join(grupo_tbl.alias("G_sub"), (col("G_sub.id") == col("CG_sub.id_grupo")) & (col("G_sub.id_cliente") == col("CG_sub.id_cliente")) & (col("G_sub.flag_status") == True), "inner"),
                          (col("C.id") == col("CG_sub.id_competicao")) & (col("C.id_cliente") == col("CG_sub.id_cliente")),"left_anti") \
                    .where(col("C.id_cliente") == clienteId) \
                    .select(lit("CP").alias("EntidadeTipoId"), col("C.id").alias("EntidadeId"), col("C.id_cliente").alias("ClienteID"), lit(False).alias("Vinculado"))
                 entidades_de_acesso_list.append(df)

            if entidadeTipoID == "AT":
                active_groups_in_entity = entidade_grupo_tbl.alias("EG_sub_act") \
                    .join(grupo_tbl.alias("G_sub_act"), (col("G_sub_act.id") == col("EG_sub_act.id_grupo")) & (col("G_sub_act.id_cliente") == col("EG_sub_act.id_cliente")) & (col("G_sub_act.flag_status") == True), "inner") \
                    .where(trim(col("EG_sub_act.id_entidade_tipo")) == "AT") & (col("EG_sub_act.id_cliente") == clienteId)
                
                df = atividade_tbl.alias("A") \
                    .join(active_groups_in_entity,
                          (col("A.id") == active_groups_in_entity.id_entidade) & (col("A.id_cliente") == active_groups_in_entity.id_cliente),"left_anti") \
                    .where(col("A.id_cliente") == clienteId) \
                    .select(lit("AT").alias("EntidadeTipoId"), col("A.id").alias("EntidadeId"), col("A.id_cliente").alias("ClienteID"), lit(False).alias("Vinculado"))
                entidades_de_acesso_list.append(df)

            if entidadeTipoID == "CQ":
                active_groups_in_entity = entidade_grupo_tbl.alias("EG_sub_con") \
                    .join(grupo_tbl.alias("G_sub_con"), (col("G_sub_con.id") == col("EG_sub_con.id_grupo")) & (col("G_sub_con.id_cliente") == col("EG_sub_con.id_cliente")) & (col("G_sub_con.flag_status") == True), "inner") \
                    .where(trim(col("EG_sub_con.id_entidade_tipo")) == "CQ") & (col("EG_sub_con.id_cliente") == clienteId)

                df = conquista_tbl.alias("CQ") \
                    .join(active_groups_in_entity,
                          (col("CQ.id") == active_groups_in_entity.id_entidade) & (col("CQ.id_cliente") == active_groups_in_entity.id_cliente),"left_anti") \
                    .where(col("CQ.id_cliente") == clienteId) \
                    .select(lit("CQ").alias("EntidadeTipoId"), col("CQ.id").alias("EntidadeId"), col("CQ.id_cliente").alias("ClienteID"), lit(False).alias("Vinculado"))
                entidades_de_acesso_list.append(df)

        if not entidades_de_acesso_list:
            return self.spark.createDataFrame([], "EntidadeTipoId STRING, EntidadeId INT, ClienteID STRING, Vinculado BOOLEAN")

        from functools import reduce
        entidades_de_acesso_df = reduce(lambda df1, df2: df1.unionByName(df2.select(df1.columns)), entidades_de_acesso_list)
        
        entidades_de_acesso_df = entidades_de_acesso_df.distinct()

        window_spec = Window.partitionBy("EntidadeId", "ClienteID").orderBy(when(col("Vinculado"), 1).otherwise(2))
        resultado = entidades_de_acesso_df \
            .withColumn("RN", row_number().over(window_spec)) \
            .where(col("RN") == 1) \
            .select("EntidadeTipoId", "EntidadeId", "ClienteID", "Vinculado")
    
        return resultado

    def fnt_rotulo_idioma_por_id(self, rotuloID: int, clienteID: str, idiomaID: str):
        rotulo = self.spark.table("prod.silver.rotulo").alias("Rotulo")
        rotulo_idioma = self.spark.table("prod.silver.rotulo_idioma").alias("RotuloIdioma")
        rotulo_idioma_cliente = self.spark.table("prod.silver.rotulo_idioma_cliente").alias("RotuloIdiomaCliente")

        window_spec = Window.partitionBy("id_rotulo").orderBy(
            when(col("IdiomaFinal") == idiomaID, 1)
            .when(col("IdiomaFinal").like(f"{idiomaID}%"), 2)
            .otherwise(4),
            "IdiomaFinal",
        )

        cte = (
            rotulo.join(
                rotulo_idioma, rotulo.id == rotulo_idioma.id_rotulo
            )
            .join(
                rotulo_idioma_cliente,
                (rotulo.id == rotulo_idioma_cliente.id_rotulo)
                & (rotulo_idioma.id_idioma == rotulo_idioma_cliente.id_idioma)
                & (rotulo_idioma_cliente.id_cliente == clienteID),
                "left",
            )
            .where(rotulo.id == rotuloID)
            .withColumn(
                "TextoFinal",
                when(
                    col("RotuloIdiomaCliente.texto_texto").isNotNull(),
                    col("RotuloIdiomaCliente.texto_texto"),
                ).otherwise(col("RotuloIdioma.texto_texto")),
            )
            .withColumn(
                "IdiomaFinal",
                when(
                    col("RotuloIdiomaCliente.id_idioma").isNotNull(),
                    col("RotuloIdiomaCliente.id_idioma"),
                ).otherwise(col("RotuloIdioma.id_idioma")),
            )
            .withColumn("RowNumber", row_number().over(window_spec))
            .where(col("RowNumber") == 1)
            .select(
                col("Rotulo.id").alias("RotuloID"),
                col("Rotulo.texto_chave").alias("Chave"),
                col("IdiomaFinal").alias("IdiomaID"),
                col("TextoFinal").alias("Texto"),
                col("Rotulo.flag_padrao").alias("IsDefault"),
            )
        )
        return cte

    def fnt_todos_grupos_usuarios(self, usuarios_df, gruposId: str = None):
        """
        Calculates a comma-separated string of all group names for each user in a given DataFrame.

        Args:
            usuarios_df: A DataFrame containing the user IDs, typically with columns 'UsuarioID' and 'ClienteID'.
            gruposId: An optional comma-separated string of group IDs to filter the results.

        Returns:
            A DataFrame with 'UsuarioID', 'ClienteID', and 'TodosGruposUsuario' columns.
        """
        usuario_grupo = self.spark.table("prod.silver.usuario_grupo").alias("ug")
        grupo = self.spark.table("prod.silver.grupo").alias("g")
        cache_arvore_grupo = self.spark.table("prod.silver.cache_arvore_grupo").alias("cag")

        # Use a separate alias for the aggregation to avoid ambiguity
        cag_for_agg = self.spark.table("prod.silver.cache_arvore_grupo").alias("cag_for_agg")
        grp_hierarchy = cag_for_agg.groupBy("id_grupo", "id_cliente").agg(count("* ").alias("DEPTH"))

        cte_grupos = None
        if gruposId:
            try:
                grupos_list = [int(g.strip()) for g in gruposId.split(',') if g.strip().isdigit()]
                if grupos_list:
                    pass
            except (ValueError, AttributeError):
                pass

        base_user_groups = usuario_grupo.join(
            usuarios_df,
            (col("ug.id_usuario") == usuarios_df.UsuarioID) & (col("ug.id_cliente") == usuarios_df.ClienteID),
            "inner"
        )

        cte = (
            base_user_groups
            .join(
                grupo,
                (col("ug.id_grupo") == col("g.id")) & (col("ug.id_cliente") == col("g.id_cliente")),
            )
            .join(
                cache_arvore_grupo,
                (col("g.id") == col("cag.id_grupo")) & (col("g.id_cliente") == col("cag.id_cliente")),
                "left",
            )
            .join(
                grp_hierarchy,
                (col("cag.id_grupo") == grp_hierarchy.id_grupo) & (col("cag.id_cliente") == grp_hierarchy.id_cliente),
                "left"
            )
            .where(col("g.flag_status") == True)
        )
        
        if cte_grupos:
            cte = cte.join(cte_grupos, on=["id_grupo", "id_cliente"], how="inner")

        window_spec = Window.partitionBy("ug.id_usuario", "ug.id_grupo", "ug.id_cliente").orderBy("cag.numero_saltos", "DEPTH")
        
        cte_with_rn = cte.withColumn("RN", row_number().over(window_spec))
        
        result = (
            cte_with_rn
            .where(col("RN") == 1)
            .orderBy("cag.numero_saltos", "DEPTH", "cag.id_pai")
            .groupBy("ug.id_usuario", "ug.id_cliente")
            .agg(concat_ws(", ", collect_list(col("g.texto_nome"))).alias("TodosGruposUsuario"))
            .select(
                 col("ug.id_usuario").alias("UsuarioID"),
                 col("ug.id_cliente").alias("ClienteID"),
                 "TodosGruposUsuario"
            )
        )

        return result

    def get_attribute_names(self, clienteID: str):
        """
        Busca os nomes dos atributos ativos para um cliente específico, que serão usados como colunas no PIVOT.
        A lógica replica a obtenção de @fieldNames na procedure original.
        """
        atributo_df = self.spark.table("prod.silver.atributo")
        
        attribute_names_df = atributo_df.where(
            (col("id_cliente") == clienteID) & 
            (col("flag_status") == True)
        ).select(
            regexp_replace(col("texto_nome"), f"{clienteID}_", "").alias("Atributo")
        ).distinct().orderBy("Atributo")

        # Collect the names into a list of strings
        attribute_names = [row.Atributo for row in attribute_names_df.collect()]
        
        return attribute_names
