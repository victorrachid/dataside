USE [prod_my_engage_autosservico]
GO

/****** Object:  StoredProcedure [Report].[engagesp_Relatorio_Trilhas_Finalizadas_Usuario]    Script Date: 10/27/2025 7:16:18 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [Report].[engagesp_Relatorio_Trilhas_Finalizadas_Usuario]
 	@adminId		INT =  NULL
	, @clienteID	NVARCHAR(24)  = NULL
	, @competicaoId	NVARCHAR(MAX) = NULL
	, @trilhaId		NVARCHAR(MAX) =  NULL
	, @userStatus	NVARCHAR(20) = '1'
	, @perfilUsuarioTrilhaIds NVARCHAR (20) = '1,2'
	, @grupo NVARCHAR(MAX) = NULL
	, @usuario NVARCHAR(MAX) = NULL
	, @tipoDeTentativa NVARCHAR(24) = NULL
	, @trackStartDateStartDate DATETIME = NULL
	, @trackStartDateEndDate DATETIME = NULL
	, @trackCompletionDateStartDate DATETIME = NULL
	, @trackCompletionDateEndDate DATETIME = NULL
	, @trackLinkDateStartDate DATETIME = NULL
	, @trackLinkDateEndDate DATETIME = NULL
	, @trackExpirationDateStartDate DATETIME = NULL
	, @trackExpirationDateEndDate DATETIME = NULL
	, @showInactiveCompetition BIT = 0
	, @showInactiveTrack BIT = 0
	, @showEmptyDates BIT = 0 
	, @statusConclusaoTrilha NVARCHAR(124) = NULL
AS



	DECLARE @subgrupos SubGrupo;
	   
	   DROP TABLE IF EXISTS #UsuariosAdmin
	   DROP TABLE IF EXISTS #CompeticoesAdmin
	   DROP TABLE IF EXISTS #UsuarioCompeticao;
	   DROP TABLE IF EXISTS #UsuarioModulosConcluidos
	   DROP TABLE IF EXISTS #Resultado
	   DROP TABLE IF EXISTS #TempAtributosUsuario
	   
	   ;WITH CTE
	   AS
	   (
			SELECT EntidadeId AS UsuarioID
				 , ClienteID
			  FROM fnt_EntidadesEditaveis(@adminId, @clienteID, 'US', @subgrupos) AS EntidadesEditaveis		
	   )
	   , DetalhesUsuario
	   AS
	   (
			SELECT CTE.UsuarioID
				 , CTE.ClienteID
				 , Usuario.TX_LOGIN
				 , Usuario.TX_EMAIL
				 , Usuario.TX_NOME_COMPLETO
				 , Usuario.FL_STATUS AS StatusUsuario
				 , Usuario.DT_CADASTRO AS DataCadastroUsuario
			  FROM CTE
			  JOIN USUARIO AS Usuario (NOLOCK)
				ON Usuario.ID = CTE.UsuarioID
			   AND Usuario.ID_CLIENTE = CTE.ClienteID
			 WHERE Usuario.FL_PADRAO = 0
			   AND Usuario.DT_EXCLUSAO IS NULL
			   AND (ISNULL(@userStatus, '') = '' OR Usuario.FL_STATUS IN ((SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@userStatus, 'statusUsuario'))))
			   AND (ISNULL(@usuario, '') = '' OR  Usuario.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@usuario, 'usuario'))) 
	   ) SELECT DetalhesUsuario.*
		      , Grupos.TodosGruposUsuario
	       INTO #UsuariosAdmin
	       FROM DetalhesUsuario
    CROSS APPLY fnt_TodosGruposUsuarios (DetalhesUsuario.UsuarioID, DetalhesUsuario.ClienteID, @grupo) AS Grupos   
		CREATE UNIQUE CLUSTERED INDEX IX_UsuarioAdmin ON #UsuariosAdmin (UsuarioID, ClienteId)
	   

	    SELECT EntidadeId AS CompeticaoId
		     , ClienteID
		 INTO #CompeticoesAdmin
	     FROM fnt_EntidadesEditaveis (@adminId, @clienteID, 'CP', @subgrupos) AS EntidadesEditaveis;
		 CREATE CLUSTERED INDEX IX_CompeticoesAdmin ON #CompeticoesAdmin (CompeticaoId, ClienteId)

		 DECLARE @permitePontuarForaDoPrazo CHAR(1) = (SELECT IIF (PontuarForaDoPrazo.[Status] = 1, 'Y', 'N')
														 FROM fnt_Configuracoes (@clienteID) AS PontuarForaDoPrazo
														WHERE PontuarForaDoPrazo.ConfiguracaoID = N'allow_score_for_late_answer');
	
				;WITH UsuarioCompeticao
				AS
				(
						SELECT UsuariosAdmin.TX_LOGIN
							 , UsuariosAdmin.TX_EMAIL
							 , UsuariosAdmin.TX_NOME_COMPLETO
							 , UsuariosAdmin.StatusUsuario
							 , UsuariosAdmin.DataCadastroUsuario
							 , UsuarioTrilha.ID_USUARIO
							 , UsuarioTrilha.ID_CLIENTE
							 , UsuarioTrilha.ID_TRILHA
							 , UsuarioTrilha.ID_COMPETICAO
							 , UsuarioTrilha.ID_TENTATIVA_RECERTIFICACAO
							 , UsuarioTrilha.DT_PRIMEIRO_ACESSO
							 , UsuarioTrilha.DT_CONCLUSAO
							 , UsuarioTrilha.NU_PONTUACAO
							 , UsuarioTrilha.NU_CARGA_HORARIA
							 , UsuarioTrilha.NU_TEMPO_ACESSO_EM_SEGUNDOS
							 , CASE WHEN UsuarioTrilha.NU_TOTAL_RODADAS_DISPENSADAS > 0 THEN (UsuarioTrilha.NU_TOTAL_RODADAS - UsuarioTrilha.NU_TOTAL_RODADAS_DISPENSADAS)
									ELSE UsuarioTrilha.NU_TOTAL_RODADAS 
								END AS NU_TOTAL_RODADAS
							 , UsuarioTrilha.NU_TOTAL_RODADAS_AGUARDANDO_CORRECAO
							 , UsuarioTrilha.NU_TOTAL_RODADAS_APROVADAS
							 , UsuarioTrilha.NU_TOTAL_RODADAS_CONCLUIDAS
							 , UsuarioTrilha.NU_TOTAL_RODADAS_DISPENSADAS
							 , UsuarioTrilha.NU_TOTAL_RODADAS_EM_ANDAMENTO
							 , UsuarioTrilha.NU_TOTAL_RODADAS_EXPIRADAS
							 , UsuarioTrilha.NU_TOTAL_RODADAS_NAO_INICIADAS
							 , UsuarioTrilha.NU_TOTAL_RODADAS_NAO_LIBERADAS
							 , UsuarioTrilha.NU_TOTAL_RODADAS_REALIZADAS_FORA_DO_PRAZO
							 , UsuarioTrilha.NU_TOTAL_RODADAS_REPROVADAS
							 , IIF(UsuarioTrilha.NU_TOTAL_RODADAS_DISPENSADAS >= UsuarioTrilha.NU_TOTAL_RODADAS, 1, 0) AS Dispensado
							 , RODADA.ID AS ID_RODADA
							 , ROW_NUMBER() OVER (PARTITION BY UsuarioTrilha.ID_USUARIO
															 , UsuarioTrilha.ID_CLIENTE
															 , UsuarioTrilha.ID_TRILHA
													  ORDER BY UsuarioTrilha.ID ASC) AS RN
						  FROM USUARIO_PROGRESSO_TRILHA AS UsuarioTrilha (NOLOCK)
						  JOIN RODADA (NOLOCK)
						    ON RODADA.ID_TRILHA = UsuarioTrilha.ID_TRILHA
						   AND RODADA.ID_CLIENTE = UsuarioTrilha.ID_CLIENTE
						   AND RODADA.FL_STATUS = 1
						  JOIN #UsuariosAdmin AS UsuariosAdmin
		 				    ON UsuariosAdmin.UsuarioId = UsuarioTrilha.ID_USUARIO
		 				   AND UsuariosAdmin.ClienteId = UsuarioTrilha.ID_CLIENTE
						 WHERE (ISNULL(@competicaoId, '') = '' OR UsuarioTrilha.ID_COMPETICAO IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@competicaoId, 'competicaoId')))
						   AND (ISNULL(@trilhaId, '') = '' OR UsuarioTrilha.ID_TRILHA IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@trilhaId, 'trilhaId')))
				) SELECT UsuarioCompeticao.*
					   , CASE WHEN UsuarioPerfilEntidade.ID_PERFIL_JOGO = 1 THEN 'Obrigatório' 
							  WHEN UsuarioPerfilEntidade.ID_PERFIL_JOGO = 2 THEN 'Participa' 
							  WHEN UsuarioPerfilEntidade.ID_PERFIL_JOGO = 3 THEN 'Gestor' 
						  END AS PerfilTrilha
					   , CASE UsuarioPerfilEntidade.ID_ORIGEM_CADASTRO WHEN 1 THEN 'Manual'
																	   WHEN 2 THEN 'Público Alvo'
																	   WHEN 3 THEN 'API'
																	   WHEN 4 THEN 'Importação de Dados' 
						  END AS OrigemVinculoTrilha
					   , CRIADO_POR
					   , DataVinculoTrilha
				    INTO #UsuarioCompeticao
				    FROM UsuarioCompeticao
			 CROSS APPLY (SELECT TOP 1 MAX(UPE.ID_PERFIL_JOGO) AS ID_PERFIL_JOGO
							   , ISNULL(UPE.ID_ORIGEM_CADASTRO, UPE.ID_ORIGEM_ULTIMA_ATUALIZACAO) AS ID_ORIGEM_CADASTRO
							   , ISNULL(UPE.CRIADO_POR, UPE.ATUALIZADO_POR) AS CRIADO_POR
							   , MIN(UPE.DT_CADASTRO) AS DataVinculoTrilha
				            FROM USUARIO_PERFIL_ENTIDADE AS UPE (NOLOCK)
						   WHERE UPE.ID_USUARIO = UsuarioCompeticao.ID_USUARIO
							 AND UPE.ID_CLIENTE = UsuarioCompeticao.ID_CLIENTE
							 AND UPE.ID_ENTIDADE = UsuarioCompeticao.ID_RODADA
							 AND UPE.ID_ENTIDADE_TIPO = 'RD'
							 AND UPE.ID_PERFIL_JOGO IN (1, 2, 3)
					    GROUP BY UPE.ID_ORIGEM_CADASTRO
							   , UPE.ID_ORIGEM_ULTIMA_ATUALIZACAO
							   , UPE.CRIADO_POR
							   , UPE.ATUALIZADO_POR) AS UsuarioPerfilEntidade
				   WHERE UsuarioCompeticao.RN = 1
				     AND (ISNULL(@perfilUsuarioTrilhaIds, '') = '' OR UsuarioPerfilEntidade.ID_PERFIL_JOGO IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@perfilUsuarioTrilhaIds, 'perfilUsuarioTrilhaIds')))


				CREATE UNIQUE CLUSTERED INDEX IX_UsuarioCompeticao ON #UsuarioCompeticao (ID_USUARIO, ID_CLIENTE, ID_TRILHA, ID_COMPETICAO) 


				SELECT ID_USUARIO
					 , ID_CLIENTE
					 , ID_TRILHA
					 , NomeModulosConcluidos
				  INTO #UsuarioModulosConcluidos
				  FROM #UsuarioCompeticao AS UsuarioCompeticao
		   CROSS APPLY (SELECT STRING_AGG(CAST(RODADA.TX_NOME AS nvarchar(MAX)), ', ') AS NomeModulosConcluidos
						  FROM RODADA (NOLOCK)
						 WHERE RODADA.ID_CLIENTE = UsuarioCompeticao.ID_CLIENTE
						   AND RODADA.ID_TRILHA = UsuarioCompeticao.ID_TRILHA
						   AND RODADA.FL_STATUS = 1
						   AND EXISTS (SELECT 1 '1'		
										FROM TENTATIVA (NOLOCK)
									   WHERE TENTATIVA.ID_USUARIO = UsuarioCompeticao.ID_USUARIO
										 AND TENTATIVA.ID_CLIENTE = RODADA.ID_CLIENTE
										 AND TENTATIVA.ID_RODADA = RODADA.ID
										 AND TENTATIVA.ID_STATUS IN ('concluido', 'aprovado', 'reprovado', 'realizado_fora_prazo')
										)
						   AND NOT EXISTS (SELECT 1 '1'
												FROM DISPENSA_RODADA AS DR (NOLOCK)
											   WHERE DR.ID_USUARIO = UsuarioCompeticao.ID_USUARIO
												 AND DR.ID_CLIENTE = UsuarioCompeticao.ID_CLIENTE
												 AND DR.ID_RODADA = RODADA.ID
																 AND DR.DT_INICIO_PARA_REALIZACAO IS NULL
																 AND DR.DT_TERMINO_PARA_REALIZACAO IS NULL)) AS RodadasFinalizadas
		CREATE UNIQUE CLUSTERED INDEX IX_UsuarioModulosConcluidos ON #UsuarioModulosConcluidos (ID_USUARIO, ID_CLIENTE, ID_TRILHA) 

				
				;WITH UsuarioTrilha
				AS
				(
						SELECT UsuarioCompeticao.TX_LOGIN
							 , UsuarioCompeticao.TX_EMAIL
							 , UsuarioCompeticao.TX_NOME_COMPLETO
							 , UsuarioCompeticao.StatusUsuario
							 , UsuarioCompeticao.DataCadastroUsuario
							 , UsuarioCompeticao.ID_USUARIO
							 , UsuarioCompeticao.ID_CLIENTE
							 , UsuarioCompeticao.ID_TRILHA
							 , UsuarioCompeticao.ID_COMPETICAO
							 , UsuarioCompeticao.DT_PRIMEIRO_ACESSO
							 , UsuarioCompeticao.DT_CONCLUSAO
							 , UsuarioCompeticao.NU_TOTAL_RODADAS
							 , UsuarioCompeticao.PerfilTrilha
							 , UsuarioCompeticao.NU_TOTAL_RODADAS_CONCLUIDAS
							 , UsuarioCompeticao.NU_TOTAL_RODADAS_APROVADAS		
							 , UsuarioCompeticao.NU_TOTAL_RODADAS_REPROVADAS
							 , UsuarioCompeticao.NU_PONTUACAO
							 , UsuarioCompeticao.NU_TOTAL_RODADAS_EM_ANDAMENTO 
							 , UsuarioCompeticao.NU_TOTAL_RODADAS_AGUARDANDO_CORRECAO
							 , UsuarioCompeticao.NU_TOTAL_RODADAS_NAO_INICIADAS
							 , ISNULL(UsuarioCompeticao.NU_CARGA_HORARIA, 0) NU_CARGA_HORARIA
							 , UsuarioCompeticao.NU_TEMPO_ACESSO_EM_SEGUNDOS
							 , UsuarioCompeticao.DataVinculoTrilha
							 , UsuarioCompeticao.OrigemVinculoTrilha
							 , CASE WHEN ISNULL(UsuarioCompeticao.ID_TENTATIVA_RECERTIFICACAO, 0) =0 THEN 'Padrão' 
									ELSE 'Recertificação' END AS TipoTentativa
							 , UsuarioModulosConcluidos.NomeModulosConcluidos
							 , DataExpiracaoRecertificacao.[Data] AS DataExpiracaoTrilha
							 , NULLIF(UsuarioCompeticao.ID_TENTATIVA_RECERTIFICACAO, 0) AS IdTentativaRecertificacao
							 , StatusTrilha.[Status] AS StatusTrilha
							 , Dispensado
						  FROM #UsuarioCompeticao AS UsuarioCompeticao
						  JOIN #UsuarioModulosConcluidos UsuarioModulosConcluidos
						    ON UsuarioModulosConcluidos.ID_USUARIO = UsuarioCompeticao.ID_USUARIO
						   AND UsuarioModulosConcluidos.ID_CLIENTE = UsuarioCompeticao.ID_CLIENTE
						   AND UsuarioModulosConcluidos.ID_TRILHA = UsuarioCompeticao.ID_TRILHA
					 LEFT JOIN RECERTIFICACAO (NOLOCK)
							ON RECERTIFICACAO.ID_TRILHA = UsuarioCompeticao.ID_TRILHA
						   AND RECERTIFICACAO.ID_CLIENTE = UsuarioCompeticao.ID_CLIENTE
						   AND RECERTIFICACAO.FL_STATUS = 1
				   OUTER APPLY (SELECT DATEADD(DAY, RECERTIFICACAO.NU_PERIODO_EXPIRACAO_RECERTIFICACAO_EM_DIAS, UsuarioCompeticao.DT_CONCLUSAO) AS [Data] 
							   ) AS DataExpiracaoRecertificacao
				   OUTER APPLY (SELECT DATEADD(DAY, -Recertificacao.NU_PERIODO_RECERTIFICACAO_DISPONIVEL_EM_DIAS, DataExpiracaoRecertificacao.[Data]) AS [Data]
							   ) AS DataInicioRecertificacao
				   CROSS APPLY (SELECT CASE WHEN UsuarioCompeticao.NU_TOTAL_RODADAS_DISPENSADAS >= NU_TOTAL_RODADAS AND UsuarioCompeticao.Dispensado = 1 THEN 'Dispensado'	
										    WHEN ((UsuarioCompeticao.NU_TOTAL_RODADAS_APROVADAS + UsuarioCompeticao.NU_TOTAL_RODADAS_REPROVADAS) >= (UsuarioCompeticao.NU_TOTAL_RODADAS - UsuarioCompeticao.NU_TOTAL_RODADAS_DISPENSADAS))
												THEN  -- 'Concluído' 
												   CASE WHEN RECERTIFICACAO.ID IS NOT NULL AND ((FL_USUARIOS_VINCULO_OBRIGATORIO = 1 AND UsuarioCompeticao.PerfilTrilha = 'Obrigatório')  OR (FL_USUARIOS_VINCULO_PARTICIPA = 1 AND UsuarioCompeticao.PerfilTrilha = 'Participa'))
														     AND GETDATE() <= DataExpiracaoRecertificacao.[Data] THEN 'Certificação Válida'
													    WHEN RECERTIFICACAO.ID IS NOT NULL AND GETDATE() > DataExpiracaoRecertificacao.[Data] THEN 'Certificação Inválida'	
														ELSE 'Concluído' 
													END
										 WHEN (UsuarioCompeticao.NU_TOTAL_RODADAS_EXPIRADAS + UsuarioCompeticao.NU_TOTAL_RODADAS_REALIZADAS_FORA_DO_PRAZO) > 0
											  AND (UsuarioCompeticao.NU_TOTAL_RODADAS_EM_ANDAMENTO
												 + UsuarioCompeticao.NU_TOTAL_RODADAS_NAO_INICIADAS
												 + UsuarioCompeticao.NU_TOTAL_RODADAS_AGUARDANDO_CORRECAO) = 0 THEN 'Expirado (Não Concluído)'
										 WHEN (UsuarioCompeticao.NU_TOTAL_RODADAS_NAO_LIBERADAS > 0
										       AND UsuarioCompeticao.NU_TOTAL_RODADAS_NAO_LIBERADAS >= UsuarioCompeticao.NU_TOTAL_RODADAS - UsuarioCompeticao.NU_TOTAL_RODADAS_DISPENSADAS) THEN 'Não Liberado'
										 WHEN UsuarioCompeticao.NU_TOTAL_RODADAS_NAO_INICIADAS > 0
											  AND (UsuarioCompeticao.NU_TOTAL_RODADAS_APROVADAS
												 + UsuarioCompeticao.NU_TOTAL_RODADAS_REPROVADAS
												 + UsuarioCompeticao.NU_TOTAL_RODADAS_AGUARDANDO_CORRECAO
												 + UsuarioCompeticao.NU_TOTAL_RODADAS_EM_ANDAMENTO
												 + UsuarioCompeticao.NU_TOTAL_RODADAS_EXPIRADAS
												 + UsuarioCompeticao.NU_TOTAL_RODADAS_REALIZADAS_FORA_DO_PRAZO) = 0 THEN 'Não Iniciado'
										 WHEN (UsuarioCompeticao.NU_TOTAL_RODADAS_EM_ANDAMENTO + UsuarioCompeticao.NU_TOTAL_RODADAS_AGUARDANDO_CORRECAO + UsuarioCompeticao.NU_TOTAL_RODADAS_NAO_INICIADAS) > 0
											 THEN 'Em Andamento'
										ELSE 'Não Iniciado'
										END AS [Status]) AS StatusTrilha
						 WHERE 1 = 1					   --
						   AND ((@showEmptyDates = 1 AND DataVinculoTrilha IS NULL)
							     OR (ISNULL(@trackLinkDateStartDate, '') != '' AND ISNULL(@trackLinkDateEndDate, '') != '' AND DataVinculoTrilha BETWEEN @trackLinkDateStartDate AND @trackLinkDateEndDate)
							     OR (ISNULL(@trackLinkDateStartDate, '') = '' OR ISNULL(@trackLinkDateEndDate, '') = ''))
						  AND EXISTS (SELECT *
		 								FROM #CompeticoesAdmin AS CompeticoesAdmin
		 							   WHERE CompeticoesAdmin.CompeticaoId = UsuarioCompeticao.ID_COMPETICAO
		 							     AND CompeticoesAdmin.ClienteId = UsuarioCompeticao.ID_CLIENTE)

				) SELECT UsuarioTrilha.ID_USUARIO AS UsuarioID
					   , UsuarioTrilha.TX_NOME_COMPLETO AS NomeUsuario
					   , UsuarioTrilha.TX_EMAIL AS EmailUsuario
					   , UsuarioTrilha.TX_LOGIN AS LoginUsuario
					   , CASE WHEN UsuarioTrilha.StatusUsuario = 1 THEN 'Ativo' ELSE 'Inativo' END AS StatusUsuario
					   , GruposUsuario.GrupoPaiID
					   , GruposUsuario.GrupoPai AS NomeGrupoPai
					   , GruposUsuario.GrupoFilhoId AS GrupoFilhoID
					   , GruposUsuario.GrupoFilho AS NomeGrupoFilho
					   , UsuariosAdmin.TodosGruposUsuario
					   , Competicao.ID AS AmbienteID
					   , Competicao.TX_NOME AS NomeAmbiente
					   , UsuarioTrilha.ID_TRILHA AS TrilhaID
					   , Trilha.TX_DESCRICAO AS NomeTrilha
					   , PerfilTrilha AS PerfilNaTrilha
					   , CASE WHEN Dispensado = 1 THEN ''
							  WHEN UsuarioTrilha.NU_CARGA_HORARIA > 0 THEN dbo.FormatarSegundosEmHorasMinSeg(UsuarioTrilha.NU_CARGA_HORARIA) END AS CargaHorariaTotalTrilhaEmHoras
					   , CASE WHEN Dispensado = 1 THEN NULL
							  WHEN UsuarioTrilha.StatusTrilha = 'Não Iniciado' THEN NULL
							  WHEN UsuarioTrilha.DT_PRIMEIRO_ACESSO IS NOT NULL THEN CONVERT(VARCHAR(10), UsuarioTrilha.DT_PRIMEIRO_ACESSO, 103) + ' ' + CONVERT(VARCHAR(8), UsuarioTrilha.DT_PRIMEIRO_ACESSO, 108) END AS DataInicioTrilha
					   , CASE WHEN Dispensado = 1 THEN ''
							  WHEN NU_TOTAL_RODADAS_CONCLUIDAS >= NU_TOTAL_RODADAS THEN CONVERT(VARCHAR(10), UsuarioTrilha.DT_CONCLUSAO , 103) + ' ' + CONVERT(VARCHAR(8), UsuarioTrilha.DT_CONCLUSAO, 108) END AS DataConclusaoTrilha
					   , CASE WHEN Dispensado = 1 THEN '' 
							  ELSE CONVERT(VARCHAR(50), CAST(UsuarioTrilha.NU_PONTUACAO AS INT)) END AS PontuacaoTrilha
					   , CASE WHEN Dispensado = 1 THEN ''
							  WHEN Valores.IniciouTrilha != 'Sim' THEN ''
							  ELSE REPLACE(CONVERT(VARCHAR(50), CONVERT(DECIMAL(18, 2), (CONVERT(DECIMAL(18, 2), UsuarioTrilha.NU_TOTAL_RODADAS_CONCLUIDAS) / CONVERT(DECIMAL(18, 2), NULLIF(UsuarioTrilha.NU_TOTAL_RODADAS, 0))) * 100)), '.', ',') + '%' END AS PercentualConclusaoTrilha
					   , CASE WHEN Dispensado = 1 THEN ''
						      WHEN UsuarioTrilha.NU_TOTAL_RODADAS = 0 THEN ''
							  ELSE FORMAT(100.0 * UsuarioTrilha.NU_TOTAL_RODADAS_APROVADAS / NULLIF(UsuarioTrilha.NU_TOTAL_RODADAS, 0), 'N2', 'pt-br') + '%' END AS PercentualAprovacaoTrilha
					   , CASE WHEN Dispensado = 1 THEN ''
							  WHEN UsuarioTrilha.NU_TEMPO_ACESSO_EM_SEGUNDOS > 0 THEN dbo.FormatarSegundosEmHorasMinSeg(UsuarioTrilha.NU_TEMPO_ACESSO_EM_SEGUNDOS) ELSE '' END AS TempoAcessoNaTrilhaEmHoras
					   , UsuarioTrilha.StatusTrilha
					   , CASE WHEN Dispensado = 1 THEN 'Não' 
							  ELSE Valores.IniciouTrilha END AS IniciouTrilha
					   , CASE WHEN Dispensado = 1 THEN 'Não' 
							  ELSE Valores.ConcluiuTrilha END AS ConcluiuTrilha
					   , CASE WHEN Dispensado = 1 THEN 'Não' 
							  WHEN UsuarioTrilha.NU_TOTAL_RODADAS_APROVADAS >= UsuarioTrilha.NU_TOTAL_RODADAS THEN 'Sim' ELSE '' END AS AprovadoNaTrilha
					   , UsuarioTrilha.NU_TOTAL_RODADAS AS TotalModulosVinculados
					   , UsuarioTrilha.NU_TOTAL_RODADAS_CONCLUIDAS AS TotalModulosConcluidos
					   , UsuarioTrilha.NU_TOTAL_RODADAS_APROVADAS AS TotalModulosAprovados
					   , FORMAT(UsuarioTrilha.DataVinculoTrilha, 'dd/MM/yyyy HH:mm') AS DataVinculoTrilha
					   , OrigemVinculoTrilha
					   , TipoTentativa
					   , NomeModulosConcluidos
					   , CASE WHEN TipoTentativa = 'Padrão' THEN ''
							  ELSE FORMAT(DataExpiracaoTrilha, 'dd/MM/yyyy HH:mm' ) END AS DataExpiracaoTrilha
					   , UsuarioTrilha.IdTentativaRecertificacao
					INTO #Resultado   
					FROM UsuarioTrilha
					JOIN #UsuariosAdmin AS UsuariosAdmin
					  ON UsuarioTrilha.ID_USUARIO = UsuariosAdmin.UsuarioID
					 AND UsuarioTrilha.ID_CLIENTE = UsuariosAdmin.ClienteID
			 OUTER APPLY (SELECT CASE WHEN UsuarioTrilha.StatusTrilha IN('Não Iniciado', 'Não Liberado')
											 THEN 'Não'
											 ELSE 'Sim' END AS IniciouTrilha
							   , IIF (UsuarioTrilha.NU_TOTAL_RODADAS_CONCLUIDAS >= UsuarioTrilha.NU_TOTAL_RODADAS, 'Sim', 'Não') AS ConcluiuTrilha) AS Valores
					JOIN COMPETICAO AS Competicao (NOLOCK)
					  ON Competicao.ID_CLIENTE = UsuarioTrilha.ID_CLIENTE
					 AND Competicao.ID = UsuarioTrilha.ID_COMPETICAO
					 AND (ISNULL(@showInactiveCompetition, 0) = 1 OR Competicao.FL_DISPONIVEL = 1)
					JOIN TRILHA AS Trilha (NOLOCK)
					  ON Trilha.ID = UsuarioTrilha.ID_TRILHA
					 AND Trilha.ID_CLIENTE = UsuarioTrilha.ID_CLIENTE
					 AND (ISNULL(@showInactiveTrack, 0) = 1 OR Trilha.FL_STATUS = 1)
			 CROSS APPLY (SELECT TOP 1 Grupo.TX_NOME AS GrupoFilho
									 , Grupo.ID AS GrupoFilhoId
									 , Pai.TX_NOME AS GrupoPai
									 , Pai.ID AS GrupoPaiId
								  FROM USUARIO_GRUPO AS UsuarioGrupo (NOLOCK)
								  JOIN GRUPO AS Grupo (NOLOCK)
									ON Grupo.ID = UsuarioGrupo.ID_GRUPO
								   AND Grupo.ID_CLIENTE = UsuarioGrupo.ID_CLIENTE
								   AND Grupo.FL_STATUS = 1
								  JOIN COMPETICAO_GRUPO as CompeticaoGrupo (NOLOCK)
									ON CompeticaoGrupo.ID_GRUPO = Grupo.ID
								   AND CompeticaoGrupo.ID_CLIENTE = Grupo.ID_CLIENTE
							 LEFT JOIN GRUPO AS Pai (NOLOCK)
									ON Pai.ID = Grupo.ID_GRUPO_PAI
								   AND Pai.ID_CLIENTE = Grupo.ID_CLIENTE
								   AND Grupo.FL_STATUS = 1
								 WHERE UsuarioGrupo.ID_USUARIO = UsuarioTrilha.ID_USUARIO
								   AND UsuarioGrupo.ID_CLIENTE = UsuarioTrilha.ID_CLIENTE
							  ORDER BY CASE WHEN Pai.ID IS NOT NULL THEN 2
											WHEN Grupo.ID IS NOT NULL THEN 3
											ELSE 4
										END ASC
										) AS GruposUsuario
		    LEFT JOIN (SELECT OriginalValue AS GrupoId FROM fnt_ConverterFiltrosParaTabela(@grupo, 'grupo')) AS RelatorioFiltroGrupo
				   ON (RelatorioFiltroGrupo.GrupoId = GruposUsuario.GrupoFilhoId OR 
					   RelatorioFiltroGrupo.GrupoId = GruposUsuario.GrupoPaiId)
				WHERE 1 = 1
				  AND (ISNULL(@grupo, '') = '' OR RelatorioFiltroGrupo.GrupoId IS NOT NULL)
				  AND (ISNULL(@tipoDeTentativa, '') = '' OR UsuarioTrilha.TipoTentativa IN (SELECT ConvertedValue FROM fnt_ConverterFiltrosParaTabela(@tipoDeTentativa, 'tipoTentativa')))
				  AND ((@showEmptyDates = 1 AND UsuarioTrilha.DT_PRIMEIRO_ACESSO IS NULL)
					  OR (ISNULL(@trackStartDateStartDate, '') != '' AND ISNULL(@trackStartDateEndDate, '') != '' AND UsuarioTrilha.DT_PRIMEIRO_ACESSO BETWEEN  @trackStartDateStartDate AND @trackStartDateEndDate) 
					  OR (ISNULL(@trackStartDateStartDate, '') = '' OR ISNULL(@trackStartDateEndDate, '') = ''))
				  AND ((@showEmptyDates = 1 AND Valores.ConcluiuTrilha = 'Não')
					   OR (ISNULL(@trackCompletionDateStartDate, '') != '' AND ISNULL(@trackCompletionDateEndDate,'') != '' AND Valores.ConcluiuTrilha = 'Sim' AND UsuarioTrilha.DT_CONCLUSAO BETWEEN @trackCompletionDateStartDate AND @trackCompletionDateEndDate)
					   OR (ISNULL(@trackCompletionDateStartDate, '') = '' OR ISNULL(@trackCompletionDateEndDate,'') = ''))
				  AND (ISNULL(@statusConclusaoTrilha, '') = '' OR StatusTrilha IN (SELECT ConvertedValue FROM fnt_ConverterFiltrosParaTabela(@statusConclusaoTrilha, 'statusConclusaoTrilha')))
				  AND ((@showEmptyDates = 1 AND UsuarioTrilha.DataExpiracaoTrilha IS NULL)
								OR (ISNULL(@trackExpirationDateStartDate, '') != '' AND ISNULL(@trackExpirationDateEndDate, '') != '' AND UsuarioTrilha.DataExpiracaoTrilha  BETWEEN @trackExpirationDateStartDate AND @trackExpirationDateEndDate)
								OR (ISNULL(@trackExpirationDateStartDate, '') = '' OR ISNULL(@trackExpirationDateEndDate, '') = ''))
				ORDER BY NomeUsuario, AmbienteID, TrilhaId		
		
			-- SELECT * FROM #Resultado


		IF EXISTS (SELECT *
					 FROM ATRIBUTO (NOLOCK)
				    WHERE Atributo.ID_CLIENTE = @clienteId
					  AND ATRIBUTO.FL_STATUS = 1
			)
		BEGIN
			DECLARE @sql NVARCHAR(MAX) = '', @fieldNames NVARCHAR(MAX) = ''

			SET @fieldNames = (SELECT ', [' + Atributo + ']' AS [text()]
			FROM (SELECT REPLACE (ATRIBUTO.TX_NOME, @clienteID + '_', '') AS Atributo
					   , Atributo.ID AS AtributoID
					FROM Atributo (NOLOCK)
				   WHERE Atributo.FL_STATUS = 1
					 AND Atributo.ID_CLIENTE = @clienteId ) AS A ORDER BY AtributoID
				 FOR XML PATH (''))

			SELECT UsuariosAdmin.UsuarioId AS EntidadeId
				 , UsuariosAdmin.ClienteID AS CustomerId
				 , Atributo
				 , CASE WHEN ATRIBUTO.ID_ATRIBUTO_TIPO = '4' 
						THEN CONVERT(VARCHAR(10), TRY_CONVERT(DATETIME, AtributoValor), 103) 
						ELSE AtributoValor END AS AtributoValor
			 INTO #TempAtributosUsuario
			  FROM #UsuariosAdmin AS UsuariosAdmin
			  JOIN fnt_AtributosUsuarios (@clienteId) AS Atributos
			    ON Atributos.UsuarioId = UsuariosAdmin.UsuarioId
			   AND Atributos.ClienteId = UsuariosAdmin.ClienteId
			  JOIN ATRIBUTO (NOLOCK)
				ON ATRIBUTO.ID = Atributos.AtributoId
			   AND ATRIBUTO.ID_CLIENTE = Atributos.ClienteId

			CREATE INDEX IX_TempAtributosUsuario ON #TempAtributosUsuario ([EntidadeId]) INCLUDE ([Atributo], [AtributoValor])

			SET @fieldNames = (SELECT SUBSTRING(@fieldNames, 2, LEN(@fieldNames)));   		 
			
			SET @sql += '; WITH CTE AS ( '
			SET @sql += '		SELECT Resultado.* '
			SET @sql += '			 , TempAtributos.Atributo '
			SET @sql += '			 , TempAtributos.AtributoValor '
			SET @sql += '		  FROM #Resultado AS Resultado '
			SET @sql += '	 LEFT JOIN #TempAtributosUsuario AS TempAtributos'
			SET @sql += '	        ON TempAtributos.EntidadeId = Resultado.UsuarioId '
			SET @sql += '	 LEFT JOIN BATCHMODE '
			SET @sql += '	        ON (SELECT 1) = (SELECT 0) '	
			SET @sql += ') SELECT pvt.*'
			SET @sql += '	 FROM CTE'
			SET @sql += '   PIVOT (MAX(AtributoValor) FOR Atributo IN(' + @fieldNames + ')) pvt '
			SET @sql += '  OPTION (MAXDOP 4)'



			EXEC (@sql);
			return;
		END 
		ELSE 
		BEGIN			
			SELECT *
			  FROM #Resultado
		END


/*
-- select * from competicao where id_cliente = 'educapermanente' and fl_disponivel = 1
-- SELECT ID FROM USUARIO WHERE TX_LOGIN =	'ricardo.carvalho@engage.bz' AND ID_CLIENTE = 'educapermanente'

	[Report.engagesp_Relatorio_Trilhas_Finalizadas_Usuario]

	EXEC [Report].[engagesp_Relatorio_Trilhas_Finalizadas_Usuario] 
	@adminId		= 1237679
	, @clienteID	= 'educapermanente'
	, @competicaoId	= '19171' 
	, @trilhaId		=   ''
	, @userStatus	= '1'
	, @perfilUsuarioTrilhaIds = '1,2'
	, @grupo = ''
	, @usuario  = ''
	, @tipoDeTentativa = ''
	, @trackStartDateStartDate = NULL
	, @trackStartDateEndDate = NULL
	, @trackStartDateShowEmptyDates = 1
	, @trackCompletionDateStartDate = NULL
	, @trackCompletionDateEndDate = NULL
	, @trackCompletionDateShowEmptyDates = 0
	, @trackLinkDateStartDate = ''
	, @trackLinkDateEndDate = ''
	, @trackLinkDateShowEmptyDates = 1
	, @trackExpirationDateStartDate = ''
	, @trackExpirationDateEndDate = ''
	, @trackExpirationDateShowEmptyDates = 1
	, @statusConclusaoTrilha = '' -- exempted, not_started




*/
GO

