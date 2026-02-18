USE [prod_my_engage_autosservico]
GO

/****** Object:  StoredProcedure [dbo].[engagesp_GetUsuarioByTargetAudience]    Script Date: 10/27/2025 7:06:43 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- use [dev.engage.bz]
-- use [test.engage.bz]
-- use [pre.engage.bz]
CREATE PROCEDURE [dbo].[engagesp_GetUsuarioByTargetAudience]
      @targetAudienceCriteriasAdd PublicoAlvoCriterio READONLY
    , @targetAudienceCriteriasException PublicoAlvoCriterio READONLY
	, @skip INT = 0
	, @take INT = 999
	, @search NVARCHAR(255)
AS

BEGIN

	DECLARE @idPublicoAlvo BIGINT;
	DECLARE @dataCadastro DATETIME = GETDATE();

	BEGIN 
		DECLARE @temCriterioCompeticao BIT = (SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM @targetAudienceCriteriasAdd WHERE Id_Entidade_Tipo = 'CP' AND Id_Publico_Alvo_Criterio_Tipo = 'competition');
		DECLARE @temCriterioCompeticaoNaoParticipa BIT = (SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM @targetAudienceCriteriasAdd WHERE Id_Entidade_Tipo = 'CP' AND Id_Publico_Alvo_Criterio_Tipo = 'competition_not_participate');

		DECLARE @temCriterioCompeticaoConcluiu BIT = (SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM @targetAudienceCriteriasAdd WHERE Id_Entidade_Tipo = 'CP' AND Id_Publico_Alvo_Criterio_Tipo = 'competition_finished');
		DECLARE @temCriterioCompeticaoNaoConcluiu BIT = (SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM @targetAudienceCriteriasAdd WHERE Id_Entidade_Tipo = 'CP' AND Id_Publico_Alvo_Criterio_Tipo = 'competition_not_finished');
		DECLARE @temCriterioCompeticaoNaoParticipaNenhumaCompeticao BIT = (SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM @targetAudienceCriteriasAdd WHERE Id_Entidade_Tipo = 'CP' AND Id_Publico_Alvo_Criterio_Tipo = 'competition_not_participate_any');
		
		DECLARE @temCriterioTrilha BIT = (SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM @targetAudienceCriteriasAdd WHERE Id_Entidade_Tipo = 'TR' AND Id_Publico_Alvo_Criterio_Tipo = 'track');
		DECLARE @temCriterioTrilhaNaoParticipa BIT = (SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM @targetAudienceCriteriasAdd WHERE Id_Entidade_Tipo = 'TR' AND Id_Publico_Alvo_Criterio_Tipo = 'track_not_participate');
		DECLARE @temCriterioTrilhaConcluiu BIT = (SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM @targetAudienceCriteriasAdd WHERE Id_Entidade_Tipo = 'TR' AND Id_Publico_Alvo_Criterio_Tipo = 'track_finished');
		DECLARE @temCriterioTrilhaNaoConcluiu BIT = (SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM @targetAudienceCriteriasAdd WHERE Id_Entidade_Tipo = 'TR' AND Id_Publico_Alvo_Criterio_Tipo = 'track_not_finished');
		
		DECLARE @temCriterioGrupo BIT = (SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM @targetAudienceCriteriasAdd WHERE Id_Entidade_Tipo = 'GR');
		DECLARE @temCriterioUsuarioPerfil BIT = (SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM @targetAudienceCriteriasAdd WHERE Id_Entidade_Tipo = 'UP');
		DECLARE @temCriterioAdminPerfil BIT = (SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM @targetAudienceCriteriasAdd WHERE Id_Entidade_Tipo = 'AP');
		DECLARE @temCriterioUsuarioStatus BIT = (SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM @targetAudienceCriteriasAdd WHERE Id_Entidade_Tipo = 'SU');
		DECLARE @temCriterioDinamico INT = (SELECT TOP 1 COUNT(*) OVER () FROM @targetAudienceCriteriasAdd WHERE Id_Entidade_Tipo = 'AV' GROUP BY ID_ATRIBUTO);
		DECLARE @contador INT = 0;

		IF (@temCriterioCompeticao = 1)
		BEGIN
			SET @contador = 1;
		END

		IF (@temCriterioCompeticaoNaoParticipa = 1)
		BEGIN
			SET @contador = (@contador + 1);
		END

		IF (@temCriterioCompeticaoConcluiu = 1)
		BEGIN
			SET @contador = (@contador + 1);
		END

		IF (@temCriterioCompeticaoNaoConcluiu = 1)
		BEGIN
			SET @contador = (@contador + 1);
		END

		IF (@temCriterioCompeticaoNaoParticipaNenhumaCompeticao = 1)
		BEGIN
			SET @contador = (@contador + 1);
		END
		
		IF (@temCriterioTrilha = 1)
		BEGIN
			SET @contador = (@contador + 1);
		END

		IF (@temCriterioTrilhaNaoParticipa = 1)
		BEGIN
			SET @contador = (@contador + 1);
		END

		IF (@temCriterioTrilhaConcluiu = 1)
		BEGIN
			SET @contador = (@contador + 1);
		END

		IF (@temCriterioTrilhaNaoConcluiu = 1)
		BEGIN
			SET @contador = (@contador + 1);
		END

		IF (@temCriterioGrupo = 1)
		BEGIN
			SET @contador = (@contador + 1);
		END

		IF (@temCriterioUsuarioPerfil = 1)
		BEGIN
			SET @contador = (@contador + 1);
		END

		IF (@temCriterioAdminPerfil = 1)
		BEGIN
			SET @contador = (@contador + 1);
		END

		IF (@temCriterioUsuarioStatus = 1)
		BEGIN
			SET @contador = (@contador + 1);
		END

		IF (@temCriterioDinamico > 0)
		BEGIN
			SET @contador = (@contador + @temCriterioDinamico);
		END

		/* Criterios de Exceção do Publico Alvo */
		  SELECT DISTINCT Id_Usuario,
				            Id_Cliente,
						    [CreatedBy],
						    [UpdatedBy],
						    [LastUpdateDate],
						    [RegistrationDate],
						    [Status]
							INTO #UsuarioPublicoAlvoExcecao
			FROM	( 
			/* Participa da Competição */
					SELECT	DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					FROM		Competicao (NOLOCK)
					INNER JOIN	Competicao_Grupo 
					ON		(Competicao.Id = Competicao_Grupo.Id_Competicao)
					AND		(Competicao.Id_Cliente = Competicao_Grupo.Id_Cliente)
					INNER JOIN	@targetAudienceCriteriasException AS Publico_Alvo_Criterio
					ON		(Competicao.Id = Publico_Alvo_Criterio.Id_Entidade)
					AND		(Competicao.Id_Cliente = Publico_Alvo_Criterio.Id_Cliente)
					INNER JOIN	Usuario_Grupo (NOLOCK)
					ON		(Competicao_Grupo.Id_Grupo = Usuario_Grupo.Id_Grupo)
					AND		(Competicao.Id_Cliente = Usuario_Grupo.Id_Cliente)
					INNER JOIN	Usuario (NOLOCK)
					ON		(Usuario_Grupo.Id_Usuario = Usuario.Id)
					AND		(Usuario_Grupo.Id_Cliente = Usuario.Id_Cliente)
					WHERE		(Competicao.Fl_Disponivel = 1)
					AND		(Publico_Alvo_Criterio.Id_Entidade_Tipo = 'CP')
					AND     (Publico_Alvo_Criterio.Id_Publico_Alvo_Criterio_Tipo = 'competition')
					AND		(Usuario.Fl_Status = 1)
					AND Usuario.FL_PADRAO = 0
					AND Usuario.DT_EXCLUSAO IS NULL
					   AND EXISTS (SELECT * FROM @targetAudienceCriteriasException WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('competition'))

					UNION ALL

			/* Não Participa da Competição */
					SELECT DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
				      JOIN @targetAudienceCriteriasException AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
					 WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'CP'
					   AND USUARIO.FL_STATUS = 1
					   AND Usuario.DT_EXCLUSAO IS NULL
					   AND PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'competition_not_participate'
					   AND NOT EXISTS (SELECT * 
										 FROM USUARIO_PROGRESSO_TRILHA AS UsuarioProgressoTrilha (NOLOCK)
										WHERE UsuarioProgressoTrilha.ID_CLIENTE = USUARIO.ID_CLIENTE
										  AND UsuarioProgressoTrilha.ID_USUARIO = USUARIO.ID
										  AND UsuarioProgressoTrilha.ID_COMPETICAO = PUBLICO_ALVO_CRITERIO.ID_ENTIDADE
										  AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'CP'
										  AND PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'competition_not_participate')
					   AND Usuario.FL_PADRAO = 0
					   AND EXISTS (SELECT * FROM @targetAudienceCriteriasException WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('competition_not_participate'))

				    UNION ALL

			/* Não Participa de nenhuma Competição */
					SELECT DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
				      JOIN @targetAudienceCriteriasException AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
					 WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'CP'
					   AND USUARIO.FL_STATUS = 1
					   AND Usuario.DT_EXCLUSAO IS NULL
					   AND PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'competition_not_participate_any'
					   AND NOT EXISTS (SELECT *
									     FROM USUARIO_PERFIL_ENTIDADE AS UPE (NOLOCK) 
										WHERE UPE.ID_USUARIO = USUARIO.ID
										  AND UPE.ID_CLIENTE = USUARIO.ID_CLIENTE
										  AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'CP'
										  AND UPE.ID_PERFIL_JOGO IN (1, 2, 3)
										  AND PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'competition_not_participate_any')
					   AND Usuario.FL_PADRAO = 0
					   AND EXISTS (SELECT * FROM @targetAudienceCriteriasException WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('competition_not_participate_any'))

					UNION ALL

			 /* Concluiu/Nao Concluiu a Competição */
					SELECT DISTINCT  Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
				      JOIN @targetAudienceCriteriasException AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
			   CROSS APPLY (SELECT CASE WHEN (SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS) - SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS_CONCLUIDAS)) = 0 
										 AND SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS) > 0
										THEN 'Sim'
										WHEN (SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS) - SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS_CONCLUIDAS)) != 0 
										 AND SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS) > 0
										THEN 'Nao'
										ELSE 'Nao'
							        END AS [CompeticaoFinalizada] 
							     , ID_USUARIO AS UsuarioId
							     , ID_COMPETICAO AS CompeticaoId
			                  FROM USUARIO_PROGRESSO_TRILHA AS UsuarioProgressoTrilha
						     WHERE UsuarioProgressoTrilha.ID_USUARIO = USUARIO.ID
							   AND UsuarioProgressoTrilha.ID_CLIENTE = USUARIO.ID_CLIENTE
							   AND UsuarioProgressoTrilha.ID_COMPETICAO = PUBLICO_ALVO_CRITERIO.ID_ENTIDADE
						  GROUP BY ID_USUARIO, ID_COMPETICAO) AS UsuarioTrilhaConsolidado
					 WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'CP'
					   AND USUARIO.FL_STATUS = 1
					   AND Usuario.DT_EXCLUSAO IS NULL
					   AND ((PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'competition_finished'
							AND UsuarioTrilhaConsolidado.CompeticaoFinalizada = 'Sim') 
						   OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'competition_not_finished'
							AND UsuarioTrilhaConsolidado.CompeticaoFinalizada = 'Nao'))
					   AND Usuario.FL_PADRAO = 0
					   AND EXISTS (SELECT * FROM @targetAudienceCriteriasException WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('competition_finished', 'competition_not_finished'))

					UNION ALL

				/* Participa da Trilha */
			        SELECT DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM TRILHA (NOLOCK)
				INNER JOIN COMPETICAO_TRILHA (NOLOCK)
					    ON TRILHA.ID = COMPETICAO_TRILHA.ID_TRILHA
					   AND TRILHA.ID_CLIENTE = COMPETICAO_TRILHA.ID_CLIENTE
				INNER JOIN @targetAudienceCriteriasException PUBLICO_ALVO_CRITERIO
				        ON TRILHA.ID = PUBLICO_ALVO_CRITERIO.ID_ENTIDADE
					   AND TRILHA.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
				INNER JOIN COMPETICAO (NOLOCK)
				        ON COMPETICAO_TRILHA.ID_COMPETICAO = COMPETICAO.ID
					   AND COMPETICAO_TRILHA.ID_CLIENTE = COMPETICAO.ID_CLIENTE
				INNER JOIN COMPETICAO_GRUPO (NOLOCK)
				        ON COMPETICAO.ID = COMPETICAO_GRUPO.ID_COMPETICAO
					   AND COMPETICAO.ID_CLIENTE = COMPETICAO_GRUPO.ID_CLIENTE
				INNER JOIN USUARIO_GRUPO (NOLOCK)
				        ON COMPETICAO_GRUPO.ID_GRUPO = USUARIO_GRUPO.ID_GRUPO
					   AND COMPETICAO_GRUPO.ID_CLIENTE = USUARIO_GRUPO.ID_CLIENTE
				INNER JOIN USUARIO (NOLOCK)
				        ON USUARIO_GRUPO.ID_USUARIO = USUARIO.ID
					   AND USUARIO_GRUPO.ID_CLIENTE = USUARIO.ID_CLIENTE
				 LEFT JOIN RODADA (NOLOCK)
				        ON RODADA.ID_TRILHA = COMPETICAO_TRILHA.ID_TRILHA
					   AND RODADA.ID_CLIENTE = COMPETICAO_TRILHA.ID_CLIENTE
			     LEFT JOIN RODADA_LINK (NOLOCK)
				        ON RODADA_LINK.ID_TRILHA = COMPETICAO_TRILHA.ID_TRILHA
					   AND RODADA_LINK.ID_CLIENTE = COMPETICAO_TRILHA.ID_CLIENTE
			   OUTER APPLY (SELECT * 
			                  FROM USUARIO_PERFIL_ENTIDADE AS UPE (NOLOCK)
							 WHERE UPE.ID_CLIENTE = RODADA.ID_CLIENTE 
							   AND UPE.ID_ENTIDADE = RODADA.ID
							   AND UPE.ID_ENTIDADE_TIPO = 'RD'
							   AND UPE.ID_PERFIL_JOGO IN (1, 2, 3)
							   AND UPE.ID_USUARIO = USUARIO.ID ) AS UsuarioPerfilRodada
			  OUTER APPLY (SELECT * 
			                  FROM USUARIO_PERFIL_ENTIDADE AS UPE (NOLOCK)
							 WHERE UPE.ID_CLIENTE = TRILHA.ID_CLIENTE 
							   AND UPE.ID_ENTIDADE = TRILHA.ID
							   AND UPE.ID_ENTIDADE_TIPO = 'TR'
							   AND UPE.ID_PERFIL_JOGO IN (1, 2, 3)
							   AND UPE.ID_USUARIO = USUARIO.ID ) AS UsuarioPerfilTrilha
			   OUTER APPLY (SELECT * 
			                  FROM USUARIO_PERFIL_ENTIDADE AS UPE (NOLOCK)
							 WHERE UPE.ID_CLIENTE = RODADA_LINK.ID_CLIENTE 
							   AND UPE.ID_ENTIDADE = RODADA_LINK.ID
							   AND UPE.ID_ENTIDADE_TIPO = 'RL'
							   AND UPE.ID_PERFIL_JOGO IN (1, 2, 3)
							   AND UPE.ID_USUARIO = USUARIO.ID ) AS UsuarioPerfilRodadaLink
					 WHERE TRILHA.FL_STATUS = 1
					   AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'TR'
					   AND USUARIO.FL_STATUS = 1
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.DT_EXCLUSAO IS NULL
					   AND (UsuarioPerfilRodada.ID_ENTIDADE IS NOT NULL OR UsuarioPerfilRodadaLink.ID_ENTIDADE IS NOT NULL  OR UsuarioPerfilTrilha.ID_ENTIDADE IS NOT NULL)
					   AND EXISTS (SELECT * FROM @targetAudienceCriteriasException WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('track'))

					UNION ALL

			/* Não Participa da Trilha */
					SELECT DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
				      JOIN @targetAudienceCriteriasException AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
					 WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'TR'
					   AND USUARIO.FL_STATUS = 1
					   AND Usuario.DT_EXCLUSAO IS NULL
					   AND PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'track_not_participate'
					   AND NOT EXISTS (SELECT * 
										 FROM USUARIO_PROGRESSO_TRILHA AS UsuarioProgressoTrilha (NOLOCK)
										WHERE UsuarioProgressoTrilha.ID_CLIENTE = USUARIO.ID_CLIENTE
										  AND UsuarioProgressoTrilha.ID_USUARIO = USUARIO.ID
										  AND UsuarioProgressoTrilha.ID_TRILHA = PUBLICO_ALVO_CRITERIO.ID_ENTIDADE
										  AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'TR'
										  AND PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'track_not_participate')
					   AND Usuario.FL_PADRAO = 0
					   AND EXISTS (SELECT * FROM @targetAudienceCriteriasException WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('track_not_participate'))


					UNION ALL

			 /* Concluiu/Nao Concluiu a Trilha */
					SELECT DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
				      JOIN @targetAudienceCriteriasException AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
			   CROSS APPLY (SELECT CASE WHEN (SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS) - SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS_CONCLUIDAS)) = 0 
										 AND SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS) > 0
										THEN 'Sim'
										WHEN (SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS) - SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS_CONCLUIDAS)) != 0 
										 AND SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS) > 0
										THEN 'Nao'
										ELSE 'Nao'
							        END AS [TrilhaFinalizada] 
							     , ID_USUARIO AS UsuarioId
								 , ID_TRILHA AS TrilhaId
			                  FROM USUARIO_PROGRESSO_TRILHA AS UsuarioProgressoTrilha
						     WHERE UsuarioProgressoTrilha.ID_USUARIO = USUARIO.ID
							   AND UsuarioProgressoTrilha.ID_CLIENTE = USUARIO.ID_CLIENTE
							   AND UsuarioProgressoTrilha.ID_TRILHA = PUBLICO_ALVO_CRITERIO.ID_ENTIDADE
						  GROUP BY ID_USUARIO, ID_TRILHA) AS UsuarioTrilhaConsolidado
					 WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'TR'
					   AND USUARIO.FL_STATUS = 1
					   AND Usuario.DT_EXCLUSAO IS NULL
					   AND ((PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'track_finished'
							AND UsuarioTrilhaConsolidado.TrilhaFinalizada = 'Sim') 
						   OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'track_not_finished'
							AND UsuarioTrilhaConsolidado.TrilhaFinalizada = 'Nao'))
					   AND Usuario.FL_PADRAO = 0
					   AND EXISTS (SELECT * FROM @targetAudienceCriteriasException WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('track_finished', 'track_not_finished'))

					UNION ALL


			/* Grupos */
					SELECT	DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					FROM		Usuario_Grupo (NOLOCK)
					INNER JOIN	@targetAudienceCriteriasException AS Publico_Alvo_Criterio
					ON		(Usuario_Grupo.Id_Grupo = Publico_Alvo_Criterio.Id_Entidade)
					AND		(Usuario_Grupo.Id_Cliente = Publico_Alvo_Criterio.Id_Cliente)
					INNER JOIN	Usuario (NOLOCK)
					ON		(Usuario_Grupo.Id_Usuario = Usuario.Id)
					AND		(Usuario_Grupo.Id_Cliente = Usuario.Id_Cliente)
					WHERE		(Publico_Alvo_Criterio.Id_Entidade_Tipo = 'GR')
					AND		(Usuario.Fl_Status = 1)
					AND Usuario.FL_PADRAO = 0
					AND Usuario.DT_EXCLUSAO IS NULL
					AND EXISTS (SELECT * FROM @targetAudienceCriteriasException WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('group'))
					

					UNION ALL


			/* Perfil Usuario */
					SELECT	DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					FROM		USUARIO_PERFIL_ENTIDADE (NOLOCK)
					INNER JOIN	@targetAudienceCriteriasException AS Publico_Alvo_Criterio
					ON		(USUARIO_PERFIL_ENTIDADE.ID_PERFIL_JOGO = Publico_Alvo_Criterio.Id_Entidade)
					AND		(USUARIO_PERFIL_ENTIDADE.ID_CLIENTE = Publico_Alvo_Criterio.Id_Cliente)
					INNER JOIN	Usuario (NOLOCK)
					ON		(USUARIO_PERFIL_ENTIDADE.ID_USUARIO = Usuario.Id)
					AND		(USUARIO_PERFIL_ENTIDADE.ID_CLIENTE = Usuario.Id_Cliente)
					WHERE		(Publico_Alvo_Criterio.Id_Entidade_Tipo = 'UP')
					AND		(Usuario.Fl_Status = 1)
					AND Usuario.FL_PADRAO = 0
					AND Usuario.DT_EXCLUSAO IS NULL
					AND USUARIO_PERFIL_ENTIDADE.ID_PERFIL_JOGO IN (1, 2, 3)
					AND EXISTS (SELECT * FROM @targetAudienceCriteriasException WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('userProfile'))


					UNION ALL
					

				/* Perfil Admin */
					SELECT	DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					FROM		Perfil_Usuario (NOLOCK)
					INNER JOIN	@targetAudienceCriteriasException AS Publico_Alvo_Criterio
					ON		(Perfil_Usuario.Id_Perfil = Publico_Alvo_Criterio.Id_Entidade)
					AND		(Perfil_Usuario.Id_Cliente = Publico_Alvo_Criterio.Id_Cliente)
					INNER JOIN	Usuario (NOLOCK)
					ON		(Perfil_Usuario.Id_Usuario = Usuario.Id)
					AND		(Perfil_Usuario.Id_Cliente = Usuario.Id_Cliente)
					WHERE		(Publico_Alvo_Criterio.Id_Entidade_Tipo = 'AP')
					AND		(Usuario.Fl_Status = 1)
					AND Usuario.FL_PADRAO = 0
					AND Usuario.DT_EXCLUSAO IS NULL
					AND EXISTS (SELECT * FROM @targetAudienceCriteriasException WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('adminProfile'))


					UNION ALL


			/* Status Usuario */
					SELECT	DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					FROM		Usuario (NOLOCK)
					INNER JOIN	@targetAudienceCriteriasException AS Publico_Alvo_Criterio
					ON		(Usuario.Fl_Status = Publico_Alvo_Criterio.Id_Entidade)
					AND		(Usuario.Id_Cliente = Publico_Alvo_Criterio.Id_Cliente)
					WHERE		(Publico_Alvo_Criterio.Id_Entidade_Tipo = 'SU')
					AND Usuario.FL_PADRAO = 0
					AND Usuario.DT_EXCLUSAO IS NULL
					AND EXISTS (SELECT * FROM @targetAudienceCriteriasException WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('userStatus'))


					UNION ALL



			/* Usuarios */ 
					SELECT	DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					FROM		Usuario (NOLOCK)
					INNER JOIN	@targetAudienceCriteriasException AS Publico_Alvo_Criterio
					ON		(Usuario.Id = Publico_Alvo_Criterio.Id_Entidade)
					AND		(Usuario.Id_Cliente = Publico_Alvo_Criterio.Id_Cliente)
					WHERE		(Publico_Alvo_Criterio.Id_Entidade_Tipo = 'US')
					AND		(Usuario.Fl_Status = 1)
					AND Usuario.FL_PADRAO = 0
					AND Usuario.DT_EXCLUSAO IS NULL
					AND EXISTS (SELECT * FROM @targetAudienceCriteriasException WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('user'))

					UNION ALL
					
			/*Campos adicionais - Select */
					SELECT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
					  JOIN @targetAudienceCriteriasException AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
					  JOIN ATRIBUTO (NOLOCK)
					    ON ATRIBUTO.ID = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
					   AND ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
					   AND ATRIBUTO.ID_ATRIBUTO_TIPO = 7  -- Select Multiple
			   OUTER APPLY (SELECT ENTIDADE_ATRIBUTO.ID AS IdEntidadeAtributo
								 , TRY_CAST(ENTIDADE_ATRIBUTO.ID_ATRIBUTO AS nvarchar(18)) AS ID_ATRIBUTO
								 , TRY_CAST(ENTIDADE_ATRIBUTO.ID_ATRIBUTO_VALOR AS nvarchar(18)) AS ID_ATRIBUTO_VALOR
			                  FROM ENTIDADE_ATRIBUTO (NOLOCK)
							  JOIN ATRIBUTO (NOLOCK)
							    ON ENTIDADE_ATRIBUTO.ID_ATRIBUTO = ATRIBUTO.ID
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = ATRIBUTO.ID_CLIENTE
							   AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
							   AND ISNULL(PUBLICO_ALVO_CRITERIO.ID_ENTIDADE, 0) > 0
							   AND ATRIBUTO.FL_STATUS = 1
							 WHERE ENTIDADE_ATRIBUTO.ID_ENTIDADE = USUARIO.ID
							   AND ENTIDADE_ATRIBUTO.ID_ENTIDADE_TIPO = 'US'
							   AND ENTIDADE_ATRIBUTO.ID_ATRIBUTO = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
							   AND ENTIDADE_ATRIBUTO.ID_ATRIBUTO_VALOR = PUBLICO_ALVO_CRITERIO.ID_ENTIDADE) AS CamposSelect 				
			         WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
					   AND USUARIO.FL_STATUS = 1
					   AND Usuario.DT_EXCLUSAO IS NULL
					   AND CamposSelect.IdEntidadeAtributo IS NOT NULL
					   AND Usuario.FL_PADRAO = 0


					   UNION ALL


				  /*Campos adicionais - Multiple */
					SELECT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
					  JOIN @targetAudienceCriteriasException AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
					  JOIN ATRIBUTO (NOLOCK)
					    ON ATRIBUTO.ID = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
					   AND ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
					   AND ATRIBUTO.ID_ATRIBUTO_TIPO = 6  -- Select, Multiple
			   OUTER APPLY (SELECT ENTIDADE_ATRIBUTO.ID AS IdEntidadeAtributo
								 , TRY_CAST(ENTIDADE_ATRIBUTO.ID_ATRIBUTO AS nvarchar(18)) AS ID_ATRIBUTO
			                  FROM ENTIDADE_ATRIBUTO (NOLOCK)
							  JOIN ATRIBUTO (NOLOCK)
							    ON ENTIDADE_ATRIBUTO.ID_ATRIBUTO = ATRIBUTO.ID
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = ATRIBUTO.ID_CLIENTE
							   AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
							   AND ISNULL(PUBLICO_ALVO_CRITERIO.ID_ENTIDADE, 0) > 0
							   AND ATRIBUTO.FL_STATUS = 1
							 WHERE ENTIDADE_ATRIBUTO.ID_ENTIDADE = USUARIO.ID
							   AND ENTIDADE_ATRIBUTO.ID_ENTIDADE_TIPO = 'US'
							   AND ENTIDADE_ATRIBUTO.ID_ATRIBUTO = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
							   AND ENTIDADE_ATRIBUTO.ID_ATRIBUTO_VALOR = PUBLICO_ALVO_CRITERIO.ID_ENTIDADE) AS CamposSelect 				
			         WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
					   AND USUARIO.FL_STATUS = 1
					   AND CamposSelect.IdEntidadeAtributo IS NOT NULL
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.DT_EXCLUSAO IS NULL

					   UNION ALL
					
					/* Campos adicionais - TEXT */
					SELECT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
					  JOIN @targetAudienceCriteriasException AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
					  JOIN ATRIBUTO (NOLOCK)
					    ON ATRIBUTO.ID = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
					   AND ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
					   AND ATRIBUTO.ID_ATRIBUTO_TIPO = 1  -- Text
			   OUTER APPLY (SELECT ENTIDADE_ATRIBUTO.ID AS IdEntidadeAtributo
								 , TRY_CAST(ENTIDADE_ATRIBUTO.ID_ATRIBUTO AS nvarchar(18)) AS ID_ATRIBUTO
								 , ENTIDADE_ATRIBUTO.TX_VALOR
			                  FROM ENTIDADE_ATRIBUTO (NOLOCK) 
							  JOIN ATRIBUTO (NOLOCK)
							    ON ENTIDADE_ATRIBUTO.ID_ATRIBUTO = ATRIBUTO.ID
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = ATRIBUTO.ID_CLIENTE
							   AND ATRIBUTO.FL_STATUS = 1
							 WHERE ENTIDADE_ATRIBUTO.ID_ENTIDADE = USUARIO.ID
							   AND ENTIDADE_ATRIBUTO.ID_ENTIDADE_TIPO = 'US'
							   AND ENTIDADE_ATRIBUTO.ID_ATRIBUTO = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
							   AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
							   AND ISNULL(PUBLICO_ALVO_CRITERIO.ID_ENTIDADE, 0) = 0
							   AND ((PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'equal_to' AND ENTIDADE_ATRIBUTO.TX_VALOR = PUBLICO_ALVO_CRITERIO.TX_VALOR)
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'ends_with' AND ENTIDADE_ATRIBUTO.TX_VALOR LIKE '%'+PUBLICO_ALVO_CRITERIO.TX_VALOR)
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'starts_with' AND ENTIDADE_ATRIBUTO.TX_VALOR LIKE PUBLICO_ALVO_CRITERIO.TX_VALOR+'%')
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'is_empty')
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'contains' AND ENTIDADE_ATRIBUTO.TX_VALOR LIKE '%'+PUBLICO_ALVO_CRITERIO.TX_VALOR+'%'))) AS CamposText 	
			         WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
					   AND USUARIO.FL_STATUS = 1
					   AND ((PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'is_empty' AND (CamposText.TX_VALOR IS NULL OR CamposText.TX_VALOR = '')) 
							OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO != 'is_empty' AND CamposText.IdEntidadeAtributo IS NOT NULL))
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.DT_EXCLUSAO IS NULL

					   UNION ALL

				  /*Campos adicionais - Integer */
					SELECT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
					  JOIN @targetAudienceCriteriasException AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
					  JOIN ATRIBUTO (NOLOCK)
					    ON ATRIBUTO.ID = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
					   AND ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
					   AND ATRIBUTO.ID_ATRIBUTO_TIPO = 2  -- Integer
			   OUTER APPLY (SELECT ENTIDADE_ATRIBUTO.ID AS IdEntidadeAtributo
								 , TRY_CAST(ENTIDADE_ATRIBUTO.ID_ATRIBUTO AS nvarchar(18)) AS ID_ATRIBUTO
			                  FROM ENTIDADE_ATRIBUTO (NOLOCK) 
							  JOIN ATRIBUTO (NOLOCK)
							    ON ENTIDADE_ATRIBUTO.ID_ATRIBUTO = ATRIBUTO.ID
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = ATRIBUTO.ID_CLIENTE
							   AND ATRIBUTO.FL_STATUS = 1
							 WHERE ENTIDADE_ATRIBUTO.ID_ENTIDADE = USUARIO.ID
							   AND ENTIDADE_ATRIBUTO.ID_ENTIDADE_TIPO = 'US'
							   AND ENTIDADE_ATRIBUTO.ID_ATRIBUTO = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
							 AND ENTIDADE_ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
							   AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
							   AND ISNULL(PUBLICO_ALVO_CRITERIO.ID_ENTIDADE, 0) = 0
							   AND ((PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'equal_to' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS INT) = TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS INT))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'in_between' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS INT) BETWEEN TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS INT) AND TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR_2 AS INT))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'greater_than' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS INT) > TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS INT))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'greater_than_or_equal_to' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS INT) >= TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS INT))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'less_than' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS INT) < TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS INT))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'less_than_or_equal_to' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS INT) <= TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS INT)))) AS CamposInteger 	
			         WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
					   AND USUARIO.FL_STATUS = 1
					   AND CamposInteger.IdEntidadeAtributo IS NOT NULL
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.DT_EXCLUSAO IS NULL

					   UNION ALL

				  /*Campos adicionais - DATE */
					SELECT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
					  JOIN @targetAudienceCriteriasException AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
					  JOIN ATRIBUTO (NOLOCK)
					    ON ATRIBUTO.ID = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
					   AND ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
					   AND ATRIBUTO.ID_ATRIBUTO_TIPO = 4  -- Date
			   OUTER APPLY (SELECT ENTIDADE_ATRIBUTO.ID AS IdEntidadeAtributo
								 , TRY_CAST(ENTIDADE_ATRIBUTO.ID_ATRIBUTO AS nvarchar(18)) AS ID_ATRIBUTO
			                  FROM ENTIDADE_ATRIBUTO (NOLOCK) 
							  JOIN ATRIBUTO (NOLOCK)
							    ON ENTIDADE_ATRIBUTO.ID_ATRIBUTO = ATRIBUTO.ID
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = ATRIBUTO.ID_CLIENTE
							   AND ATRIBUTO.FL_STATUS = 1
							 WHERE ENTIDADE_ATRIBUTO.ID_ENTIDADE = USUARIO.ID
							   AND ENTIDADE_ATRIBUTO.ID_ENTIDADE_TIPO = 'US'
							   AND ENTIDADE_ATRIBUTO.ID_ATRIBUTO = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
							   AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
							   AND ISNULL(PUBLICO_ALVO_CRITERIO.ID_ENTIDADE, 0) = 0
							   AND ((PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'equal_to' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS DATE) = TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS DATE))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'in_between' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS DATE) BETWEEN TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS DATE) AND TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR_2 AS DATE))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'greater_than' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS DATE) > TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS DATE))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'greater_than_or_equal_to' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS DATE) >= TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS DATE))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'less_than' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS DATE) < TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS DATE))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'less_than_or_equal_to' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS DATE) <= TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS DATE)))) AS CamposDate 	
					 WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
					   AND USUARIO.FL_STATUS = 1
					   AND CamposDate.IdEntidadeAtributo IS NOT NULL
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.DT_EXCLUSAO IS NULL
				) Resultado
			GROUP BY Id_Usuario
				   , Id_Cliente
				   , [CreatedBy]
				   , [UpdatedBy]
				   , [LastUpdateDate]
				   , [RegistrationDate]
				   , [Status]
			--HAVING	COUNT(Id_Usuario, Id_Criterio) = @contador


		/* Criterios de Inclusao do Publico Alvo */
		;WITH UsuarioPublicoAlvo 
		AS
		(
			SELECT DISTINCT Id_Usuario,
				Id_Cliente,
				NomeCompleto,
				[Tx_Email],
				[Login],
				Id_Criterio,
				[CreatedBy],
				[UpdatedBy],
				[LastUpdateDate],
				[RegistrationDate],
				[Status]
			FROM	( 
			/* Participa da Competição */
					SELECT	DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.Tx_Nome_Completo AS NomeCompleto
						 , ISNULL(Usuario.Tx_Email, '') AS [Tx_Email]
						 , Usuario.Tx_Login AS [Login]
						 , 'competition' AS Id_Criterio
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					FROM		Competicao (NOLOCK)
					INNER JOIN	Competicao_Grupo 
					ON		(Competicao.Id = Competicao_Grupo.Id_Competicao)
					AND		(Competicao.Id_Cliente = Competicao_Grupo.Id_Cliente)
					INNER JOIN	@targetAudienceCriteriasAdd AS Publico_Alvo_Criterio
					ON		(Competicao.Id = Publico_Alvo_Criterio.Id_Entidade)
					AND		(Competicao.Id_Cliente = Publico_Alvo_Criterio.Id_Cliente)
					INNER JOIN	Usuario_Grupo (NOLOCK)
					ON		(Competicao_Grupo.Id_Grupo = Usuario_Grupo.Id_Grupo)
					AND		(Competicao.Id_Cliente = Usuario_Grupo.Id_Cliente)
					INNER JOIN	Usuario (NOLOCK)
					ON		(Usuario_Grupo.Id_Usuario = Usuario.Id)
					AND		(Usuario_Grupo.Id_Cliente = Usuario.Id_Cliente)
					WHERE		(Competicao.Fl_Disponivel = 1)
					AND		(Publico_Alvo_Criterio.Id_Entidade_Tipo = 'CP')
					AND     (Publico_Alvo_Criterio.Id_Publico_Alvo_Criterio_Tipo = 'competition')
					AND		(Usuario.Fl_Status = 1)
					AND Usuario.FL_PADRAO = 0
					AND Usuario.DT_EXCLUSAO IS NULL
					   AND EXISTS (SELECT * FROM @targetAudienceCriteriasAdd WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('competition'))

					UNION ALL

			/* Não Participa da Competição */
					SELECT DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.Tx_Nome_Completo
						 , ISNULL(Usuario.Tx_Email, '') AS [Tx_Email]
						 , Usuario.Tx_Login AS [Login]
						 , 'competition_not_participate' AS Id_Criterio
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
				      JOIN @targetAudienceCriteriasAdd AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
					 WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'CP'
					   AND USUARIO.FL_STATUS = 1
					   AND PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'competition_not_participate'
					   AND NOT EXISTS (SELECT * 
										 FROM USUARIO_PROGRESSO_TRILHA AS UsuarioProgressoTrilha (NOLOCK)
										WHERE UsuarioProgressoTrilha.ID_CLIENTE = USUARIO.ID_CLIENTE
										  AND UsuarioProgressoTrilha.ID_USUARIO = USUARIO.ID
										  AND UsuarioProgressoTrilha.ID_COMPETICAO = PUBLICO_ALVO_CRITERIO.ID_ENTIDADE
										  AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'CP'
										  AND PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'competition_not_participate')
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.DT_EXCLUSAO IS NULL
					   AND EXISTS (SELECT * FROM @targetAudienceCriteriasAdd WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('competition_not_participate'))

					UNION ALL

			 /* Concluiu/Nao Concluiu a Competição */
					SELECT DISTINCT  Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.Tx_Nome_Completo
						 , ISNULL(Usuario.Tx_Email, '') AS [Tx_Email]
						 , Usuario.Tx_Login AS [Login]
						 ,  CASE WHEN [CompeticaoFinalizada] = 'Sim' 
							     THEN 'competition_finished'
								 ELSE 'competition_not_finished' END AS Id_Criterio
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
				      JOIN @targetAudienceCriteriasAdd AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
			   CROSS APPLY (SELECT CASE WHEN (SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS) - SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS_CONCLUIDAS)) = 0 
										 AND SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS) > 0
										THEN 'Sim'
										WHEN (SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS) - SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS_CONCLUIDAS)) != 0 
										 AND SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS) > 0
										THEN 'Nao'
										ELSE 'Nao'
							        END AS [CompeticaoFinalizada] 
							     , ID_USUARIO AS UsuarioId
							     , ID_COMPETICAO AS CompeticaoId
			                  FROM USUARIO_PROGRESSO_TRILHA AS UsuarioProgressoTrilha
						     WHERE UsuarioProgressoTrilha.ID_USUARIO = USUARIO.ID
							   AND UsuarioProgressoTrilha.ID_CLIENTE = USUARIO.ID_CLIENTE
							   AND UsuarioProgressoTrilha.ID_COMPETICAO = PUBLICO_ALVO_CRITERIO.ID_ENTIDADE
						  GROUP BY ID_USUARIO, ID_COMPETICAO) AS UsuarioTrilhaConsolidado
					 WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'CP'
					   AND USUARIO.FL_STATUS = 1
					   AND ((PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'competition_finished'
							AND UsuarioTrilhaConsolidado.CompeticaoFinalizada = 'Sim') 
						   OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'competition_not_finished'
							AND UsuarioTrilhaConsolidado.CompeticaoFinalizada = 'Nao'))
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.DT_EXCLUSAO IS NULL
					   AND EXISTS (SELECT * FROM @targetAudienceCriteriasAdd WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('competition_finished', 'competition_not_finished'))

					UNION ALL

			/* Não Participa da Competição */
					SELECT DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.Tx_Nome_Completo
						 , ISNULL(Usuario.Tx_Email, '') AS [Tx_Email]
						 , Usuario.Tx_Login AS [Login]
						 , 'competition_not_participate_any' AS Id_Criterio
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
				      JOIN @targetAudienceCriteriasAdd AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
					 WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'CP'
					   AND USUARIO.FL_STATUS = 1
					   AND PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'competition_not_participate_any'
					   AND NOT EXISTS (SELECT * 
										 FROM USUARIO_PERFIL_ENTIDADE AS UsuarioPerfilEntidade (NOLOCK)
										WHERE UsuarioPerfilEntidade.ID_CLIENTE = USUARIO.ID_CLIENTE
										  AND UsuarioPerfilEntidade.ID_USUARIO = USUARIO.ID
										  AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'CP'
										  AND UsuarioPerfilEntidade.ID_PERFIL_JOGO IN (1, 2, 3)
										  AND PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'competition_not_participate_any')
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.DT_EXCLUSAO IS NULL
					   AND EXISTS (SELECT * FROM @targetAudienceCriteriasAdd WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('competition_not_participate_any'))

					   UNION ALL

				/* Participa da Trilha */
			        SELECT DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.Tx_Nome_Completo
						 , ISNULL(Usuario.Tx_Email, '') AS [Tx_Email]
						 , Usuario.Tx_Login AS [Login]
						 , 'track' AS ID_CRITERIO
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM TRILHA (NOLOCK)
				INNER JOIN COMPETICAO_TRILHA (NOLOCK)
					    ON TRILHA.ID = COMPETICAO_TRILHA.ID_TRILHA
					   AND TRILHA.ID_CLIENTE = COMPETICAO_TRILHA.ID_CLIENTE
				INNER JOIN @targetAudienceCriteriasAdd PUBLICO_ALVO_CRITERIO
				        ON TRILHA.ID = PUBLICO_ALVO_CRITERIO.ID_ENTIDADE
					   AND TRILHA.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
				INNER JOIN COMPETICAO (NOLOCK)
				        ON COMPETICAO_TRILHA.ID_COMPETICAO = COMPETICAO.ID
					   AND COMPETICAO_TRILHA.ID_CLIENTE = COMPETICAO.ID_CLIENTE
				INNER JOIN COMPETICAO_GRUPO (NOLOCK)
				        ON COMPETICAO.ID = COMPETICAO_GRUPO.ID_COMPETICAO
					   AND COMPETICAO.ID_CLIENTE = COMPETICAO_GRUPO.ID_CLIENTE
				INNER JOIN USUARIO_GRUPO (NOLOCK)
				        ON COMPETICAO_GRUPO.ID_GRUPO = USUARIO_GRUPO.ID_GRUPO
					   AND COMPETICAO_GRUPO.ID_CLIENTE = USUARIO_GRUPO.ID_CLIENTE
				INNER JOIN USUARIO (NOLOCK)
				        ON USUARIO_GRUPO.ID_USUARIO = USUARIO.ID
					   AND USUARIO_GRUPO.ID_CLIENTE = USUARIO.ID_CLIENTE
			     LEFT JOIN RODADA (NOLOCK)
				        ON RODADA.ID_TRILHA = COMPETICAO_TRILHA.ID_TRILHA
					   AND RODADA.ID_CLIENTE = COMPETICAO_TRILHA.ID_CLIENTE
			     LEFT JOIN RODADA_LINK (NOLOCK)
				        ON RODADA_LINK.ID_TRILHA = COMPETICAO_TRILHA.ID_TRILHA
					   AND RODADA_LINK.ID_CLIENTE = COMPETICAO_TRILHA.ID_CLIENTE
			   OUTER APPLY (SELECT * 
			                  FROM USUARIO_PERFIL_ENTIDADE AS UPE (NOLOCK)
							 WHERE UPE.ID_CLIENTE = RODADA.ID_CLIENTE 
							   AND UPE.ID_ENTIDADE = RODADA.ID
							   AND UPE.ID_ENTIDADE_TIPO = 'RD'
							   AND UPE.ID_PERFIL_JOGO IN (1, 2, 3)
							   AND UPE.ID_USUARIO = USUARIO.ID ) AS UsuarioPerfilRodada
			  OUTER APPLY (SELECT * 
			                  FROM USUARIO_PERFIL_ENTIDADE AS UPE (NOLOCK)
							 WHERE UPE.ID_CLIENTE = TRILHA.ID_CLIENTE 
							   AND UPE.ID_ENTIDADE = TRILHA.ID
							   AND UPE.ID_ENTIDADE_TIPO = 'TR'
							   AND UPE.ID_PERFIL_JOGO IN (1, 2, 3)
							   AND UPE.ID_USUARIO = USUARIO.ID ) AS UsuarioPerfilTrilha
			   OUTER APPLY (SELECT * 
			                  FROM USUARIO_PERFIL_ENTIDADE AS UPE (NOLOCK)
							 WHERE UPE.ID_CLIENTE = RODADA_LINK.ID_CLIENTE 
							   AND UPE.ID_ENTIDADE = RODADA_LINK.ID
							   AND UPE.ID_ENTIDADE_TIPO = 'RL'
							   AND UPE.ID_PERFIL_JOGO IN (1, 2, 3)
							   AND UPE.ID_USUARIO = USUARIO.ID ) AS UsuarioPerfilRodadaLink
					 WHERE TRILHA.FL_STATUS = 1
					   AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'TR'
					   AND USUARIO.FL_STATUS = 1
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.DT_EXCLUSAO IS NULL
					   AND (UsuarioPerfilRodada.ID_ENTIDADE IS NOT NULL OR UsuarioPerfilRodadaLink.ID_ENTIDADE IS NOT NULL  OR UsuarioPerfilTrilha.ID_ENTIDADE IS NOT NULL)
					   AND EXISTS (SELECT * FROM @targetAudienceCriteriasAdd WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('track'))
					UNION ALL

			/* Não Participa da Trilha */
					SELECT DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.Tx_Nome_Completo
						 , ISNULL(Usuario.Tx_Email, '') AS [Tx_Email]
						 , Usuario.Tx_Login AS [Login]
						 , 'track_not_participate' AS ID_CRITERIO
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
				      JOIN @targetAudienceCriteriasAdd AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
					 WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'TR'
					   AND USUARIO.FL_STATUS = 1
					   AND PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'track_not_participate'
					   AND NOT EXISTS (SELECT * 
										 FROM USUARIO_PROGRESSO_TRILHA AS UsuarioProgressoTrilha (NOLOCK)
										WHERE UsuarioProgressoTrilha.ID_CLIENTE = USUARIO.ID_CLIENTE
										  AND UsuarioProgressoTrilha.ID_USUARIO = USUARIO.ID
										  AND UsuarioProgressoTrilha.ID_TRILHA = PUBLICO_ALVO_CRITERIO.ID_ENTIDADE
										  AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'TR'
										  AND PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'track_not_participate')
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.DT_EXCLUSAO IS NULL
					   AND EXISTS (SELECT * FROM @targetAudienceCriteriasAdd WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('track_not_participate'))


					UNION ALL

			 /* Concluiu/Nao Concluiu a Trilha */
					SELECT DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.Tx_Nome_Completo
						 , ISNULL(Usuario.Tx_Email, '') AS [Tx_Email]
						 , Usuario.Tx_Login AS [Login]
					     ,  CASE WHEN [TrilhaFinalizada] = 'Sim' 
							     THEN 'track_finished'
								 ELSE 'track_not_finished' END AS Id_Criterio
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
				      JOIN @targetAudienceCriteriasAdd AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
			   CROSS APPLY (SELECT CASE WHEN (SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS) - SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS_CONCLUIDAS)) = 0 
										 AND SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS) > 0
										THEN 'Sim'
										WHEN (SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS) - SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS_CONCLUIDAS)) != 0 
										 AND SUM(UsuarioProgressoTrilha.NU_TOTAL_RODADAS) > 0
										THEN 'Nao'
										ELSE 'Nao'
							        END AS [TrilhaFinalizada] 
							     , ID_USUARIO AS UsuarioId
								 , ID_TRILHA AS TrilhaId
			                  FROM USUARIO_PROGRESSO_TRILHA AS UsuarioProgressoTrilha
						     WHERE UsuarioProgressoTrilha.ID_USUARIO = USUARIO.ID
							   AND UsuarioProgressoTrilha.ID_CLIENTE = USUARIO.ID_CLIENTE
							   AND UsuarioProgressoTrilha.ID_TRILHA = PUBLICO_ALVO_CRITERIO.ID_ENTIDADE
						  GROUP BY ID_USUARIO, ID_TRILHA) AS UsuarioTrilhaConsolidado
					 WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'TR'
					   AND USUARIO.FL_STATUS = 1
					   AND ((PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'track_finished'
							AND UsuarioTrilhaConsolidado.TrilhaFinalizada = 'Sim') 
						   OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'track_not_finished'
							AND UsuarioTrilhaConsolidado.TrilhaFinalizada = 'Nao'))
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.DT_EXCLUSAO IS NULL
					   AND EXISTS (SELECT * FROM @targetAudienceCriteriasAdd WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('track_finished', 'track_not_finished'))

					UNION ALL


			/* Grupos */
					SELECT	DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.Tx_Nome_Completo
						 , ISNULL(Usuario.Tx_Email, '') AS [Tx_Email]
						 , Usuario.Tx_Login AS [Login]
						 , 'group' AS ID_CRITERIO
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					FROM		Usuario_Grupo (NOLOCK)
					INNER JOIN	@targetAudienceCriteriasAdd AS Publico_Alvo_Criterio
					ON		(Usuario_Grupo.Id_Grupo = Publico_Alvo_Criterio.Id_Entidade)
					AND		(Usuario_Grupo.Id_Cliente = Publico_Alvo_Criterio.Id_Cliente)
					INNER JOIN	Usuario (NOLOCK)
					ON		(Usuario_Grupo.Id_Usuario = Usuario.Id)
					AND		(Usuario_Grupo.Id_Cliente = Usuario.Id_Cliente)
					WHERE		(Publico_Alvo_Criterio.Id_Entidade_Tipo = 'GR')
					AND		(Usuario.Fl_Status = 1)
					AND Usuario.FL_PADRAO = 0
					AND Usuario.DT_EXCLUSAO IS NULL
					AND EXISTS (SELECT * FROM @targetAudienceCriteriasAdd WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('group'))

					UNION ALL

					
			/* Usuario Perfil */
					SELECT	DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.Tx_Nome_Completo
						 , ISNULL(Usuario.Tx_Email, '') AS [Tx_Email]
						 , Usuario.Tx_Login AS [Login]
						 , 'userProfile' AS ID_CRITERIO
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					FROM		USUARIO_PERFIL_ENTIDADE (NOLOCK)
					INNER JOIN	@targetAudienceCriteriasAdd AS Publico_Alvo_Criterio
					ON		(USUARIO_PERFIL_ENTIDADE.Id_Perfil_Jogo = Publico_Alvo_Criterio.Id_Entidade)
					AND		(USUARIO_PERFIL_ENTIDADE.Id_Cliente = Publico_Alvo_Criterio.Id_Cliente)
					INNER JOIN	Usuario (NOLOCK)
					ON		(USUARIO_PERFIL_ENTIDADE.Id_Usuario = Usuario.Id)
					AND		(USUARIO_PERFIL_ENTIDADE.Id_Cliente = Usuario.Id_Cliente)
					WHERE		(Publico_Alvo_Criterio.Id_Entidade_Tipo = 'UP')
					AND		(Usuario.Fl_Status = 1)
					AND Usuario.FL_PADRAO = 0
					AND Usuario.DT_EXCLUSAO IS NULL
					AND USUARIO_PERFIL_ENTIDADE.ID_PERFIL_JOGO IN (1, 2, 3)
					AND EXISTS (SELECT * FROM @targetAudienceCriteriasAdd WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('userProfile'))

					UNION ALL

			/* Admin Perfil */
					SELECT	DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.Tx_Nome_Completo
						 , ISNULL(Usuario.Tx_Email, '') AS [Tx_Email]
						 , Usuario.Tx_Login AS [Login]
						 , 'userProfile' AS ID_CRITERIO
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					FROM		Perfil_Usuario (NOLOCK)
					INNER JOIN	@targetAudienceCriteriasAdd AS Publico_Alvo_Criterio
					ON		(Perfil_Usuario.Id_Perfil = Publico_Alvo_Criterio.Id_Entidade)
					AND		(Perfil_Usuario.Id_Cliente = Publico_Alvo_Criterio.Id_Cliente)
					INNER JOIN	Usuario (NOLOCK)
					ON		(Perfil_Usuario.Id_Usuario = Usuario.Id)
					AND		(Perfil_Usuario.Id_Cliente = Usuario.Id_Cliente)
					WHERE		(Publico_Alvo_Criterio.Id_Entidade_Tipo = 'AP')
					AND		(Usuario.Fl_Status = 1)
					AND Usuario.FL_PADRAO = 0
					AND Usuario.DT_EXCLUSAO IS NULL
					AND EXISTS (SELECT * FROM @targetAudienceCriteriasAdd WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('adminProfile'))

					UNION ALL


			/* Usuario Status */
					SELECT	DISTINCT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.Tx_Nome_Completo
						 , ISNULL(Usuario.Tx_Email, '') AS [Tx_Email]
						 , Usuario.Tx_Login AS [Login]
						 , 'userStatus' AS ID_CRITERIO
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					FROM		Usuario (NOLOCK)
					INNER JOIN	@targetAudienceCriteriasAdd AS Publico_Alvo_Criterio
					ON		(Usuario.Fl_Status = Publico_Alvo_Criterio.Id_Entidade)
					AND		(Usuario.Id_Cliente = Publico_Alvo_Criterio.Id_Cliente)
					WHERE		(Publico_Alvo_Criterio.Id_Entidade_Tipo = 'SU')
					AND Usuario.FL_PADRAO = 0
					AND Usuario.DT_EXCLUSAO IS NULL
					AND EXISTS (SELECT * FROM @targetAudienceCriteriasAdd WHERE ID_PUBLICO_ALVO_CRITERIO_TIPO IN ('userStatus'))

					UNION ALL

					
			/*Campos adicionais - Select */
					SELECT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.Tx_Nome_Completo
						 , ISNULL(Usuario.Tx_Email, '') AS [Tx_Email]
						 , Usuario.Tx_Login AS [Login]
						 , 'additional_fields_' + CamposSelect.ID_ATRIBUTO + '_' + CamposSelect.ID_ATRIBUTO_VALOR AS ID_CRITERIO
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
					  JOIN @targetAudienceCriteriasAdd AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
					  JOIN ATRIBUTO (NOLOCK)
					    ON ATRIBUTO.ID = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
					   AND ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
					   AND ATRIBUTO.ID_ATRIBUTO_TIPO = 7  -- Select Multiple
			   OUTER APPLY (SELECT ENTIDADE_ATRIBUTO.ID AS IdEntidadeAtributo
								 , TRY_CAST(ENTIDADE_ATRIBUTO.ID_ATRIBUTO AS nvarchar(18)) AS ID_ATRIBUTO
								 , TRY_CAST(ENTIDADE_ATRIBUTO.ID_ATRIBUTO_VALOR AS nvarchar(18)) AS ID_ATRIBUTO_VALOR
			                  FROM ENTIDADE_ATRIBUTO (NOLOCK)
							  JOIN ATRIBUTO (NOLOCK)
							    ON ENTIDADE_ATRIBUTO.ID_ATRIBUTO = ATRIBUTO.ID
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = ATRIBUTO.ID_CLIENTE
							   AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
							   AND ISNULL(PUBLICO_ALVO_CRITERIO.ID_ENTIDADE, 0) > 0
							   AND ATRIBUTO.FL_STATUS = 1
							 WHERE ENTIDADE_ATRIBUTO.ID_ENTIDADE = USUARIO.ID
							   AND ENTIDADE_ATRIBUTO.ID_ENTIDADE_TIPO = 'US'
							   AND ENTIDADE_ATRIBUTO.ID_ATRIBUTO = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
							   AND ENTIDADE_ATRIBUTO.ID_ATRIBUTO_VALOR = PUBLICO_ALVO_CRITERIO.ID_ENTIDADE) AS CamposSelect 				
			         WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
					   AND USUARIO.FL_STATUS = 1
					   AND CamposSelect.IdEntidadeAtributo IS NOT NULL
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.DT_EXCLUSAO IS NULL


					   UNION ALL


				  /*Campos adicionais - Multiple */
					SELECT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.Tx_Nome_Completo
						 , ISNULL(Usuario.Tx_Email, '') AS [Tx_Email]
						 , Usuario.Tx_Login AS [Login]
						 , 'additional_fields_' + CamposSelect.ID_ATRIBUTO AS ID_CRITERIO
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
					  JOIN @targetAudienceCriteriasAdd AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
					  JOIN ATRIBUTO (NOLOCK)
					    ON ATRIBUTO.ID = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
					   AND ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
					   AND ATRIBUTO.ID_ATRIBUTO_TIPO = 6  -- Select, Multiple
			   OUTER APPLY (SELECT ENTIDADE_ATRIBUTO.ID AS IdEntidadeAtributo
								 , TRY_CAST(ENTIDADE_ATRIBUTO.ID_ATRIBUTO AS nvarchar(18)) AS ID_ATRIBUTO
			                  FROM ENTIDADE_ATRIBUTO (NOLOCK)
							  JOIN ATRIBUTO (NOLOCK)
							    ON ENTIDADE_ATRIBUTO.ID_ATRIBUTO = ATRIBUTO.ID
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = ATRIBUTO.ID_CLIENTE
							   AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
							   AND ISNULL(PUBLICO_ALVO_CRITERIO.ID_ENTIDADE, 0) > 0
							   AND ATRIBUTO.FL_STATUS = 1
							 WHERE ENTIDADE_ATRIBUTO.ID_ENTIDADE = USUARIO.ID
							   AND ENTIDADE_ATRIBUTO.ID_ENTIDADE_TIPO = 'US'
							   AND ENTIDADE_ATRIBUTO.ID_ATRIBUTO = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
							   AND ENTIDADE_ATRIBUTO.ID_ATRIBUTO_VALOR = PUBLICO_ALVO_CRITERIO.ID_ENTIDADE) AS CamposSelect 				
			         WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
					   AND USUARIO.FL_STATUS = 1
					   AND CamposSelect.IdEntidadeAtributo IS NOT NULL
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.DT_EXCLUSAO IS NULL

					   UNION ALL
					
					/* Campos adicionais - TEXT */
					SELECT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.Tx_Nome_Completo
						 , ISNULL(Usuario.Tx_Email, '') AS [Tx_Email]
						 , Usuario.Tx_Login AS [Login]
						 , 'additional_fields_' + CamposText.ID_ATRIBUTO AS ID_CRITERIO
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
					  JOIN @targetAudienceCriteriasAdd AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
					  JOIN ATRIBUTO (NOLOCK)
					    ON ATRIBUTO.ID = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
					   AND ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
					   AND ATRIBUTO.ID_ATRIBUTO_TIPO = 1  -- Text
			   OUTER APPLY (SELECT ENTIDADE_ATRIBUTO.ID AS IdEntidadeAtributo
								 , TRY_CAST(ENTIDADE_ATRIBUTO.ID_ATRIBUTO AS nvarchar(18)) AS ID_ATRIBUTO
								 , ENTIDADE_ATRIBUTO.TX_VALOR
			                  FROM ENTIDADE_ATRIBUTO (NOLOCK) 
							  JOIN ATRIBUTO (NOLOCK)
							    ON ENTIDADE_ATRIBUTO.ID_ATRIBUTO = ATRIBUTO.ID
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = ATRIBUTO.ID_CLIENTE
							   AND ATRIBUTO.FL_STATUS = 1
							 WHERE ENTIDADE_ATRIBUTO.ID_ENTIDADE = USUARIO.ID
							   AND ENTIDADE_ATRIBUTO.ID_ENTIDADE_TIPO = 'US'
							   AND ENTIDADE_ATRIBUTO.ID_ATRIBUTO = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
							   AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
							   AND ISNULL(PUBLICO_ALVO_CRITERIO.ID_ENTIDADE, 0) = 0
							   AND ((PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'equal_to' AND ENTIDADE_ATRIBUTO.TX_VALOR = PUBLICO_ALVO_CRITERIO.TX_VALOR)
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'ends_with' AND ENTIDADE_ATRIBUTO.TX_VALOR LIKE '%'+PUBLICO_ALVO_CRITERIO.TX_VALOR)
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'starts_with' AND ENTIDADE_ATRIBUTO.TX_VALOR LIKE PUBLICO_ALVO_CRITERIO.TX_VALOR+'%')
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'is_empty')
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'contains' AND ENTIDADE_ATRIBUTO.TX_VALOR LIKE '%'+PUBLICO_ALVO_CRITERIO.TX_VALOR+'%'))) AS CamposText 	
			         WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
					   AND USUARIO.FL_STATUS = 1
					   AND ((PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'is_empty' AND (CamposText.TX_VALOR IS NULL OR CamposText.TX_VALOR = '')) 
							OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO != 'is_empty' AND CamposText.IdEntidadeAtributo IS NOT NULL))
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.DT_EXCLUSAO IS NULL

					   UNION ALL

				  /*Campos adicionais - Integer */
					SELECT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.Tx_Nome_Completo
						 , ISNULL(Usuario.Tx_Email, '') AS [Tx_Email]
						 , Usuario.Tx_Login AS [Login]
						 , 'additional_fields_' + CamposInteger.ID_ATRIBUTO AS ID_CRITERIO
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
					  JOIN @targetAudienceCriteriasAdd AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
					  JOIN ATRIBUTO (NOLOCK)
					    ON ATRIBUTO.ID = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
					   AND ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
					   AND ATRIBUTO.ID_ATRIBUTO_TIPO = 2  -- Integer
			   OUTER APPLY (SELECT ENTIDADE_ATRIBUTO.ID AS IdEntidadeAtributo
								 , TRY_CAST(ENTIDADE_ATRIBUTO.ID_ATRIBUTO AS nvarchar(18)) AS ID_ATRIBUTO
			                  FROM ENTIDADE_ATRIBUTO (NOLOCK) 
							  JOIN ATRIBUTO (NOLOCK)
							    ON ENTIDADE_ATRIBUTO.ID_ATRIBUTO = ATRIBUTO.ID
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = ATRIBUTO.ID_CLIENTE
							   AND ATRIBUTO.FL_STATUS = 1
							 WHERE ENTIDADE_ATRIBUTO.ID_ENTIDADE = USUARIO.ID
							   AND ENTIDADE_ATRIBUTO.ID_ENTIDADE_TIPO = 'US'
							   AND ENTIDADE_ATRIBUTO.ID_ATRIBUTO = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
							 AND ENTIDADE_ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
							   AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
							   AND ISNULL(PUBLICO_ALVO_CRITERIO.ID_ENTIDADE, 0) = 0
							   AND ((PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'equal_to' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS INT) = TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS INT))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'in_between' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS INT) BETWEEN TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS INT) AND TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR_2 AS INT))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'greater_than' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS INT) > TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS INT))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'greater_than_or_equal_to' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS INT) >= TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS INT))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'less_than' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS INT) < TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS INT))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'less_than_or_equal_to' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS INT) <= TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS INT)))) AS CamposInteger 	
			         WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
					   AND USUARIO.FL_STATUS = 1
					   AND CamposInteger.IdEntidadeAtributo IS NOT NULL
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.DT_EXCLUSAO IS NULL

					   UNION ALL

				  /*Campos adicionais - DATE */
					SELECT Usuario.Id Id_Usuario
						 , Usuario.Id_Cliente
						 , Usuario.Tx_Nome_Completo
						 , ISNULL(Usuario.Tx_Email, '') AS [Tx_Email]
						 , Usuario.Tx_Login AS [Login]
						 , 'additional_fields_' + CamposDate.ID_ATRIBUTO AS ID_CRITERIO
						 , Usuario.CRIADO_POR AS [CreatedBy]
						 , Usuario.ATUALIZADO_POR AS [UpdatedBy]
						 , Usuario.DT_ULTIMA_ATUALIZACAO AS [LastUpdateDate]
						 , Usuario.DT_CADASTRO AS [RegistrationDate]
						 , Usuario.FL_STATUS AS [Status]
					  FROM USUARIO (NOLOCK)
					  JOIN @targetAudienceCriteriasAdd AS PUBLICO_ALVO_CRITERIO
					    ON PUBLICO_ALVO_CRITERIO.ID_CLIENTE = USUARIO.ID_CLIENTE
					  JOIN ATRIBUTO (NOLOCK)
					    ON ATRIBUTO.ID = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
					   AND ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
					   AND ATRIBUTO.ID_ATRIBUTO_TIPO = 4  -- Date
			   OUTER APPLY (SELECT ENTIDADE_ATRIBUTO.ID AS IdEntidadeAtributo
								 , TRY_CAST(ENTIDADE_ATRIBUTO.ID_ATRIBUTO AS nvarchar(18)) AS ID_ATRIBUTO
			                  FROM ENTIDADE_ATRIBUTO (NOLOCK) 
							  JOIN ATRIBUTO (NOLOCK)
							    ON ENTIDADE_ATRIBUTO.ID_ATRIBUTO = ATRIBUTO.ID
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = ATRIBUTO.ID_CLIENTE
							   AND ATRIBUTO.FL_STATUS = 1
							 WHERE ENTIDADE_ATRIBUTO.ID_ENTIDADE = USUARIO.ID
							   AND ENTIDADE_ATRIBUTO.ID_ENTIDADE_TIPO = 'US'
							   AND ENTIDADE_ATRIBUTO.ID_ATRIBUTO = PUBLICO_ALVO_CRITERIO.ID_ATRIBUTO
							   AND ENTIDADE_ATRIBUTO.ID_CLIENTE = PUBLICO_ALVO_CRITERIO.ID_CLIENTE
							   AND PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
							   AND ISNULL(PUBLICO_ALVO_CRITERIO.ID_ENTIDADE, 0) = 0
							   AND ((PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'equal_to' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS DATE) = TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS DATE))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'in_between' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS DATE) BETWEEN TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS DATE) AND TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR_2 AS DATE))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'greater_than' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS DATE) > TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS DATE))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'greater_than_or_equal_to' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS DATE) >= TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS DATE))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'less_than' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS DATE) < TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS DATE))
								 OR (PUBLICO_ALVO_CRITERIO.ID_PUBLICO_ALVO_CRITERIO_TIPO = 'less_than_or_equal_to' AND TRY_CAST(ENTIDADE_ATRIBUTO.TX_VALOR AS DATE) <= TRY_CAST(PUBLICO_ALVO_CRITERIO.TX_VALOR AS DATE)))) AS CamposDate 	
					 WHERE PUBLICO_ALVO_CRITERIO.ID_ENTIDADE_TIPO = 'AV'
					   AND USUARIO.FL_STATUS = 1
					   AND CamposDate.IdEntidadeAtributo IS NOT NULL
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.DT_EXCLUSAO IS NULL
				) Resultado
		    WHERE NOT EXISTS (
				SELECT *
				  FROM #UsuarioPublicoAlvoExcecao AS UsuarioPublicoAlvoExcecao
				 WHERE UsuarioPublicoAlvoExcecao.ID_USUARIO = Resultado.ID_USUARIO
				   AND UsuarioPublicoAlvoExcecao.ID_CLIENTE = Resultado.ID_CLIENTE
			)
			GROUP BY Id_Usuario
				 , Id_Cliente
				 , NomeCompleto
				 , [Tx_Email]
				 , [Login]
				 , [CreatedBy]
				 , [UpdatedBy]
				 , [LastUpdateDate]
				 , [RegistrationDate]
				 , [Status]
				 , Id_Criterio
			--HAVING	COUNT(Id_Usuario, Id_Criterio) = @contador
		) --select * from UsuarioPublicoAlvo
		
		, ResultadoUsuarioPublicoAlvo AS (
			SELECT Id_Usuario
				 , Id_Cliente
				 , NomeCompleto
				 , [Tx_Email]
				 , [Login]
				 , [CreatedBy]
				 , [UpdatedBy]
				 , [LastUpdateDate]
				 , [RegistrationDate]
				 , [Status]
				 , COUNT(ID_USUARIO) AS TotalValidacoes
			  FROM UsuarioPublicoAlvo
		  GROUP BY Id_Usuario
				 , Id_Cliente
				 , NomeCompleto
				 , [Tx_Email]
				 , [Login]
				 , [CreatedBy]
				 , [UpdatedBy]
				 , [LastUpdateDate]
				 , [RegistrationDate]
				 , [Status]
		HAVING COUNT (ID_USUARIO) = @contador
		) SELECT Id_Usuario AS UserId
				 , Id_Cliente AS CustomerId
				 , NomeCompleto AS [Name]
				 , [Tx_Email] AS [Email]
				 , [Login] AS [Login]
				 , [CreatedBy] AS [CreatedBy]
				 , [UpdatedBy] AS [UpdatedBy]
				 , FORMAT([LastUpdateDate], 'dd/MM/yyyy HH:mm:ss') AS [LastUpdateDate]
				 , FORMAT([RegistrationDate], 'dd/MM/yyyy HH:mm:ss') AS [RegistrationDate]
				 , [Status] AS [Status]
				 , COUNT (*) OVER () AS TotalOfUsers 
			  FROM ResultadoUsuarioPublicoAlvo 
			  WHERE (@search IS NULL OR (NomeCompleto LIKE '%' + @search + '%' OR [Login] LIKE '%' + @search + '%' OR TX_EMAIL LIKE '%' + @search + '%' OR ID_USUARIO LIKE '%' + @search + '%'))
			  ORDER BY [Name]
			  OFFSET @skip ROWS
			  FETCH NEXT @take ROWS ONLY;--UsuarioPublicoAlvo
		----UPDATE Publico_Alvo SET Fl_Processado = 1 WHERE Id = @idPublicoAlvo;
	END


	/*
	
	DECLARE
      @targetAudienceCriteriasAdd PublicoAlvoCriterio 
    , @targetAudienceCriteriasException PublicoAlvoCriterio 
	, @skip INT = 0
	, @take INT = 999
	, @search NVARCHAR(255)

	INSERT INTO @targetAudienceCriteriasAdd 
	SELECT 'sanpaolo', 'AV', 189, 'contains', 0, 'Gelateir', NULL
	UNION ALL
	SELECT 'sanpaolo', 'TR', NULL, 'track', 31400, NULL, NULL
	UNION ALL
	SELECT 'sanpaolo', 'CP', NULL, 'competition', 1844, NULL, NULL
	UNION ALL
	SELECT 'sanpaolo', 'UP', NULL, 'userProfile', 2, NULL, NULL
	
	*/
	
END
GO

