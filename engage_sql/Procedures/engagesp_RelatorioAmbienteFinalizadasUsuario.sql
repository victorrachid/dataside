USE [prod_my_engage_autosservico]
GO

/****** Object:  StoredProcedure [Report].[engagesp_RelatorioAmbienteFinalizadasUsuario]    Script Date: 10/27/2025 7:19:25 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Report].[engagesp_RelatorioAmbienteFinalizadasUsuario]
	@adminId		INT = NULL
	, @clienteID	NVARCHAR(24) = NULL
	, @competicaoId	NVARCHAR(255) = NULL
	, @userStatus	NVARCHAR(20) = NULL
	, @perfilUsuarioTrilhaIds NVARCHAR (20) = NULL
	, @usuario	    NVARCHAR(255) = NULL 
	, @statusConclusaoCompeticao NVARCHAR(20) = NULL
	, @grupo NVARCHAR(255) = NULL
    , @competitionCompletionDateStartDate DATETIME = NULL
	, @competitionCompletionDateEndDate DATETIME = NULL
	, @competitionStartDateStartDate DATETIME = NULL 
	, @competitionStartDateEndDate DATETIME =  NULL 
	, @lastCompetitionAccessDateStartDate DATETIME = NULL
	, @lastCompetitionAccessDateEndDate DATETIME = NULL
	, @competitionLinkDateStartDate DATETIME = NULL
	, @competitionLinkDateEndDate DATETIME = NULL
	, @showInactiveCompetition BIT = 0
	, @showEmptyDates BIT = 0 
	, @filterAttributes PublicoAlvoCriterio READONLY
AS




BEGIN

	   IF OBJECT_ID(N'tempdb..#UsuariosAdmin', 'U') IS NOT NULL DROP TABLE #UsuariosAdmin
	   IF OBJECT_ID(N'tempdb..#CompeticoesAdmin', 'U') IS NOT NULL DROP TABLE #CompeticoesAdmin
	   IF OBJECT_ID(N'tempdb..#usuarioTrilha', 'U') IS NOT NULL DROP TABLE #usuarioTrilha
	   IF OBJECT_ID(N'tempdb..#SumarizacaoModulos', 'U') IS NOT NULL DROP TABLE #SumarizacaoModulos
	   IF OBJECT_ID(N'tempdb..#Resultado', 'U') IS NOT NULL DROP TABLE #Resultado
	   IF OBJECT_ID(N'tempdb..#TempAtributosUsuario', 'U') IS NOT NULL DROP TABLE #TempAtributosUsuario
	   IF OBJECT_ID(N'tempdb..#UsuariosAtributo', 'U') IS NOT NULL DROP TABLE #UsuariosAtributo
	   IF OBJECT_ID(N'tempdb..#ResultadoConsolidado', 'U') IS NOT NULL DROP TABLE #ResultadoConsolidado
	
	   DECLARE @subgrupos SubGrupo;
	   DECLARE @showUsersWhoHaveNeverAccessed BIT = 0; -- @todo: Cada filtro de data deve ter um filtro desses. Como ainda não existe isso, está aqui.
	   SET @showUsersWhoHaveNeverAccessed = (SELECT ISNULL(@showUsersWhoHaveNeverAccessed, 0));
	   
		SELECT EntidadeId AS UsuarioID
			 , ClienteID 
			 , Grupos.TodosGruposUsuario
		  INTO #UsuariosAdmin
		  FROM fnt_EntidadesEditaveis(@adminId, @clienteID, 'US', @subgrupos) AS EntidadesEditaveis		
   OUTER APPLY fnt_TodosGruposUsuarios (EntidadesEditaveis.EntidadeId, EntidadesEditaveis.ClienteID, @grupo) AS Grupos
		CREATE UNIQUE CLUSTERED INDEX IX_UsuarioAdmin ON #UsuariosAdmin (UsuarioID, ClienteId)


	   SELECT EntidadeId AS CompeticaoId
		    , ClienteID
		 INTO #CompeticoesAdmin
	     FROM fnt_EntidadesEditaveis (@adminId, @clienteID, 'CP', @subgrupos) AS EntidadesEditaveis;
	   CREATE UNIQUE CLUSTERED INDEX IX_CompeticaoAdmin ON #CompeticoesAdmin (CompeticaoId, ClienteId)

	   CREATE TABLE #UsuariosAtributo (
			UserId INT,
			CustomerId NVARCHAR(50),
			Name NVARCHAR(255),
			Email NVARCHAR(255),
			Login NVARCHAR(255),
			CreatedBy INT,
			UpdatedBy INT,
			LastUpdateDate NVARCHAR (255),
			RegistrationDate NVARCHAR (255),
			Status BIT,
			TotalOfUsers INT
		);


		IF EXISTS (SELECT TOP 1 1 FROM @filterAttributes)
        BEGIN
	        DECLARE @p5 dbo.PublicoAlvoCriterio
            EXEC engagesp_GetUsuarioByTargetAudience @skip=0, @take=99999, @search=NULL, @targetAudienceCriteriasAdd=@filterAttributes, @targetAudienceCriteriasException=@p5
        END


	   CREATE TABLE #UsuarioTrilha (TrilhaId INT NOT NULL, ClienteId NVARCHAR(24) COLLATE LAtin1_General_CI_AI NOT NULL, UsuarioId INT NOT NULL, CompeticaoId INT NOT NULL, CompeticaoNome NVARCHAR(200) NOT NULL)

	   IF ISNULL(@competicaoId, '') != ''
	   BEGIN
		   ;WITH UsuarioTrilha
			AS
			(
					SELECT DISTINCT 
						   CompeticaoTrilha.ID_TRILHA AS TrilhaId
						 , CompeticaoTrilha.ID_CLIENTE AS ClienteId
						 , UsuarioGrupo.ID_USUARIO AS UsuarioId
						 , CompeticaoTrilha.ID_COMPETICAO  AS CompeticaoId
						 , COMPETICAO.TX_NOME AS CompeticaoNome
					  FROM COMPETICAO_TRILHA AS CompeticaoTrilha (NOLOCK)
					  JOIN COMPETICAO_GRUPO AS CompeticaoGrupo (NOLOCK)
						ON CompeticaoGrupo.ID_COMPETICAO = CompeticaoTrilha.ID_COMPETICAO
					   AND CompeticaoGrupo.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
					  JOIN COMPETICAO (NOLOCK)
						ON COMPETICAO.ID = CompeticaoTrilha.ID_COMPETICAO
					   AND COMPETICAO.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
					   AND (ISNULL(@showInactiveCompetition, 0) = 1 OR COMPETICAO.FL_DISPONIVEL = 1)
					  JOIN USUARIO_GRUPO AS UsuarioGrupo (NOLOCK)
						ON UsuarioGrupo.ID_GRUPO = CompeticaoGrupo.ID_GRUPO
					   AND UsuarioGrupo.ID_CLIENTE = CompeticaoGrupo.ID_CLIENTE
					  JOIN TRILHA AS Trilha (NOLOCK)
						ON Trilha.ID = CompeticaoTrilha.ID_TRILHA
					   AND Trilha.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
					   AND Trilha.FL_STATUS = 1
					   AND Trilha.FL_VISIVEL = 1
				 LEFT JOIN #UsuariosAtributo ON UsuarioGrupo.ID_USUARIO = UserId
					 WHERE CompeticaoTrilha.ID_COMPETICAO IN (SELECT OriginalValue 
					                                            FROM fnt_ConverterFiltrosParaTabela(@competicaoId, 'competicaoId'))
					   AND (ISNULL(@usuario, '') = '' OR UsuarioGrupo.ID_USUARIO IN (SELECT OriginalValue
					                                                                   FROM fnt_ConverterFiltrosParaTabela(@usuario, 'usuario')))
					   AND EXISTS (SELECT *
									 FROM RODADA AS RD (NOLOCK)
									 JOIN USUARIO_PERFIL_ENTIDADE AS UPE (NOLOCK)
									   ON UPE.ID_ENTIDADE = RD.ID
									  AND UPE.ID_ENTIDADE_TIPO = 'RD'
									  AND UPE.ID_CLIENTE = RD.ID_CLIENTE
									  AND RD.FL_STATUS = 1
									  AND UPE.ID_PERFIL_JOGO IN (1, 2, 3)
									WHERE UPE.ID_USUARIO = UsuarioGrupo.ID_USUARIO
									  AND UPE.ID_CLIENTE = UsuarioGrupo.ID_CLIENTE
									  AND RD.ID_TRILHA = CompeticaoTrilha.ID_TRILHA
									  AND RD.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
									  AND (ISNULL(@perfilUsuarioTrilhaIds, '') = '' OR UPE.ID_PERFIL_JOGO in (SELECT OriginalValue
			                                                                                                    FROM fnt_ConverterFiltrosParaTabela(@perfilUsuarioTrilhaIds, 'perfilUsuarioTrilhaIds')))
									  AND ((@competitionLinkDateStartDate IS NULL AND @competitionLinkDateEndDate IS NULL) 
									       OR UPE.DT_CADASTRO BETWEEN @competitionLinkDateStartDate AND @competitionLinkDateEndDate))
						  AND EXISTS (SELECT *
		 							    FROM #UsuariosAdmin AS UsuariosAdmin
		 							   WHERE UsuariosAdmin.UsuarioId = UsuarioGrupo.ID_USUARIO
		 							     AND UsuariosAdmin.ClienteId = UsuarioGrupo.ID_CLIENTE)
						  AND EXISTS (SELECT *
		 							    FROM #CompeticoesAdmin AS CompeticoesAdmin
		 							   WHERE CompeticoesAdmin.CompeticaoId = COMPETICAO.ID
		 							     AND CompeticoesAdmin.ClienteId = COMPETICAO.ID_CLIENTE)
					   AND CompeticaoTrilha.ID_CLIENTE = @clienteId
			) INSERT #UsuarioTrilha (TrilhaId, ClienteId, UsuarioId, CompeticaoId, CompeticaoNome)
			  SELECT TrilhaId
				   , ClienteId
				   , UsuarioId
				   , CompeticaoId
				   , CompeticaoNome
			    FROM UsuarioTrilha
	   
	   END
	   ELSE 
	   BEGIN
			;WITH UsuarioTrilha
			AS
			(
					SELECT DISTINCT 
						   CompeticaoTrilha.ID_TRILHA AS TrilhaId
						 , CompeticaoTrilha.ID_CLIENTE AS ClienteId
						 , UsuarioGrupo.ID_USUARIO AS UsuarioId
						 , CompeticaoTrilha.ID_COMPETICAO  AS CompeticaoId
						 , COMPETICAO.TX_NOME AS CompeticaoNome
					  FROM COMPETICAO_TRILHA AS CompeticaoTrilha (NOLOCK)
					  JOIN COMPETICAO_GRUPO AS CompeticaoGrupo (NOLOCK)
						ON CompeticaoGrupo.ID_COMPETICAO = CompeticaoTrilha.ID_COMPETICAO
					   AND CompeticaoGrupo.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
					  JOIN USUARIO_GRUPO AS UsuarioGrupo (NOLOCK)
						ON UsuarioGrupo.ID_GRUPO = CompeticaoGrupo.ID_GRUPO
					   AND UsuarioGrupo.ID_CLIENTE = CompeticaoGrupo.ID_CLIENTE
					  JOIN COMPETICAO (NOLOCK)
						ON COMPETICAO.ID = CompeticaoTrilha.ID_COMPETICAO
					   AND COMPETICAO.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
					   AND (ISNULL(@showInactiveCompetition, 0) = 1 OR COMPETICAO.FL_DISPONIVEL = 1)
					  JOIN TRILHA AS Trilha (NOLOCK)
						ON Trilha.ID = CompeticaoTrilha.ID_TRILHA
					   AND Trilha.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
					   AND Trilha.FL_STATUS = 1
					   AND Trilha.FL_VISIVEL = 1
					 WHERE EXISTS (SELECT *
									 FROM RODADA AS RD (NOLOCK)
									 JOIN USUARIO_PERFIL_ENTIDADE AS UPE (NOLOCK)
									   ON UPE.ID_ENTIDADE = RD.ID
									  AND UPE.ID_ENTIDADE_TIPO = 'RD'
									  AND UPE.ID_CLIENTE = RD.ID_CLIENTE
									  AND RD.FL_STATUS = 1
									  AND UPE.ID_PERFIL_JOGO IN (1, 2, 3)
									WHERE UPE.ID_USUARIO = UsuarioGrupo.ID_USUARIO
									  AND UPE.ID_CLIENTE = UsuarioGrupo.ID_CLIENTE
									  AND RD.ID_TRILHA = CompeticaoTrilha.ID_TRILHA
									  AND RD.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
									  AND (ISNULL(@perfilUsuarioTrilhaIds, '') = '' OR UPE.ID_PERFIL_JOGO in (SELECT OriginalValue
			                                                                                                    FROM fnt_ConverterFiltrosParaTabela(@perfilUsuarioTrilhaIds, 'perfilUsuarioTrilhaIds'))))
					   AND (ISNULL(@usuario, '') = '' OR UsuarioGrupo.ID_USUARIO IN (SELECT OriginalValue
					                                                                   FROM fnt_ConverterFiltrosParaTabela(@usuario, 'usuario')))
					   AND CompeticaoTrilha.ID_CLIENTE = @clienteId
					    -- Adicionado por conta do ticket abaixo
						-- https://engage-bz.freshdesk.com/a/tickets/80167 --
						AND EXISTS (SELECT *
		 							 FROM #UsuariosAdmin AS UsuariosAdmin
		 							WHERE UsuariosAdmin.UsuarioId = UsuarioGrupo.ID_USUARIO
		 							  AND UsuariosAdmin.ClienteId = UsuarioGrupo.ID_CLIENTE)
					    AND EXISTS (SELECT *
		 							 FROM #CompeticoesAdmin AS CompeticoesAdmin
		 							WHERE CompeticoesAdmin.CompeticaoId = COMPETICAO.ID
		 							  AND CompeticoesAdmin.ClienteId = COMPETICAO.ID_CLIENTE)
					    -------------------------
			) INSERT #UsuarioTrilha (TrilhaId, ClienteId, UsuarioId, CompeticaoId, CompeticaoNome)
			  SELECT TrilhaId
				   , ClienteId
				   , UsuarioId
				   , CompeticaoId
				   , CompeticaoNome
			    FROM UsuarioTrilha
	   
	   END
		
		;WITH Sumarizacao
		AS
		(
				SELECT 
					   UsuarioTrilha.CompeticaoId AS CompeticaoId 
					 , UsuarioTrilha.CompeticaoNome AS CompeticaoNome
					 , Usuario.ID AS UsuarioID
					 , Usuario.ID_CLIENTE AS ClienteID
					 , GrupoPaiId
					 , GrupoPai
					 , GrupoFilhoId
					 , GrupoFilho
					 , Usuario.TX_LOGIN AS [Login]
					 , DataUltimaAtualizacaoTentativa
					 , CASE WHEN Usuario.FL_STATUS = 1 THEN 'Ativo' ELSE 'Inativo' END AS [Status] 
					 , Usuario.TX_NOME_COMPLETO AS Usuario
					 , Usuario.TX_EMAIL AS Email
					 , CONVERT(VARCHAR(10), Usuario.DT_CADASTRO, 103) + ' ' + CONVERT(VARCHAR(5), Usuario.DT_CADASTRO, 108) AS DataCadastroUsuario
					 , Trilha.ID AS TrilhaID
					 , Trilha.TX_DESCRICAO AS Trilha
					 , MIN(DataCadastroTentativa) AS DataInicioTrilha
					 , MAX(DataUltimaAtualizacaoTentativa) AS DataConclusaoTrilha
					 , COUNT(Rodada.ID) AS TotalMissoes
					 , COUNT(CASE WHEN UltimaTentativa.TentativaId IS NOT NULL THEN 1 END) AS TotalMissoesIniciadas
					 , COUNT(CASE WHEN UltimaTentativa.EstaConcluida = 1 THEN 1 END) AS TotalMissoesConcluidas
					 , COUNT(CASE WHEN UltimaTentativa.EstaAprovada = 1 THEN 1 END) AS TotalMissoesAprovadas
					 , CONVERT(DECIMAL(18, 2), ISNULL(SUM(CASE WHEN UltimaTentativa.EstaConcluida = 1 THEN UltimaTentativa.Pontuacao END), 0)) AS PontuacaoNaTrilha
					 , SUM(TempoAcessoTentativaEmSegundos) AS TempoAcessoTrilhaEmSegundos
					 , Rodada.SomaCargaHorariaModulos
					 , AcessoEntidade.DT_ULTIMO_ACESSO AS DtUltimoAcessoAmbiente
					 , COUNT(CASE WHEN Rodada.DT_INICIO > GETDATE() THEN 1 END) AS TotalMissoesNaoLiberadas
				  FROM #UsuarioTrilha AS UsuarioTrilha
				  JOIN USUARIO AS Usuario (NOLOCK)
				    ON Usuario.ID = UsuarioTrilha.UsuarioID
				   AND Usuario.ID_CLIENTE = UsuarioTrilha.ClienteID
				   AND (Usuario.FL_STATUS IN (SELECT OriginalValue 
				                                FROM fnt_ConverterFiltrosParaTabela(@userStatus, 'userStatus')) OR ISNULL(@userStatus, '') = '')
				   AND Usuario.FL_PADRAO = 0
				   AND Usuario.DT_EXCLUSAO IS NULL
			 LEFT JOIN ACESSO_ENTIDADE AS AcessoEntidade (NOLOCK)
				    ON AcessoEntidade.ID_USUARIO = UsuarioTrilha.UsuarioID
				   AND AcessoEntidade.ID_CLIENTE = UsuarioTrilha.ClienteId
				   AND AcessoEntidade.ID_ENTIDADE = UsuarioTrilha.CompeticaoId
				   AND AcessoEntidade.ID_ENTIDADE_TIPO = 'CP'
		   OUTER APPLY (SELECT TOP 1 Grupo.TX_NOME AS GrupoFilho
							 , Grupo.ID AS GrupoFilhoId
							 , Pai.TX_NOME AS GrupoPai
							 , Pai.ID AS GrupoPaiId
						  FROM USUARIO_GRUPO AS UsuarioGrupo (NOLOCK)
						  JOIN COMPETICAO_GRUPO AS CompeticaoGrupo (NOLOCK)
						    ON CompeticaoGrupo.ID_GRUPO = UsuarioGrupo.ID_GRUPO
						   AND CompeticaoGrupo.ID_CLIENTE = UsuarioGrupo.ID_CLIENTE
						   AND CompeticaoGrupo.ID_COMPETICAO = UsuarioTrilha.CompeticaoId
						  JOIN GRUPO AS Grupo (NOLOCK)
						    ON Grupo.ID = UsuarioGrupo.ID_GRUPO
						   AND Grupo.ID_CLIENTE = UsuarioGrupo.ID_CLIENTE
						   AND Grupo.FL_STATUS = 1
					 LEFT JOIN GRUPO AS Pai (NOLOCK)
							ON Pai.ID = Grupo.ID_GRUPO_PAI
						   AND Pai.ID_CLIENTE = Grupo.ID_CLIENTE
						   AND Grupo.FL_STATUS = 1
						 WHERE UsuarioGrupo.ID_USUARIO = Usuario.ID
						   AND UsuarioGrupo.ID_CLIENTE = Usuario.ID_CLIENTE
						   AND (ISNULL(@grupo, '') = '' 
						    OR (GRUPO.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@grupo, 'grupoId')) 
							OR    Pai.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@grupo, 'grupoId'))))
					  ORDER BY CASE WHEN Pai.ID IS NOT NULL THEN 2
									WHEN Grupo.ID IS NOT NULL THEN 3
									ELSE 4
						  		 END ASC
						   ) AS GruposUsuario
				  JOIN TRILHA AS Trilha (NOLOCK)
				    ON Trilha.ID = UsuarioTrilha.TrilhaId
				   AND Trilha.ID_CLIENTE = UsuarioTrilha.ClienteId
				   AND Trilha.FL_STATUS = 1
				   AND Trilha.FL_VISIVEL = 1
		    OUTER APPLY ( SELECT RODADA.ID
							   , RODADA.FL_PONTUACAO_TEMPO_RESTANTE
							   , RODADA.NU_CARGA_HORARIA
							   , SUM(ISNULL(Rodada.NU_CARGA_HORARIA, 0)) AS SomaCargaHorariaModulos
							   , RODADA.DT_INICIO
							FROM RODADA (NOLOCK)
						   WHERE RODADA.ID_TRILHA = TRILHA.ID
							 AND RODADA.ID_CLIENTE = TRILHA.ID_CLIENTE
							 AND RODADA.FL_STATUS = 1
							 AND RODADA.FL_VISIVEL = 1
						GROUP BY RODADA.ID
							   , RODADA.FL_PONTUACAO_TEMPO_RESTANTE
							   , RODADA.NU_CARGA_HORARIA
							   , RODADA.DT_INICIO
		      ) Rodada
		   OUTER APPLY (SELECT TOP 1
							   TENTATIVA.ID AS TentativaId
							 , IIF (Tentativa.ID_STATUS IN('aguardando_correcao', 'concluido', 'aprovado', 'reprovado', 'realizado_fora_prazo'), 1, 0) AS EstaConcluida
							 , IIF (Tentativa.ID_STATUS IN('concluido', 'aprovado'), 1, 0) AS EstaAprovada
							 , ISNULL(Tentativa.NU_PONTUACAO, 0) + ISNULL(Tentativa.NU_BONUS_DESAFIO, 0) +
									CASE WHEN Rodada.FL_PONTUACAO_TEMPO_RESTANTE = 1 THEN ISNULL(Tentativa.NU_PONTUACAO_TEMPO_RESTANTE, 0) ELSE 0 END AS Pontuacao
							 , CASE WHEN ISNULL(Rodada.NU_CARGA_HORARIA, 0) > 0 THEN IIF (Tentativa.ID_STATUS IN('concluido', 'aprovado', 'reprovado', 'realizado_fora_prazo'), Rodada.NU_CARGA_HORARIA * 60, NULL)
									ELSE TENTATIVA.NU_TEMPO_ACESSO_EM_SEGUNDOS
								END AS TempoAcessoTentativaEmSegundos
							 , Tentativa.DT_CADASTRO AS DataCadastroTentativa					 
							 , Tentativa.DT_ULTIMA_ATUALIZACAO AS DataUltimaAtualizacaoTentativa
						  FROM TENTATIVA AS Tentativa (NOLOCK)
						 WHERE Tentativa.ID_USUARIO = Usuario.ID
						   AND Tentativa.ID_CLIENTE = Usuario.ID_CLIENTE
						   AND Tentativa.ID_RODADA = Rodada.ID
						   AND Tentativa.ID_STATUS != 'cancelado'
						   AND USUARIO.FL_PADRAO = 0
						   --AND ((@usuariosNuncaAcessaram = 1) OR (@usuariosNuncaAcessaram = 0 AND Tentativa.DT_CADASTRO IS NOT NULL) OR @usuariosNuncaAcessaram IS NULL)
					  ORDER BY Tentativa.ID DESC) AS UltimaTentativa
			  GROUP BY Usuario.ID
					 , Usuario.ID_CLIENTE
					 , Usuario.TX_LOGIN
					 , Usuario.TX_NOME_COMPLETO
					 , Usuario.TX_EMAIL
					 , Usuario.FL_STATUS
					 , Usuario.DT_CADASTRO
					 , Trilha.ID
					 , Trilha.TX_DESCRICAO
					 , CompeticaoId
					 , CompeticaoNome
					 , GrupoPai
					 , GrupoPaiId
					 , GrupoFilho
					 , GrupoFilhoId
					 , DataUltimaAtualizacaoTentativa
					 , SomaCargaHorariaModulos
					 , AcessoEntidade.DT_ULTIMO_ACESSO
				HAVING COUNT(Rodada.ID) > 0 -- Elimina da conta trilhas que não possuem módulo
		) 
		, SumarizacaoLink
		AS
		(
				SELECT UsuarioTrilha.CompeticaoId AS CompeticaoId 
					 , UsuarioTrilha.CompeticaoNome AS CompeticaoNome
					 , Usuario.ID AS UsuarioID
					 , Usuario.ID_CLIENTE AS ClienteID
					 , GrupoPaiId
					 , GrupoPai
					 , GrupoFilhoId
					 , GrupoFilho
					 , Usuario.TX_LOGIN AS [Login]
					 , DataUltimaAtualizacaoTentativa
					 , CASE WHEN Usuario.FL_STATUS = 1 THEN 'Ativo' ELSE 'Inativo' END AS [Status] 
					 , Usuario.TX_NOME_COMPLETO AS Usuario
					 , Usuario.TX_EMAIL AS Email
					 , CONVERT(VARCHAR(10), Usuario.DT_CADASTRO, 103) + ' ' + CONVERT(VARCHAR(5), Usuario.DT_CADASTRO, 108) AS DataCadastroUsuario
					 , Trilha.ID AS TrilhaID
					 , Trilha.TX_DESCRICAO AS Trilha
					 , MIN(RodadaLink.DT_CADASTRO) AS DataInicioTrilha
					 , MAX(DataUltimaAtualizacaoTentativa) AS DataConclusaoTrilha
					 , COUNT(RodadaTarget.ID) AS TotalMissoes
					 , COUNT(CASE WHEN UltimaTentativa.TentativaId IS NOT NULL THEN 1 END) AS TotalMissoesIniciadas
					 , COUNT(CASE WHEN UltimaTentativa.EstaConcluida = 1 THEN 1 END) AS TotalMissoesConcluidas
					 , COUNT(CASE WHEN UltimaTentativa.EstaAprovada = 1 THEN 1 END) AS TotalMissoesAprovadas
					 , CONVERT(DECIMAL(18, 2), ISNULL(SUM(CASE WHEN UltimaTentativa.EstaConcluida = 1 THEN UltimaTentativa.Pontuacao END), 0)) AS PontuacaoNaTrilha
					 , SUM(TempoAcessoTentativaEmSegundos) AS TempoAcessoTrilhaEmSegundos
					 , RodadaTarget.SomaCargaHorariaModulos
					 , AcessoEntidade.DT_ULTIMO_ACESSO AS DtUltimoAcessoAmbiente
					 , COUNT(CASE WHEN RodadaTarget.DT_INICIO > GETDATE() THEN 1 END) AS TotalMissoesNaoLiberadas
				  FROM #UsuarioTrilha AS UsuarioTrilha
				  JOIN USUARIO AS Usuario (NOLOCK)
				    ON Usuario.ID = UsuarioTrilha.UsuarioID
				   AND Usuario.ID_CLIENTE = UsuarioTrilha.ClienteID
				   AND (Usuario.FL_STATUS IN (SELECT OriginalValue 
				                                FROM fnt_ConverterFiltrosParaTabela(@userStatus, 'userStatus')) OR ISNULL(@userStatus, '') = '')
				   AND Usuario.FL_PADRAO = 0
				   AND Usuario.DT_EXCLUSAO IS NULL
			 LEFT JOIN ACESSO_ENTIDADE AS AcessoEntidade (NOLOCK)
				    ON AcessoEntidade.ID_USUARIO = UsuarioTrilha.UsuarioID
				   AND AcessoEntidade.ID_CLIENTE = UsuarioTrilha.ClienteId
				   AND AcessoEntidade.ID_ENTIDADE = UsuarioTrilha.CompeticaoId
				   AND AcessoEntidade.ID_ENTIDADE_TIPO = 'CP'
		   OUTER APPLY (SELECT TOP 1 Grupo.TX_NOME AS GrupoFilho
							 , Grupo.ID AS GrupoFilhoId
							 , Pai.TX_NOME AS GrupoPai
							 , Pai.ID AS GrupoPaiId
						  FROM USUARIO_GRUPO AS UsuarioGrupo (NOLOCK)
						  JOIN COMPETICAO_GRUPO AS CompeticaoGrupo (NOLOCK)
						    ON CompeticaoGrupo.ID_GRUPO = UsuarioGrupo.ID_GRUPO
						   AND CompeticaoGrupo.ID_CLIENTE = UsuarioGrupo.ID_CLIENTE
						   AND CompeticaoGrupo.ID_COMPETICAO = UsuarioTrilha.CompeticaoId
						  JOIN GRUPO AS Grupo (NOLOCK)
						    ON Grupo.ID = UsuarioGrupo.ID_GRUPO
						   AND Grupo.ID_CLIENTE = UsuarioGrupo.ID_CLIENTE
						   AND Grupo.FL_STATUS = 1
					 LEFT JOIN GRUPO AS Pai (NOLOCK)
							ON Pai.ID = Grupo.ID_GRUPO_PAI
						   AND Pai.ID_CLIENTE = Grupo.ID_CLIENTE
						   AND Grupo.FL_STATUS = 1
						 WHERE UsuarioGrupo.ID_USUARIO = Usuario.ID
						   AND UsuarioGrupo.ID_CLIENTE = Usuario.ID_CLIENTE
						   AND (ISNULL(@grupo, '') = '' 
						    OR (GRUPO.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@grupo, 'grupoId')) 
							OR    Pai.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@grupo, 'grupoId'))))
					  ORDER BY CASE WHEN Pai.ID IS NOT NULL THEN 2
									WHEN Grupo.ID IS NOT NULL THEN 3
									ELSE 4
						  		 END ASC
						   ) AS GruposUsuario
				  JOIN TRILHA AS Trilha (NOLOCK)
				    ON Trilha.ID = UsuarioTrilha.TrilhaId
				   AND Trilha.ID_CLIENTE = UsuarioTrilha.ClienteId
				   AND Trilha.FL_STATUS = 1
				   AND Trilha.FL_VISIVEL = 1
			      JOIN RODADA_LINK AS RodadaLink (NOLOCK)
				    ON RodadaLink.ID_TRILHA = Trilha.ID
				   AND RodadaLink.ID_CLIENTE = Trilha.ID_CLIENTE
				   AND RodadaLink.TX_TIPO = 'round'
				   AND RodadaLink.FL_STATUS = 1
		   OUTER APPLY (SELECT  
						 RODADA.ID
	                   , RODADA.FL_PONTUACAO_TEMPO_RESTANTE
					   , RODADA.NU_CARGA_HORARIA
					   , SUM(ISNULL(RODADA.NU_CARGA_HORARIA,0)) AS SomaCargaHorariaModulos
					   , RODADA.DT_INICIO
				    FROM RODADA (NOLOCK)
				   WHERE RODADA.ID = RodadaLink.ID_RODADA_URL
			         AND RODADA.ID_CLIENTE = RodadaLink.ID_CLIENTE
				     AND RODADA.FL_STATUS = 1
					 AND RODADA.FL_VISIVEL = 1
				GROUP BY RODADA.ID
	                   , RODADA.FL_PONTUACAO_TEMPO_RESTANTE
					   , RODADA.NU_CARGA_HORARIA
					   , RODADA.DT_INICIO ) RodadaTarget
		   OUTER APPLY (SELECT TOP 1
							   TENTATIVA.ID AS TentativaId
							 , IIF (Tentativa.ID_STATUS IN('aguardando_correcao', 'concluido', 'aprovado', 'reprovado', 'realizado_fora_prazo'), 1, 0) AS EstaConcluida
							 , IIF (Tentativa.ID_STATUS IN('concluido', 'aprovado'), 1, 0) AS EstaAprovada
							 , ISNULL(Tentativa.NU_PONTUACAO, 0) + ISNULL(Tentativa.NU_BONUS_DESAFIO, 0) +
									CASE WHEN RodadaTarget.FL_PONTUACAO_TEMPO_RESTANTE = 1 THEN ISNULL(Tentativa.NU_PONTUACAO_TEMPO_RESTANTE, 0) ELSE 0 END AS Pontuacao
							 , CASE WHEN ISNULL(RodadaTarget.NU_CARGA_HORARIA, 0) > 0 THEN IIF (Tentativa.ID_STATUS IN('concluido', 'aprovado', 'reprovado', 'realizado_fora_prazo'), RodadaTarget.NU_CARGA_HORARIA * 60, NULL)
									ELSE TENTATIVA.NU_TEMPO_ACESSO_EM_SEGUNDOS
								END AS TempoAcessoTentativaEmSegundos
							 , Tentativa.DT_CADASTRO AS DataCadastroTentativa					 
							 , Tentativa.DT_ULTIMA_ATUALIZACAO AS DataUltimaAtualizacaoTentativa
						  FROM TENTATIVA AS Tentativa (NOLOCK)
						 WHERE Tentativa.ID_USUARIO = Usuario.ID
						   AND Tentativa.ID_CLIENTE = Usuario.ID_CLIENTE
						   AND Tentativa.ID_RODADA = RodadaTarget.ID
						   AND Tentativa.ID_STATUS != 'cancelado'
						   AND USUARIO.FL_PADRAO = 0
					  ORDER BY Tentativa.ID DESC) AS UltimaTentativa
			  GROUP BY Usuario.ID
					 , Usuario.ID_CLIENTE
					 , Usuario.TX_LOGIN
					 , Usuario.TX_NOME_COMPLETO
					 , Usuario.TX_EMAIL
					 , Usuario.FL_STATUS
					 , Usuario.DT_CADASTRO
					 , Trilha.ID
					 , Trilha.TX_DESCRICAO
					 , CompeticaoId
					 , CompeticaoNome
					 , GrupoPai
					 , GrupoPaiId
					 , GrupoFilho
					 , GrupoFilhoId
					 , DataUltimaAtualizacaoTentativa
					 , SomaCargaHorariaModulos
					 , AcessoEntidade.DT_ULTIMO_ACESSO
				HAVING COUNT(RodadaTarget.ID) > 0 -- Elimina da conta trilhas que não possuem módulo
		) SELECT *
		    INTO #SumarizacaoModulos
			FROM Sumarizacao
		   UNION
		  SELECT * 
		    FROM SumarizacaoLink

		;WITH SumarizacaoTrilhas
		AS
		(
				  SELECT Sumarizacao.CompeticaoId
					   , Sumarizacao.CompeticaoNome
					   , Sumarizacao.UsuarioID
					   , Sumarizacao.ClienteID
					   , Sumarizacao.GrupoPaiId
					   , Sumarizacao.GrupoPai
					   , Sumarizacao.GrupoFilhoId
					   , Sumarizacao.GrupoFilho
					   , Sumarizacao.Login
					   , Sumarizacao.[Status]
					   , Sumarizacao.Usuario
					   , Sumarizacao.Email
					   , Sumarizacao.DataCadastroUsuario
					   , Sumarizacao.TrilhaID
					   , Sumarizacao.Trilha
					   , MIN(Sumarizacao.DtUltimoAcessoAmbiente) AS DtUltimoAcessoAmbiente
					   , SUM(Sumarizacao.TotalMissoes) AS TotalMissoes
					   , SUM(Sumarizacao.TotalMissoesConcluidas) AS TotalMissoesConcluidas
					   , SUM(Sumarizacao.TotalMissoesAprovadas) AS TotalMissoesAprovadas
					   , SUM(Sumarizacao.PontuacaoNaTrilha) AS PontuacaoNaTrilha
					   , MIN(Sumarizacao.DataInicioTrilha) AS DataInicioTrilha
					   , MAX(Sumarizacao.DataConclusaoTrilha) AS DataConclusaoTrilha
					   , MAX(Sumarizacao.DataUltimaAtualizacaoTentativa) AS DataUltimaAtualizacaoTentativa
					   , SUM(PontuacaoNaTrilha) AS PontuacaoNaCompeticao
					   , SUM(TempoAcessoTrilhaEmSegundos) AS TempoAcessoTrilhaEmSegundos
					   , SUM(Sumarizacao.SomaCargaHorariaModulos) AS SomaCargaHorariaModulos
					   , SUM (TotalMissoesIniciadas) AS TotalMissoesIniciadas
					   , SUM(TotalMissoesNaoLiberadas) AS TotalMissoesNaoLiberadas
					FROM #SumarizacaoModulos AS Sumarizacao
				GROUP BY Sumarizacao.CompeticaoId
					   , Sumarizacao.CompeticaoNome
					   , Sumarizacao.UsuarioID
					   , Sumarizacao.ClienteID
					   , Sumarizacao.GrupoPaiId
					   , Sumarizacao.GrupoPai
					   , Sumarizacao.GrupoFilhoId
					   , Sumarizacao.GrupoFilho
					   , Sumarizacao.Login
					   , Sumarizacao.[Status]
					   , Sumarizacao.Usuario
					   , Sumarizacao.Email
					   , Sumarizacao.DataCadastroUsuario
					   , Sumarizacao.TrilhaID
					   , Sumarizacao.Trilha
		) -- SELECT * FROM SumarizacaoTrilhas end
		, Resultado
		AS
		(
				  SELECT SumarizacaoTrilhas.CompeticaoId
					   , SumarizacaoTrilhas.CompeticaoNome
					   , SumarizacaoTrilhas.UsuarioId
					   , SumarizacaoTrilhas.GrupoPaiId
					   , SumarizacaoTrilhas.GrupoPai
					   , SumarizacaoTrilhas.GrupoFilhoId
					   , SumarizacaoTrilhas.GrupoFilho
					   , SumarizacaoTrilhas.[Login]
					   , SumarizacaoTrilhas.Status
					   , SumarizacaoTrilhas.Usuario
					   , SumarizacaoTrilhas.Email
					   , SumarizacaoTrilhas.DtUltimoAcessoAmbiente
					   , MIN(SumarizacaoTrilhas.DataCadastroUsuario) AS DataCadastroUsuario
					   , MAX(SumarizacaoTrilhas.DataUltimaAtualizacaoTentativa) AS DataUltimaAtualizacaoTentativa
					   , SUM(PontuacaoNaCompeticao) AS PontuacaoNaCompeticao
					   , COUNT(TrilhaID) AS TotalTrilhas
					   , COUNT(CASE WHEN TrilhaIniciada = 'Sim' THEN 1 END) AS TotalTrilhasIniciadas
					   , COUNT(CASE WHEN TrilhaConcluida = 'Sim' THEN 1 END) AS TotalTrilhasConcluidas
					   , SUM(CASE WHEN TotalMissoes > 0 AND TotalMissoesAprovadas >= TotalMissoes THEN 1 ELSE 0 END) AS TotalTrilhasAprovadas
					   , SUM(CASE WHEN TotalMissoes > 0 AND TotalMissoesNaoLiberadas >= TotalMissoes THEN 1 ELSE 0 END) AS TotalTrilhasNaoLiberadas
					   , SUM(ISNULL(TempoAcessoTrilhaEmSegundos, 0)) AS TempoAcessoCompeticaoEmSegundos
					   , MIN(DataInicioTrilha) AS DataInicioCompeticao
					   , MAX(DataConclusaoTrilha) AS DataTerminoCompeticao
					   , dbo.FormatarSegundosEmhorasMinSeg(SUM(CASE WHEN ISNULL(SumarizacaoTrilhas.SomaCargaHorariaModulos, 0) > 0 THEN ISNULL(SomaCargaHorariaModulos, 0) * 60
														            ELSE ISNULL(TempoAcessoTrilhaEmSegundos, 0) END)
													       ) AS TempoAcessoNoAmbienteEmHoras
					FROM SumarizacaoTrilhas
				OUTER APPLY (SELECT IIF (TotalMissoesIniciadas > 0, 'Sim', 'Não') AS TrilhaIniciada
							 , IIF(TotalMissoes = 0, 'Não', IIF(TotalMissoesConcluidas >= TotalMissoes, 'Sim', 'Não')) AS TrilhaConcluida) AS ValoresComputados

				GROUP BY SumarizacaoTrilhas.CompeticaoId
					   , SumarizacaoTrilhas.CompeticaoNome
					   , SumarizacaoTrilhas.UsuarioId
					   , SumarizacaoTrilhas.GrupoPaiId
					   , SumarizacaoTrilhas.GrupoPai
					   , SumarizacaoTrilhas.GrupoFilhoId
					   , SumarizacaoTrilhas.GrupoFilho
					   , SumarizacaoTrilhas.[Login]
					   , SumarizacaoTrilhas.Status
					   , SumarizacaoTrilhas.Usuario
					   , SumarizacaoTrilhas.Email
					   , SumarizacaoTrilhas.DataCadastroUsuario
					   , SumarizacaoTrilhas.DtUltimoAcessoAmbiente
		), -- select * from Resultado end
		 ResultadoDetalhado AS  
		   (SELECT 
			     Resultado.UsuarioId AS UsuarioID
			   , Usuario AS NomeUsuario
			   , ISNULL(Email, '' )AS EmailUsuario
			   , [Login] AS LoginUsuario
			   , Status AS StatusUsuario
			   , GrupoPaiId AS GrupoPaiID
			   , GrupoPai AS NomeGrupoPai
			   , GrupoFilhoId AS GrupoFilhoID
			   , GrupoFilho AS NomeGrupoFilho
			   , UsuariosAdmin.TodosGruposUsuario
			   , CompeticaoId AS AmbienteID
			   , CompeticaoNome AS NomeAmbiente
			   , TotalTrilhas AS TotalTrilhasVinculadas 
			   , TotalTrilhasIniciadas 
			   , TotalTrilhasConcluidas
			   , TotalTrilhasAprovadas
			   , TotalTrilhasNaoLiberadas
			   , CASE WHEN ValoresComputados.CompeticaoIniciada = 'Sim' THEN DataInicioCompeticao END AS DataInicioAmbiente
			   , CASE WHEN ValoresComputados.CompeticaoConcluida = 'Sim' THEN CONVERT(VARCHAR(10), DataTerminoCompeticao, 103) + ' ' + CONVERT(VARCHAR(8), DataTerminoCompeticao, 108) END AS DataConclusaoAmbiente	
			   , PontuacaoNaCompeticao AS PontuacaoNoAmbiente
			   , TempoAcessoNoAmbienteEmHoras
			   , CASE WHEN DtUltimoAcessoAmbiente IS NOT NULL THEN DtUltimoAcessoAmbiente
					  ELSE DataUltimaAtualizacaoTentativa 
					  END AS DataUltimoAcessoAmbiente
			   , ValoresComputados.CompeticaoIniciada AS AmbienteIniciado
			   , ISNULL(Rodadas.Dispensado, 0) AS Dispensado
			   , Rodadas.DataInicioRealizacaoDispensa
			   , Rodadas.DT_INICIO AS DataInicioRodada
			   , Rodadas.DataTerminoRealizacaoDispensa
			   , Rodadas.DT_TERMINO AS DataTerminoRodada
			   , Rodadas.ID_RODADA AS RodadaId
			FROM Resultado
			JOIN #UsuariosAdmin AS UsuariosAdmin
		 	  ON UsuariosAdmin.UsuarioId = Resultado.UsuarioID
	 OUTER APPLY (SELECT IIF (TotalTrilhasIniciadas > 0, 'Sim', 'Não') AS CompeticaoIniciada
					   , IIF (TotalTrilhasConcluidas >= TotalTrilhas, 'Sim', 'Não') AS CompeticaoConcluida) AS ValoresComputados
	 OUTER APPLY (SELECT CT.ID_COMPETICAO
					   , R.ID AS ID_RODADA
					   , R.DT_INICIO
					   , R.DT_TERMINO
	                   , DispensaRodadas.Dispensado
	                   , DispensaRodadas.DataInicioRealizacaoDispensa 
	                   , DispensaRodadas.DataTerminoRealizacaoDispensa
	                FROM COMPETICAO_TRILHA AS CT (NOLOCK)
					JOIN TRILHA AS T (NOLOCK)
					  ON T.ID = CT.ID_TRILHA
					 AND T.ID_CLIENTE = CT.ID_CLIENTE
					 AND T.FL_STATUS = 1
					JOIN RODADA AS R (NOLOCK)
					  ON CT.ID_TRILHA = R.ID_TRILHA
					 AND CT.ID_CLIENTE = R.ID_CLIENTE
					 AND R.FL_STATUS = 1
			 OUTER APPLY (SELECT DR.DT_INICIO_PARA_REALIZACAO AS DataInicioRealizacaoDispensa
			                   , DR.DT_TERMINO_PARA_REALIZACAO AS DataTerminoRealizacaoDispensa
			                   , CASE WHEN DR.ID_RODADA IS NOT NULL AND DR.DT_INICIO_PARA_REALIZACAO IS NULL AND DR.DT_TERMINO_PARA_REALIZACAO IS NULL THEN 1 ELSE 0 END AS Dispensado
			                FROM DISPENSA_RODADA AS DR (NOLOCK)
					 	   WHERE DR.ID_RODADA = R.ID
						     AND DR.ID_CLIENTE = R.ID_CLIENTE
							 AND DR.ID_USUARIO = Resultado.UsuarioID) AS DispensaRodadas
				   WHERE CT.ID_CLIENTE = @clienteID
				     AND CT.ID_COMPETICAO = Resultado.CompeticaoId
				  ) AS Rodadas
		   WHERE ((@competitionCompletionDateStartDate IS NULL AND @competitionCompletionDateEndDate IS NULL)
		          OR (Resultado.DataTerminoCompeticao BETWEEN @competitionCompletionDateStartDate AND @competitionCompletionDateEndDate AND ValoresComputados.CompeticaoConcluida = 'Sim')
				  OR (@showEmptyDates = 1 AND ValoresComputados.CompeticaoConcluida = 'Não'))
				AND (ISNULL(@grupo, '') = '' OR (GrupoFilho IN (UsuariosAdmin.TodosGruposUsuario) OR 
												 GrupoPai IN ((UsuariosAdmin.TodosGruposUsuario))))
		),
		
	Consolidado AS (
		SELECT UsuarioID
			 , NomeUsuario
			 , EmailUsuario
			 , LoginUsuario
			 , StatusUsuario
			 , GrupoPaiID
			 , NomeGrupoPai
			 , GrupoFilhoID
			 , NomeGrupoFilho
			 , TodosGruposUsuario
			 , AmbienteID
			 , NomeAmbiente
			 , DataInicioAmbiente AS DataInicioAmbiente
			 , DataConclusaoAmbiente AS DataConclusaoAmbiente
			 , PontuacaoNoAmbiente
			 , TempoAcessoNoAmbienteEmHoras
			 , TotalTrilhasVinculadas AS TotalTrilhasVinculadas
			 , TotalTrilhasIniciadas AS TotalTrilhasIniciadas
			 , TotalTrilhasConcluidas AS TotalTrilhasConcluidas
			 , TotalTrilhasAprovadas AS TotalTrilhasAprovadas
			 , TotalTrilhasNaoLiberadas AS TotalTrilhasNaoLiberadas
			 , DataUltimoAcessoAmbiente AS DataUltimoAcessoAmbiente
			 , AmbienteIniciado AS AmbienteIniciado
			 , MIN(Dispensado) AS Dispensado
			 , MIN(CASE WHEN DataInicioRealizacaoDispensa IS NOT NULL THEN DataInicioRealizacaoDispensa ELSE DataInicioRodada END) AS DataInicioRodada
			 , MAX(CASE WHEN DataTerminoRealizacaoDispensa IS NOT NULL THEN DataTerminoRealizacaoDispensa ELSE DataTerminoRodada END) AS DataTerminoRodada
			 --, PerfilNaTrilha
			 --, DataVinculoAmbiente
			 --, OrigemVinculoAmbiente
			 --, VinculadoPor
		  FROM ResultadoDetalhado
		 WHERE ((@lastCompetitionAccessDateStartDate IS NULL AND @lastCompetitionAccessDateEndDate IS NULL)
			     OR (DataUltimoAcessoAmbiente BETWEEN @lastCompetitionAccessDateStartDate AND @lastCompetitionAccessDateEndDate)
				 OR (@showEmptyDates = 1 AND DataUltimoAcessoAmbiente IS NULL))
		   AND ((@competitionStartDateStartDate IS NULL AND @competitionStartDateEndDate IS NULL) 
					OR DataInicioAmbiente BETWEEN @competitionStartDateStartDate AND @competitionStartDateEndDate
					OR (@showEmptyDates = 1 AND DataInicioAmbiente IS NULL))
		GROUP BY UsuarioID, NomeUsuario, EmailUsuario, LoginUsuario, StatusUsuario, GrupoPaiID, 
		         NomeGrupoPai, GrupoFilhoID, NomeGrupoFilho, TodosGruposUsuario, AmbienteID, NomeAmbiente,
				 TotalTrilhasVinculadas, TotalTrilhasIniciadas, TotalTrilhasConcluidas, TotalTrilhasAprovadas, TotalTrilhasNaoLiberadas, PontuacaoNoAmbiente,
			     TempoAcessoNoAmbienteEmHoras, AmbienteIniciado, DataUltimoAcessoAmbiente, DataInicioAmbiente, DataConclusaoAmbiente

    )		
	    SELECT UsuarioID
			 , NomeUsuario
			 , EmailUsuario
			 , LoginUsuario
			 , StatusUsuario
			 , GrupoPaiID
			 , NomeGrupoPai
			 , GrupoFilhoID
			 , NomeGrupoFilho
			 , TodosGruposUsuario
			 , AmbienteID
			 , NomeAmbiente
			 , DataInicioAmbiente
			 , DataConclusaoAmbiente
			 , PontuacaoNoAmbiente
			 , REPLACE(CONVERT(VARCHAR(50), CONVERT(DECIMAL(18, 2), (CONVERT(DECIMAL(18, 2), TotalModulosConcluidos) / CONVERT(DECIMAL(18, 2), NULLIF(TotalModulosDisponiveis, 0))) * 100)), '.', ',') + '%' AS PercentualConclusaoAmbiente
			 , REPLACE(CONVERT(VARCHAR(50), CONVERT(DECIMAL(18, 2), (CONVERT(DECIMAL(18, 2), TotalModulosAprovados) / CONVERT(DECIMAL(18, 2), NULLIF(TotalModulosDisponiveis, 0))) * 100)), '.', ',') + '%' AS PercentualAprovacaoAmbiente
			 , TempoAcessoNoAmbienteEmHoras
			 , TotalTrilhasVinculadas
			 , TotalTrilhasIniciadas
			 , TotalTrilhasConcluidas
			 , TotalTrilhasAprovadas
			 , TotalTrilhasNaoLiberadas
			 , DataUltimoAcessoAmbiente
			 , AmbienteIniciado
			 , CASE WHEN DataConclusaoAmbiente IS NOT NULL THEN IIF (TotalModulosAprovados >= TotalModulosDisponiveis, 'Sim', 'Não') 
					  WHEN DataInicioAmbiente IS NOT NULL THEN ''
					END AS AprovadoNoAmbiente
			 , Dispensado
			 , DataInicioRodada
			 , DataTerminoRodada
			 , CASE WHEN TotalModulosDispensados >= TotalModulos OR Dispensado = 1 THEN 'Dispensado'
			        WHEN (TotalModulosAprovados + TotalModulosReprovados) >= TotalModulosDisponiveis THEN 'Concluído'
					WHEN (TotalModulosExpirados + TotalModulosForaPrazo) > 0 
					     AND (TotalModulosEmAndamento 
						      + TotalModulosNaoIniciados 
							  + TotalModulosAguardandoCorrecao) = 0 THEN 'Expirado (Não concluído)'
					WHEN TotalModulosNaoLiberados >= 0 AND TotalModulosNaoLiberados >= TotalModulosDisponiveis
						 OR (TotalTrilhasNaoLiberadas  >= TotalTrilhasVinculadas ) THEN 'Não liberado'
					WHEN TotalModulosNaoIniciados > 0 
					     AND (TotalModulosAprovados 
						      + TotalModulosReprovados 
							  + TotalModulosAguardandoCorrecao
							  + TotalModulosEmAndamento
							  + TotalModulosExpirados
							  + TotalModulosForaPrazo) = 0 THEN 'Não iniciado'
					WHEN (TotalModulosEmAndamento + TotalModulosAguardandoCorrecao + TotalModulosNaoIniciados) > 0 THEN 'Em andamento'
					ELSE 'Não iniciado'
			    END StatusConclusaoAmbiente
		, TotalModulosAprovados
		, TotalModulosReprovados
		, TotalModulosEmAndamento
		, TotalModulosNaoIniciados
		, TotalModulosNaoLiberados
		, TotalModulosConcluidos
		, TotalModulosExpirados
		, TotalModulosForaPrazo
		, TotalModulosDispensados
		, TotalModulosAguardandoCorrecao
		, TotalModulosDisponiveis
		, TotalModulos 
		  INTO #ResultadoConsolidado
		  FROM Consolidado
OUTER APPLY (
    SELECT
          SUM(UPT.NU_TOTAL_RODADAS_APROVADAS)                          AS TotalModulosAprovados
        , SUM(UPT.NU_TOTAL_RODADAS_REPROVADAS)                         AS TotalModulosReprovados
        , SUM(UPT.NU_TOTAL_RODADAS_EM_ANDAMENTO)                       AS TotalModulosEmAndamento
        , SUM(UPT.NU_TOTAL_RODADAS_NAO_INICIADAS)                      AS TotalModulosNaoIniciados
        , SUM(UPT.NU_TOTAL_RODADAS_NAO_LIBERADAS)                      AS TotalModulosNaoLiberados
        , SUM(UPT.NU_TOTAL_RODADAS_CONCLUIDAS)                         AS TotalModulosConcluidos
        , SUM(UPT.NU_TOTAL_RODADAS_EXPIRADAS)                          AS TotalModulosExpirados
        , SUM(UPT.NU_TOTAL_RODADAS_REALIZADAS_FORA_DO_PRAZO)           AS TotalModulosForaPrazo
        , SUM(UPT.NU_TOTAL_RODADAS_DISPENSADAS)                        AS TotalModulosDispensados
        , SUM(UPT.NU_TOTAL_RODADAS_AGUARDANDO_CORRECAO)                AS TotalModulosAguardandoCorrecao
        , SUM(UPT.NU_TOTAL_RODADAS - UPT.NU_TOTAL_RODADAS_DISPENSADAS) AS TotalModulosDisponiveis
        , SUM(UPT.NU_TOTAL_RODADAS)                                    AS TotalModulos
    FROM USUARIO_PROGRESSO_TRILHA UPT WITH (NOLOCK)
    WHERE UPT.ID_USUARIO    = UsuarioID
      AND UPT.ID_COMPETICAO = AmbienteID
      AND UPT.ID_CLIENTE    = @clienteID
      -- trilha ativa/visível
      AND EXISTS (
            SELECT 1
            FROM TRILHA T WITH (NOLOCK)
            WHERE T.ID = UPT.ID_TRILHA
              AND T.ID_CLIENTE = UPT.ID_CLIENTE
              AND T.FL_STATUS = 1
              AND T.FL_VISIVEL = 1
      )
      -- trilha realmente vinculada ao usuário/competição em #UsuarioTrilha
      AND EXISTS (
            SELECT 1
            FROM #UsuarioTrilha UT
            WHERE UT.TrilhaId     = UPT.ID_TRILHA
              AND UT.ClienteId    = UPT.ID_CLIENTE
              AND UT.UsuarioId    = UPT.ID_USUARIO
              AND UT.CompeticaoId = AmbienteID
      )
      -- existe ao menos uma rodada ativa/visível nessa trilha
      AND EXISTS (
            SELECT 1
            FROM RODADA R WITH (NOLOCK)
            WHERE R.ID_TRILHA  = UPT.ID_TRILHA
              AND R.ID_CLIENTE = UPT.ID_CLIENTE
              AND R.FL_STATUS  = 1
              AND R.FL_VISIVEL = 1
      )
) AS UsuarioProgressoTrilha

	    SELECT UsuarioID									    
			 , NomeUsuario
			 , EmailUsuario
			 , LoginUsuario
			 , StatusUsuario
			 , GrupoPaiID
			 , NomeGrupoPai
			 , GrupoFilhoID
			 , NomeGrupoFilho
			 , TodosGruposUsuario
			 , AmbienteID
			 , NomeAmbiente
			 , IIF(Dispensado = 1, '', CASE WHEN StatusConclusaoAmbiente IN ('Não liberado', 'Não iniciado') THEN ''
											ELSE CONVERT(VARCHAR(10), DataInicioAmbiente, 103) + ' ' + CONVERT(VARCHAR(8), DataInicioAmbiente, 108) END) AS DataInicioAmbiente
			 , IIF(Dispensado = 1, '', DataConclusaoAmbiente) AS DataConclusaoAmbiente
			 , IIF(Dispensado = 1, '', CASE WHEN StatusConclusaoAmbiente IN ('Não liberado', 'Não iniciado') THEN '0.00'
			        ELSE CAST(PontuacaoNoAmbiente AS NVARCHAR(50)) END) AS PontuacaoNoAmbiente
			 , IIF(Dispensado = 1, '', PercentualConclusaoAmbiente) AS PercentualConclusaoAmbiente
			 , IIF(Dispensado = 1, '', PercentualAprovacaoAmbiente) AS PercentualAprovacaoAmbiente
			 , IIF(Dispensado = 1, '',  CASE WHEN StatusConclusaoAmbiente IN ('Não liberado', 'Não iniciado') THEN '0:00:00' 
										     ELSE TempoAcessoNoAmbienteEmHoras END) AS TempoAcessoNoAmbienteEmHoras
			 , IIF(Dispensado = 1, '', TotalTrilhasVinculadas) AS TotalTrilhasVinculadas
			 , IIF(Dispensado = 1, '', TotalTrilhasIniciadas) AS TotalTrilhasIniciadas
			 , IIF(Dispensado = 1, '', TotalTrilhasConcluidas) AS TotalTrilhasConcluidas
			 , IIF(Dispensado = 1, '', TotalTrilhasAprovadas) AS TotalTrilhasAprovadas
			 , FORMAT(CONVERT(DATETIME, DataUltimoAcessoAmbiente, 120),'dd/MM/yyyy') AS DataUltimoAcessoAmbiente
			 , IIF(Dispensado = 1, '', CASE WHEN StatusConclusaoAmbiente IN ('Não liberado', 'Não iniciado') THEN 'Não'
											ELSE AmbienteIniciado END) AS AmbienteIniciado
			 , IIF(Dispensado = 1, '', AprovadoNoAmbiente) AS AprovadoNoAmbiente
			 , StatusConclusaoAmbiente 
			 , CASE ID_PERFIL_JOGO WHEN 1 THEN 'Obrigatório'
			                       WHEN 2 THEN 'Participa'
								   WHEN 3 THEN 'Gestor'
								   ELSE 'Não participa' END AS PerfilNaTrilha
			 , FORMAT(CONVERT(DATETIME, DT_VINCULO_AMBIENTE, 120),'dd/MM/yyyy HH:mm:ss') AS DataVinculoAmbiente
			 , CASE ID_ORIGEM_CADASTRO WHEN 1 THEN 'Manual'
			                           WHEN 2 THEN 'Público Alvo'
									   WHEN 3 THEN
									   'API'
									   WHEN 4 THEN 'Importação de Dados' 
                END AS OrigemVinculoAmbiente
			 , CASE WHEN ISNULL(ID_ORIGEM_CADASTRO, ID_ORIGEM_ULTIMA_ATUALIZACAO) = 1 THEN ISNULL(UsuarioVinculo.TX_NOME_COMPLETO, ISNULL(UsuarioPerfil.CRIADO_POR, UsuarioPerfil.ATUALIZADO_POR)) -- Nome do usuário
				    WHEN ISNULL(ID_ORIGEM_CADASTRO, ID_ORIGEM_ULTIMA_ATUALIZACAO) = 2 THEN ISNULL(PublicoAlvo.TX_NOME, ISNULL(UsuarioPerfil.CRIADO_POR, UsuarioPerfil.ATUALIZADO_POR)) -- Nome do público-alvo
				    WHEN ISNULL(ID_ORIGEM_CADASTRO, ID_ORIGEM_ULTIMA_ATUALIZACAO) = 3 THEN ISNULL(UsuarioPerfil.CRIADO_POR, UsuarioPerfil.ATUALIZADO_POR) -- API
				    WHEN ISNULL(ID_ORIGEM_CADASTRO, ID_ORIGEM_ULTIMA_ATUALIZACAO) = 4 THEN ISNULL(UsuarioAdminImportacao.TX_NOME_COMPLETO, ISNULL(UsuarioPerfil.CRIADO_POR, UsuarioPerfil.ATUALIZADO_POR)) -- ID da importação
			    END AS VinculadoPor
		  INTO #Resultado
	      FROM #ResultadoConsolidado AS Consolidado
   CROSS APPLY (SELECT TOP 1 ID_PERFIL_JOGO
					 , DT_CADASTRO AS DT_VINCULO_AMBIENTE
					 , ID_ORIGEM_CADASTRO
					 , ID_ORIGEM_ULTIMA_ATUALIZACAO
					 , ATUALIZADO_POR
					 , CRIADO_POR
                  FROM(SELECT UPE.ID_PERFIL_JOGO
							, UPE.DT_CADASTRO
							, UPE.ID_ORIGEM_CADASTRO
							, UPE.ID_ORIGEM_ULTIMA_ATUALIZACAO
							, UPE.CRIADO_POR
							, UPE.ATUALIZADO_POR
							, ROW_NUMBER() OVER (PARTITION BY UPE.ID_USUARIO ORDER BY CASE 
							                                                            WHEN UPE.ID_PERFIL_JOGO = 1 THEN 1
																						WHEN UPE.ID_PERFIL_JOGO = 2 THEN 2
																						WHEN UPE.ID_PERFIL_JOGO = 3 THEN 3
																						ELSE 4
																					END ASC,
																					UPE.DT_CADASTRO ASC) AS RowNum
						 FROM COMPETICAO_TRILHA AS CT (NOLOCK)
						 JOIN RODADA AS RD (NOLOCK)
						   ON CT.ID_TRILHA = RD.ID_TRILHA
						  AND CT.ID_CLIENTE = RD.ID_CLIENTE
						 JOIN USUARIO_PERFIL_ENTIDADE AS UPE (NOLOCK)
						   ON UPE.ID_ENTIDADE = RD.ID
						  AND UPE.ID_CLIENTE = RD.ID_CLIENTE
						  AND UPE.ID_ENTIDADE_TIPO = 'RD'
						  AND UPE.ID_PERFIL_JOGO IN (1, 2, 3)
						WHERE Consolidado.UsuarioID = UPE.ID_USUARIO
						  AND UPE.ID_CLIENTE = @clienteID
						  AND (ISNULL(@perfilUsuarioTrilhaIds, '') = '' OR UPE.ID_PERFIL_JOGO in (SELECT OriginalValue
			                                                                                                    FROM fnt_ConverterFiltrosParaTabela(@perfilUsuarioTrilhaIds, 'perfilUsuarioTrilhaIds')))
						  AND CT.ID_COMPETICAO = Consolidado.AmbienteID
						  AND ((@competitionLinkDateStartDate IS NULL AND @competitionLinkDateEndDate IS NULL) 
									       OR UPE.DT_CADASTRO BETWEEN @competitionLinkDateStartDate AND @competitionLinkDateEndDate)) AS UsuarioPerfilRanked
				 WHERE UsuarioPerfilRanked.RowNum = 1) AS UsuarioPerfil
	 LEFT JOIN USUARIO AS UsuarioVinculo (NOLOCK)
			ON ISNULL(UsuarioPerfil.CRIADO_POR, UsuarioPerfil.ATUALIZADO_POR) = UsuarioVinculo.ID
		   AND UsuarioVinculo.ID_CLIENTE = @clienteID
		   AND ISNULL(UsuarioPerfil.ID_ORIGEM_CADASTRO, UsuarioPerfil.ID_ORIGEM_ULTIMA_ATUALIZACAO) = 1 -- Apenas para origem manual
	 LEFT JOIN PUBLICO_ALVO AS PublicoAlvo (NOLOCK)
			ON ISNULL(UsuarioPerfil.CRIADO_POR, UsuarioPerfil.ATUALIZADO_POR) = PublicoAlvo.ID
		   AND PublicoAlvo.ID_CLIENTE = @clienteID
		   AND ISNULL(UsuarioPerfil.ID_ORIGEM_CADASTRO, UsuarioPerfil.ID_ORIGEM_ULTIMA_ATUALIZACAO) = 2 -- Apenas para público-alvo
	 LEFT JOIN IMPORTACAO AS Importacao (NOLOCK)
		    ON ISNULL(UsuarioPerfil.CRIADO_POR, UsuarioPerfil.ATUALIZADO_POR) = Importacao.ID
		   AND Importacao.ID_CLIENTE = @clienteID
		   AND ISNULL(UsuarioPerfil.ID_ORIGEM_CADASTRO, UsuarioPerfil.ID_ORIGEM_ULTIMA_ATUALIZACAO) = 4 -- Apenas para importação de dados
	 LEFT JOIN USUARIO AS UsuarioAdminImportacao (NOLOCK)
		    ON Importacao.ID_ADMIN = UsuarioAdminImportacao.ID -- Buscar o nome do admin que realizou a importação
		   AND UsuarioAdminImportacao.ID_CLIENTE = @clienteID
		 WHERE (ISNULL(@statusConclusaoCompeticao, '') = '' OR StatusConclusaoAmbiente IN (SELECT ConvertedValue 
		                                                                                     FROM fnt_ConverterFiltrosParaTabela(@statusConclusaoCompeticao, 'statusConclusaoCompeticao')))

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
			   AND Atributos.ClienteId = UsuariosAdmin.ClienteID
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
			SET @sql += '	   ON (SELECT 1) = (SELECT 0) '
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
END

/*		

DECLARE
	@adminId		INT = (select id from usuario where id_cliente = 'leosilva'  AND TX_LOGIN = 'leonardo.silva@engage.bz')
	, @clienteID	NVARCHAR(24) = 'leosilva'
	, @competicaoId	NVARCHAR(255) =  '22128'
	, @userStatus	NVARCHAR(20) = NULL
	, @perfilUsuarioTrilhaIds NVARCHAR (20) = NULL
	, @usuario	    NVARCHAR(255) = '689972'
	, @statusConclusaoCompeticao NVARCHAR(20) = NULL
	, @grupo NVARCHAR(255) = NULL
    , @competitionCompletionDateStartDate DATETIME = NULL
	, @competitionCompletionDateEndDate DATETIME = NULL
	, @competitionCompletionDateShowEmptyDates BIT = 1 
	, @competitionStartDateStartDate DATETIME = NULL 
	, @competitionStartDateEndDate DATETIME =  NULL 
	, @competitionStartDateShowEmptyDates BIT =  1
	, @lastCompetitionAccessDateStartDate DATETIME = NULL
	, @lastCompetitionAccessDateEndDate DATETIME = NULL
	, @lastCompetitionAccessDateShowEmptyDates BIT = 1
	, @competitionLinkDateStartDate DATETIME = NULL
	, @competitionLinkDateEndDate DATETIME = NULL
	, @filterAttributes PublicoAlvoCriterio 
*/
GO

