USE [prod_my_engage_autosservico]
GO

/****** Object:  StoredProcedure [Report].[engagesp_RelatorioStatusModulosLinhas]    Script Date: 10/27/2025 7:20:21 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Report].[engagesp_RelatorioStatusModulosLinhas] 
		@adminId		INT
	  , @clienteID		NVARCHAR(24)
	  , @usuario	    NVARCHAR(MAX) = NULL
	  , @userStatus		NVARCHAR(20) = NULL
	  , @grupo			NVARCHAR(MAX) = NULL
	  , @competicaoId	NVARCHAR(MAX) = NULL
	  , @trilhaId		NVARCHAR(MAX) = NULL
      , @rodadaId		NVARCHAR(MAX) = NULL
	  , @startDate		DATETIME = NULL
	  , @endDate		DATETIME = NULL
	  , @showEmptyDates BIT = 0
	  , @perfilUsuarioTrilhaIds NVARCHAR (20) = '1, 2, 3'
	  , @roundStartDateStartDate DATETIME = NULL
      , @roundStartDateEndDate DATETIME = NULL
	  , @roundCompletionDateStartDate DATETIME = NULL
	  , @roundCompletionDateEndDate DATETIME = NULL
	  , @statusConclusaoRodada NVARCHAR(MAX) = NULL
	  , @roundLinkDateStartDate DATETIME = NULL
      , @roundLinkDateEndDate DATETIME = NULL
	  , @enableReport	BIT = 1
	  , @exibirTodasTentativas BIT = 0
	  , @showInactiveCompetition BIT = 0
	  , @showInactiveTrack BIT = 0
	  , @showInactiveRound BIT = 0
	  , @filterAttributes PublicoAlvoCriterio READONLY
AS

BEGIN

	  IF @enableReport = 0
	  BEGIN
		   RETURN;
	  END

	  DECLARE @grupos SubGrupo;
	  DECLARE @perfilUsuarioTrilha PerfilJogo

	  IF OBJECT_ID (N'tempdb..#CompeticoesAdmin', N'U') IS NOT NULL DROP TABLE #CompeticoesAdmin
	  IF OBJECT_ID (N'tempdb..#ResultadosModulos', N'U') IS NOT NULL DROP TABLE #ResultadosModulos
	  IF OBJECT_ID (N'tempdb..#TempAtributosUsuario', N'U') IS NOT NULL DROP TABLE #TempAtributosUsuario
	  IF OBJECT_ID (N'tempdb..#CargaHorarias', N'U') IS NOT NULL DROP TABLE #CargaHorarias
	  IF OBJECT_ID (N'tempdb..#UsuariosAdmin', N'U')	IS NOT NULL DROP TABLE #UsuariosAdmin
	  IF OBJECT_ID('tempdb..#UsuariosAtributo', 'U') IS NOT NULL DROP TABLE #UsuariosAtributo;

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
			INSERT INTO #UsuariosAtributo
            EXEC engagesp_GetUsuarioByTargetAudience @skip=0, @take=99999, @search=NULL, @targetAudienceCriteriasAdd=@filterAttributes, @targetAudienceCriteriasException=@p5
        END
	  

       SELECT EntidadeId AS CompeticaoId
		    , ClienteID
		 INTO #CompeticoesAdmin
	     FROM fnt_EntidadesEditaveis (@adminId, @clienteID, 'CP', @grupos) AS EntidadesEditaveis		
CREATE UNIQUE CLUSTERED INDEX IX_CompeticaoAdmin ON #CompeticoesAdmin (CompeticaoId, ClienteId)
 
		SELECT EntidadeId AS UsuarioId
			 , ClienteID 
			 , Grupos.TodosGruposUsuario
			 , USUARIO.TX_NOME_COMPLETO AS NomeUsuario
			 , USUARIO.TX_LOGIN AS LoginUsuario
			 , Usuario.TX_EMAIL AS EmailUsuario
			 , Usuario.FL_STATUS AS StatusUsuario
		  INTO #UsuariosAdmin
		  FROM fnt_EntidadesEditaveis(@adminId, @clienteID, 'US', @grupos) AS EntidadesEditaveis
		  JOIN USUARIO (NOLOCK)
		    ON USUARIO.ID_CLIENTE = EntidadesEditaveis.ClienteID
		   AND USUARIO.ID = EntidadesEditaveis.EntidadeId
	 LEFT JOIN (SELECT OriginalValue AS UsuarioID
	              FROM dbo.fnt_ConverterFiltrosParaTabela(@usuario, 'usuario')) AS RelatorioFiltroUsuario
			ON RelatorioFiltroUsuario.UsuarioID = USUARIO.ID
   OUTER APPLY fnt_TodosGruposUsuarios (EntidadesEditaveis.EntidadeId, EntidadesEditaveis.ClienteID, @grupo) AS Grupos
	     WHERE USUARIO.ID_CLIENTE = @clienteID
		   AND USUARIO.FL_PADRAO = 0
		   AND (USUARIO.FL_STATUS IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@userStatus, 'userStatus')) OR ISNULL(@userStatus, '') = '')
		   AND USUARIO.DT_EXCLUSAO IS NULL
		   AND (ISNULL(@usuario, '') = '' OR RelatorioFiltroUsuario.UsuarioID IS NOT NULL)
		   AND (NOT EXISTS (SELECT TOP 1 1 FROM @filterAttributes)
				OR EXISTS (SELECT TOP 1 1 
							FROM #UsuariosAtributo AS UsuariosAtributo
						   WHERE Usuario.ID = UsuariosAtributo.UserId
							 AND USUARIO.ID_CLIENTE = UsuariosAtributo.CustomerId COLLATE Latin1_General_CI_AI 
							 AND UsuariosAtributo.UserId IS NOT NULL)
				)

	CREATE UNIQUE CLUSTERED INDEX IX_UsuarioAdmin ON #UsuariosAdmin (UsuarioId, ClienteId)


	;WITH CTE
	AS
	(
		SELECT Rodada.ID AS ModuloId
			 , Rodada.ID_CLIENTE AS ClienteID
			 , CASE WHEN ISNULL(CargaHoraraAtividades.CargaHoraria, 0) > 0 THEN CargaHoraraAtividades.CargaHoraria
					WHEN Rodada.NU_CARGA_HORARIA > 0 THEN Rodada.NU_CARGA_HORARIA
				END AS CargaHoraria
			 , COMPETICAO.ID AS AmbienteId
			 , COMPETICAO.TX_NOME AS NomeAmbiente
			 , TRILHA.ID AS TrilhaId
			 , TRILHA.TX_DESCRICAO AS NomeTrilha
			 , SUBSTRING(RODADA.TX_NOME, 0, 128) AS NomeModulo
			 , RODADA.DT_INICIO AS DataInicioModulo
			 , RODADA.DT_TERMINO AS DataTerminoModulo
			 , RODADA.DT_CADASTRO AS DataCadastroModulo
			 , RODADA.DT_ULTIMA_ATUALIZACAO AS DataUltimaAtualizacaoModulo
			 , AtualizadoPor.TX_NOME_COMPLETO AS AtualizadoPor
			 , RODADA.ID_TIPO AS ModuloTipoId
			 , CASE 
					WHEN EXISTS (
						SELECT 1
						FROM fnt_ConfiguracoesCompeticao(@clienteID, COMPETICAO.ID) AS Config
						WHERE Config.ConfiguracaoID = N'count_partial_score_in_rankings'
						  AND Config.[Status] = 1
					) THEN 'Y'
					ELSE 'N'
				END AS ContabilizarPontuacaoParcial
		  FROM COMPETICAO_TRILHA CT (NOLOCK)
		  JOIN COMPETICAO  (NOLOCK)
		    ON COMPETICAO.ID = CT.ID_COMPETICAO
		   AND COMPETICAO.ID_CLIENTE = CT.ID_CLIENTE
		   AND (ISNULL(@showInactiveCompetition, 0) = 1 OR COMPETICAO.FL_DISPONIVEL = 1)
		  JOIN #CompeticoesAdmin AS CompeticoesAdmin
			ON CompeticoesAdmin.CompeticaoId = COMPETICAO.ID
		   AND CompeticoesAdmin.ClienteID = COMPETICAO.ID_CLIENTE
		  JOIN TRILHA (NOLOCK)
		    ON TRILHA.ID = CT.ID_TRILHA
		   AND TRILHA.ID_CLIENTE = CT.ID_CLIENTE
		   AND (ISNULL(@showInactiveTrack, 0) = 1 OR TRILHA.FL_STATUS = 1)
		  JOIN RODADA (NOLOCK)
		    ON RODADA.ID_CLIENTE = TRILHA.ID_CLIENTE
		   AND RODADA.ID_TRILHA = TRILHA.ID
		   AND (ISNULL(@showInactiveRound, 0) = 1 OR RODADA.FL_STATUS = 1)
		   AND RODADA.FL_VISIVEL = 1
	 LEFT JOIN (SELECT OriginalValue AS CompeticaoId 
	              FROM fnt_ConverterFiltrosParaTabela(@competicaoId, 'competicaoId')) AS RelatorioFiltroCompeticao
			ON RelatorioFiltroCompeticao.CompeticaoId = COMPETICAO.ID
	 LEFT JOIN (SELECT OriginalValue AS TrilhaId 
	              FROM fnt_ConverterFiltrosParaTabela(@trilhaId, 'trilhaId')) AS RelatorioFiltroTrilha
			ON RelatorioFiltroTrilha.TrilhaId= TRILHA.ID
	 LEFT JOIN (SELECT OriginalValue AS RodadaId 
	              FROM fnt_ConverterFiltrosParaTabela(@rodadaId, 'rodadaId')) AS RelatorioFiltroRodada
			ON RelatorioFiltroRodada.RodadaId = RODADA.ID
   CROSS APPLY (SELECT SUM(ATIVIDADE.NU_CARGA_HORARIA) AS CargaHoraria
				  FROM RODADA_ATIVIDADE RA (NOLOCK)
				  JOIN ATIVIDADE (NOLOCK)
					ON ATIVIDADE.ID = RA.ID_ATIVIDADE
				   AND ATIVIDADE.ID_CLIENTE = RA.ID_CLIENTE
				   AND ATIVIDADE.FL_STATUS = 1
				 WHERE RA.ID_RODADA = RODADA.ID
				   AND RA.ID_CLIENTE = RODADA.ID_CLIENTE) AS CargaHoraraAtividades
   OUTER APPLY (SELECT TX_NOME_COMPLETO 
                  FROM USUARIO (NOLOCK)
				 WHERE USUARIO.ID = TRY_PARSE(RODADA.ATUALIZADO_POR AS INT)
				   AND USUARIO.ID_CLIENTE = RODADA.ID_CLIENTE) AS AtualizadoPor
		 WHERE (ISNULL(@showInactiveCompetition, 0) = 1 OR COMPETICAO.FL_DISPONIVEL = 1)
		   AND COMPETICAO.ID_CLIENTE = @clienteID
		   AND (RelatorioFiltroCompeticao.CompeticaoId IS NOT NULL OR ISNULL(@competicaoId, '') = '')
		   AND (RelatorioFiltroTrilha.TrilhaId IS NOT NULL OR ISNULL(@trilhaId, '') = '')
		   AND (RelatorioFiltroRodada.RodadaId IS NOT NULL OR ISNULL(@rodadaId, '') = '')
	) SELECT *
	    INTO #CargaHorarias
		FROM CTE
		

		CREATE UNIQUE CLUSTERED INDEX IX_CargaHoraria ON #CargaHorarias (ModuloId, ClienteId)
		CREATE UNIQUE NONCLUSTERED INDEX IX_CargaHoraria_1 ON #CargaHorarias (TrilhaId, ClienteId, ModuloId)				INCLUDE (CargaHoraria, NomeAmbiente, NomeTrilha, NomeModulo, DataInicioModulo, DataTerminoModulo, DataCadastroModulo, DataUltimaAtualizacaoModulo, AtualizadoPor, ModuloTipoId)
		CREATE UNIQUE NONCLUSTERED INDEX IX_CargaHoraria_2 ON #CargaHorarias (AmbienteId, ClienteId, TrilhaId, ModuloId)	INCLUDE (CargaHoraria, NomeAmbiente, NomeTrilha, NomeModulo, DataInicioModulo, DataTerminoModulo, DataCadastroModulo, DataUltimaAtualizacaoModulo, AtualizadoPor, ModuloTipoId)
		DECLARE @dateFormat VARCHAR(24) = 'dd/MM/yyyy HH:mm'	

				;WITH CTE AS
				(
					SELECT UsuariosAdmin.UsuarioID
						 , UsuariosAdmin.ClienteID
						 , CargaHoraria.ModuloId
						 , UsuariosAdmin.NomeUsuario
						 , UsuariosAdmin.LoginUsuario
						 , UsuariosAdmin.EmailUsuario
						 , CargaHoraria.NomeModulo
						 , CargaHoraria.AmbienteId
						 , CargaHoraria.NomeAmbiente
						 , CargaHoraria.TrilhaId
						 , CargaHoraria.NomeTrilha						 
						 , UsuarioGrupo.[Id Grupo Filho]
						 , UsuarioGrupo.[Grupo Filho]
						 , UsuarioGrupo.[Id Grupo Pai]					 
						 , UsuarioGrupo.[Grupo Pai]
						 , CASE WHEN UsuariosAdmin.StatusUsuario = 1											THEN 'Ativo' ELSE 'Inativo' END AS StatusUsuario
						 , CASE WHEN UltimaTentativa.MissaoConcluida = 1 OR ContabilizarPontuacaoParcial = 'Y' THEN Nota END AS Nota
						 , CASE 
								WHEN Valores.StatusID = 'null'													THEN 'Dispensado'
								WHEN UltimaTentativa.MissaoConcluida = 1 OR ContabilizarPontuacaoParcial = 'Y'	THEN CONVERT(VARCHAR, CONVERT(DECIMAL(10,2), Pontuacao))
						   END AS Pontuacao
						
						 , DetalhesModulo.[StatusRodada]
						 , CASE WHEN UltimaTentativa.MissaoConcluida = 1 OR ContabilizarPontuacaoParcial = 'Y' THEN CONVERT(VARCHAR, CONVERT(DECIMAL(10,2), ISNULL(UltimaTentativa.PontuacaoSemBonus, 0))) END AS PontuacaoSemBonus
						 , CASE WHEN UltimaTentativa.MissaoConcluida = 1 OR ContabilizarPontuacaoParcial = 'Y' THEN CONVERT(VARCHAR, CONVERT(DECIMAL(10,2), ISNULL(UltimaTentativa.BonusDesafio, 0))) END AS BonusDesafio
						 , CASE WHEN UltimaTentativa.MissaoConcluida = 1 OR ContabilizarPontuacaoParcial = 'Y' THEN CONVERT(VARCHAR, CONVERT(DECIMAL(10,2), ISNULL(UltimaTentativa.PontuacaoTempoRestante, 0))) END AS BonusTempoRestante
						 , FORMAT(UltimaTentativa.DataConclusao, @dateFormat) AS DataConclusao	
						 , FORMAT(CargaHoraria.DataTerminoModulo, @dateFormat) AS DataTerminoModulo
						 , DataPrimeiroAcessoNaMissao
						 , CASE WHEN UPE.ID_PERFIL_JOGO = 1 THEN 'Obrigatório' 
								WHEN UPE.ID_PERFIL_JOGO = 2 THEN 'Participa'
								WHEN UPE.ID_PERFIL_JOGO = 3 THEN 'Gestor'
								ELSE '' 
						    END AS PerfilNaTrilha
						 , FORMAT(CargaHoraria.DataCadastroModulo, @dateFormat) AS DataCadastroModulo
						 , FORMAT(CargaHoraria.DataUltimaAtualizacaoModulo, @dateFormat) AS DataUltimaAtualizacaoRodada						 
						 , CargaHoraria.CargaHoraria
						 , ISNULL(TempoAcessoNaMissao, 0) AS TempoAcesso
						 , UsuariosAdmin.TodosGruposUsuario
						 , UltimaTentativa.TentativaID
						 , CargaHoraria.AtualizadoPor
						 , UltimaTentativa.NumeroTentativa
						 , FORMAT(UPE.DT_CADASTRO, @dateFormat) AS DataVinculoModulo
						 , CASE ID_ORIGEM_CADASTRO WHEN 1 THEN 'Manual'
                                       WHEN 2 THEN 'Público Alvo'
                                       WHEN 3 THEN 'API'
                                       WHEN 4 THEN 'Importação de Dados' 
						    END AS OrigemVinculoModulo 
						 , CASE WHEN ID_ORIGEM_CADASTRO = 1 THEN ISNULL(UsuarioVinculo.TX_NOME_COMPLETO, UPE.CRIADO_POR) -- Nome do usuário
								WHEN ID_ORIGEM_CADASTRO = 2 THEN ISNULL(PublicoAlvo.TX_NOME, UPE.CRIADO_POR) -- Nome do público-alvo
								WHEN ID_ORIGEM_CADASTRO = 3 THEN UPE.CRIADO_POR -- API
								WHEN ID_ORIGEM_CADASTRO = 4 THEN ISNULL(UsuarioAdminImportacao.TX_NOME_COMPLETO, UPE.CRIADO_POR) -- ID da importação
							END AS VinculadoPor
					    , COUNT(ModuloId) OVER (PARTITION BY TrilhaId, UsuariosAdmin.UsuarioID) AS TotalModulos
					    , SUM(IIF(UltimaTentativa.MissaoConcluida = 1, 1, 0)) OVER (PARTITION BY TrilhaId, UsuariosAdmin.UsuarioID) AS TotalModulosConcluidos
						, MAX(UltimaTentativa.DataConclusao) OVER(PARTITION BY UsuariosAdmin.UsuarioID) AS DataConclusaoTentativa
					  FROM #UsuariosAdmin AS UsuariosAdmin (NOLOCK)
					  JOIN USUARIO_PERFIL_ENTIDADE AS UPE (NOLOCK)
					    ON UPE.ID_USUARIO = UsuariosAdmin.UsuarioId
					   AND UPE.ID_CLIENTE = UsuariosAdmin.ClienteId
			     LEFT JOIN USUARIO AS UsuarioVinculo (NOLOCK)
						ON TRY_CAST(UPE.CRIADO_POR AS INT) = UsuarioVinculo.ID
					   AND UsuarioVinculo.ID_CLIENTE = @clienteID
					   AND UPE.ID_ORIGEM_CADASTRO = 1 -- Apenas para origem manual
				 LEFT JOIN PUBLICO_ALVO AS PublicoAlvo (NOLOCK)
						ON TRY_CAST(UPE.CRIADO_POR AS INT) = PublicoAlvo.ID
					   AND PublicoAlvo.ID_CLIENTE = @clienteID
					   AND UPE.ID_ORIGEM_CADASTRO = 2 -- Apenas para público-alvo
				 LEFT JOIN IMPORTACAO AS Importacao (NOLOCK)
						ON TRY_CAST(UPE.CRIADO_POR AS INT) = Importacao.ID
					   AND Importacao.ID_CLIENTE = @clienteID
					   AND UPE.ID_ORIGEM_CADASTRO = 4 -- Apenas para importação de dados
				 LEFT JOIN USUARIO AS UsuarioAdminImportacao (NOLOCK)
						ON Importacao.ID_ADMIN = UsuarioAdminImportacao.ID -- Buscar o nome do admin que realizou a importação
					   AND UsuarioAdminImportacao.ID_CLIENTE = @clienteID
					  JOIN #CargaHorarias AS CargaHoraria
						ON UPE.ID_ENTIDADE = CargaHoraria.ModuloId
					   AND UPE.ID_ENTIDADE_TIPO = 'RD'
					   AND UPE.ID_CLIENTE = CargaHoraria.ClienteId
			   CROSS APPLY (SELECT TOP 1 GRUPO.TX_NOME AS [Grupo Filho]
			                     , GRUPO.ID AS [Id Grupo Filho]
								 , Pai.TX_NOME AS [Grupo Pai]
								 , Pai.ID AS [Id Grupo Pai]
							  FROM USUARIO_GRUPO AS UsuarioGrupo (NOLOCK)
							  JOIN GRUPO (NOLOCK)
								ON GRUPO.ID = UsuarioGrupo.ID_GRUPO
							   AND Grupo.ID_CLIENTE = UsuarioGrupo.ID_CLIENTE
							   AND Grupo.FL_STATUS = 1
							  JOIN COMPETICAO_GRUPO AS COMPETICAOGRUPO (NOLOCK)
								ON COMPETICAOGRUPO.ID_CLIENTE = CargaHoraria.ClienteID
							   AND COMPETICAOGRUPO.ID_COMPETICAO = CargaHoraria.AmbienteId
							   AND COMPETICAOGRUPO.ID_GRUPO = GRUPO.ID
						 LEFT JOIN GRUPO AS Pai (NOLOCK)
								ON Pai.ID = Grupo.ID_GRUPO_PAI
							   AND Pai.ID_CLIENTE = Grupo.ID_CLIENTE
							   AND Pai.FL_STATUS = 1
							 WHERE UsuarioGrupo.ID_USUARIO = UsuariosAdmin.UsuarioId
							   AND UsuarioGrupo.ID_CLIENTE = UsuariosAdmin.ClienteId
					      ORDER BY CASE WHEN Pai.ID IS NOT NULL THEN 2
										WHEN Grupo.ID IS NOT NULL THEN 3										  
										ELSE 4
									END ASC) AS UsuarioGrupo
			   OUTER APPLY (SELECT TOP (CASE WHEN @exibirTodasTentativas = 0 THEN 1 ELSE 100 END) Tentativa.ID AS TentativaId
								 , Tentativa.ID_CLIENTE AS ClienteId
								 , Tentativa.NU_NOTA AS Nota
								 , Tentativa.ID_STATUS AS StatusId
								 , Tentativa.NU_PONTUACAO AS PontuacaoSemBonus
								 , Tentativa.NU_BONUS_DESAFIO AS BonusDesafio
								 , Tentativa.NU_PONTUACAO_TEMPO_RESTANTE AS PontuacaoTempoRestante
								 , CASE WHEN Tentativa.ID_STATUS IN('nao_iniciado') THEN NULL 
										ELSE Tentativa.DT_CADASTRO END AS DataPrimeiroAcessoNaMissao
								 , ISNULL(Tentativa.NU_PONTUACAO, 0) + ISNULL(Tentativa.NU_BONUS_DESAFIO, 0) + ISNULL(Tentativa.NU_PONTUACAO_TEMPO_RESTANTE, 0) AS Pontuacao
								 , CASE WHEN Tentativa.ID_STATUS IN('concluido', 'aprovado', 'reprovado', 'realizado_fora_prazo') THEN 1 ELSE 0 END AS MissaoConcluida
								 , CASE WHEN Tentativa.ID_STATUS IN('nao_iniciado') THEN NULL 
								        WHEN (Tentativa.ID_STATUS IN('concluido', 'aprovado', 'reprovado', 'realizado_fora_prazo') OR CargaHoraria.ModuloTipoID = 'recorrente') THEN Tentativa.DT_ULTIMA_ATUALIZACAO END AS DataConclusao
								 , ISNULL(NU_TEMPO_ACESSO_EM_SEGUNDOS, 0) AS TempoAcessoNaMissao
								 , ROW_NUMBER() OVER(PARTITION BY Tentativa.ID_USUARIO, Tentativa.ID_RODADA ORDER BY Tentativa.ID ASC) AS NumeroTentativa
							  FROM TENTATIVA AS Tentativa (NOLOCK)
							 WHERE Tentativa.ID_USUARIO = UsuariosAdmin.UsuarioID
							   AND Tentativa.ID_CLIENTE = UsuariosAdmin.ClienteId
							   AND Tentativa.ID_RODADA = CargaHoraria.ModuloId
							   AND Tentativa.ID_STATUS != 'cancelado'
						  ORDER BY Tentativa.ID DESC) AS UltimaTentativa
			     LEFT JOIN DISPENSA_RODADA AS DispensaRodada (NOLOCK)
						ON DispensaRodada.ID_USUARIO = UsuariosAdmin.UsuarioID
					   AND DispensaRodada.ID_CLIENTE = UsuariosAdmin.ClienteId
					   AND DispensaRodada.ID_RODADA = CargaHoraria.ModuloId
				 LEFT JOIN (SELECT OriginalValue AS GrupoId
				              FROM fnt_ConverterFiltrosParaTabela(@grupo, 'grupo')) AS RelatorioFiltroGrupo
					    ON (RelatorioFiltroGrupo.GrupoId = UsuarioGrupo.[Id Grupo Filho] OR 
						    RelatorioFiltroGrupo.GrupoId = UsuarioGrupo.[Id Grupo Pai])
			   OUTER APPLY (SELECT CASE WHEN DispensaRodada.DT_DISPENSA IS NOT NULL AND DispensaRodada.DT_INICIO_PARA_REALIZACAO IS NOT NULL AND GETDATE() < DispensaRodada.DT_INICIO_PARA_REALIZACAO THEN 'not_current'
									    WHEN DispensaRodada.DT_DISPENSA IS NOT NULL AND (DispensaRodada.DT_INICIO_PARA_REALIZACAO IS NULL OR DispensaRodada.DT_INICIO_PARA_REALIZACAO > GETDATE()) THEN 'null'						 			    
						 			    WHEN GETDATE() BETWEEN CargaHoraria.DataInicioModulo AND CargaHoraria.DataTerminoModulo THEN  'current' -- Verifica apenas a data
						 			    WHEN GETDATE() < COALESCE(DispensaRodada.DT_INICIO_PARA_REALIZACAO, CargaHoraria.DataInicioModulo) THEN 'not_current'
						 			    ELSE 'not_done'
									END AS StatusID) AS Valores	
			   OUTER APPLY (SELECT CASE WHEN Valores.StatusID = 'null' THEN 'Dispensado'
										WHEN UltimaTentativa.StatusId = 'aguardando_correcao' THEN 'Aguardando Correção' 
										WHEN UltimaTentativa.MissaoConcluida = 1 THEN CASE WHEN UltimaTentativa.StatusId = 'aprovado' THEN 'Aprovado'
																				 		  WHEN UltimaTentativa.StatusId = 'reprovado' THEN 'Reprovado'
																				 		  WHEN UltimaTentativa.StatusId = 'realizado_fora_prazo' THEN 'Fora do Prazo'
																				 		  ELSE 'Concluido'
																				 	  END
										WHEN Valores.StatusID = 'current' THEN CASE WHEN ISNULL(UltimaTentativa.StatusId, '') = '' THEN 'Não Iniciado'
																					WHEN UltimaTentativa.StatusId = 'nao_iniciado' AND UltimaTentativa.TentativaId IS NOT NULL THEN 'Em Andamento'
										WHEN ISNULL(UltimaTentativa.StatusId, '') = ''	 THEN 'Não Iniciado'
																					ELSE 'Em Andamento'
																				END
										WHEN Valores.StatusID = 'not_current' THEN CASE WHEN ISNULL(UltimaTentativa.StatusId, '') = '' AND GETDATE() < COALESCE(DispensaRodada.DT_INICIO_PARA_REALIZACAO, CargaHoraria.DataInicioModulo) THEN 'Não Liberado'
																						WHEN UltimaTentativa.StatusId = 'em_andamento' THEN 'Em Andamento'
																						ELSE 'Não Iniciado'
																					END
										WHEN Valores.StatusId = 'not_done' THEN 'Expirado (Não Realizado)'
									END AS StatusRodada) DetalhesModulo
					 WHERE (ISNULL(@competicaoId, '') = '' OR CargaHoraria.AmbienteId IN (SELECT OriginalValue 
																							FROM fnt_ConverterFiltrosParaTabela(@competicaoId, 'competicaoId')))
					   AND (ISNULL(@trilhaId, '') = '' OR CargaHoraria.TrilhaId IN (SELECT OriginalValue 
																					  FROM fnt_ConverterFiltrosParaTabela(@trilhaId, 'trilhaId')))
					   AND (ISNULL(@rodadaId, '') = '' OR CargaHoraria.ModuloId IN (SELECT OriginalValue 
																					  FROM fnt_ConverterFiltrosParaTabela(@rodadaId, 'rodadaId')))
					   AND (RelatorioFiltroGrupo.GrupoId IS NOT NULL OR ISNULL(@grupo, '') = '')
					   AND ((ISNULL(@perfilUsuarioTrilhaIds, '') = '' AND UPE.ID_PERFIL_JOGO IN (1, 2, 3)) 
					                                                 OR EXISTS(SELECT 1  
																			     FROM USUARIO_PERFIL_ENTIDADE AS UsuarioPerfilEntidade (NOLOCK)
																				WHERE UsuarioPerfilEntidade.ID_PERFIL_JOGO IN (
																					SELECT OriginalValue 
																					  FROM fnt_ConverterFiltrosParaTabela(@perfilUsuarioTrilhaIds, 'perfilUsuarioTrilhaIds') 
																				)
																				  AND UsuarioPerfilEntidade.ID_USUARIO = UPE.ID_USUARIO 
																				  AND UsuarioPerfilEntidade.ID_ENTIDADE = UPE.ID_ENTIDADE
																				  AND UsuarioPerfilEntidade.ID_ENTIDADE_TIPO = UPE.ID_ENTIDADE_TIPO))
					   AND ((ISNULL(@roundLinkDateStartDate, '') != '' AND ISNULL(@roundLinkDateEndDate, '') != '' AND UPE.DT_CADASTRO BETWEEN @roundLinkDateStartDate AND @roundLinkDateEndDate) 
					         OR (ISNULL(@roundLinkDateStartDate, '') = '' OR ISNULL(@roundLinkDateEndDate, '') = ''))
					   AND ((@showEmptyDates = 1 AND UltimaTentativa.DataPrimeiroAcessoNaMissao IS NULL) 
					         OR (ISNULL(@roundStartDateStartDate, '') != '' AND ISNULL(@roundStartDateEndDate, '') != '' AND UltimaTentativa.DataPrimeiroAcessoNaMissao BETWEEN @roundStartDateStartDate AND @roundStartDateEndDate) 
					         OR (ISNULL(@roundStartDateStartDate, '') = '' OR ISNULL(@roundStartDateEndDate, '') = ''))
				       AND ((@showEmptyDates = 1 AND UltimaTentativa.DataConclusao IS NULL) 
					         OR (ISNULL(@roundCompletionDateStartDate, '') != '' AND ISNULL(@roundCompletionDateEndDate, '') != '' AND UltimaTentativa.DataConclusao BETWEEN @roundCompletionDateStartDate AND @roundCompletionDateEndDate) 
					         OR (ISNULL(@roundCompletionDateStartDate, '') = '' OR ISNULL(@roundCompletionDateEndDate, '') = ''))
					   AND ((@showEmptyDates = 1 AND (UltimaTentativa.DataPrimeiroAcessoNaMissao IS NULL))
					         OR (ISNULL(@startDate, '') != '' AND ISNULL(@endDate, '') != '' AND UltimaTentativa.DataPrimeiroAcessoNaMissao BETWEEN @startDate AND @endDate)
							 OR (ISNULL(@startDate, '') != '' AND ISNULL(@endDate, '') != '' AND UltimaTentativa.DataConclusao BETWEEN @startDate AND @endDate)
							 OR @startDate IS NULL AND @endDate IS NULL)
					   -- @todo: implements ShowUsersWhoHaverNeverAccessed?
					   --AND (((UltimaTentativa.DataPrimeiroAcessoNaMissao BETWEEN @dataInicio AND @dataTermino) OR (@showUsersWhoHaveNeverAccessed = 1 AND UltimaTentativa.DataPrimeiroAcessoNaMissao IS NULL)) OR @dataInicio IS NULL OR @dataTermino IS NULL)
					   --AND ((@showUsersWhoHaveNeverAccessed = 1) OR (@showUsersWhoHaveNeverAccessed = 0 AND DetalhesModulo.StatusRodada NOT IN('Não Iniciado')))
				) SELECT UsuarioID
					   , NomeUsuario
					   , EmailUsuario
					   , LoginUsuario
					   , StatusUsuario
					   , [Id Grupo Pai] AS [GrupoPaiID]
					   , [Grupo Pai] AS [NomeGrupoPai]
					   , [Id Grupo Filho] AS [GrupoFilhoID]
					   , [Grupo Filho] AS [NomeGrupoFilho]
					   , [TodosGruposUsuario]
				       , AmbienteID
					   , NomeAmbiente
					   , TrilhaID
					   , NomeTrilha
					   , ModuloID
					   , NomeModulo
					   , TentativaID
					   , PerfilNaTrilha
					   , FN_INLINE1.FormattedTime AS CargaHorariaModulo
					   , FORMAT(DataPrimeiroAcessoNaMissao, @dateFormat) AS DataInicioModulo
					   , DataConclusao AS DataConclusaoModulo
					   , CASE WHEN StatusRodada IN('Aprovado', 'Reprovado', 'Fora do Prazo') THEN REPLACE(ISNULL(PontuacaoSemBonus, '0'),'.',',') ELSE '' END AS PontuacaoSemBonus
					   , CASE WHEN StatusRodada IN('Aprovado', 'Reprovado', 'Fora do Prazo') THEN BonusDesafio ELSE '' END AS BonusDesafio
					   , CASE WHEN StatusRodada IN('Aprovado', 'Reprovado', 'Fora do Prazo') THEN REPLACE(ISNULL(BonusTempoRestante, '0'),'.',',') ELSE '' END AS BonusTempoRestante
					   , CASE WHEN TRY_PARSE(Pontuacao AS DECIMAL(10, 4)) IS NOT NULL AND StatusRodada IN('Aprovado', 'Reprovado', 'Fora do Prazo') THEN REPLACE(CONVERT(VARCHAR(50), CONVERT(DECIMAL(10,2), Pontuacao)), '.', ',') ELSE '' END AS PontuacaoTotalModulo
					   , [StatusRodada] AS [StatusModulo]
					   , FN_INLINE2.FormattedTime AS TempoAcessoModuloEmHoras
					   , CASE WHEN StatusRodada = 'Fora do Prazo' THEN 'Fora do Prazo'
							  WHEN StatusRodada IN('Aprovado', 'Reprovado') THEN 'Dentro do Prazo'
							  WHEN StatusRodada IN('Não Iniciado', 'Em Andamento') THEN '' END AS RealizadoDentroDoPrazo
					   , DataCadastroModulo AS DataCriacaoModulo
					   , DataUltimaAtualizacaoRodada AS DataUltimaAtualizacaoModulo
					   , AtualizadoPor
					   , NumeroTentativa
					   , DataVinculoModulo
					   , OrigemVinculoModulo
					   , VinculadoPor
					INTO #ResultadosModulos
					FROM CTE
			 OUTER APPLY dbo.[FormatarSegundosEmHoras_inline](CargaHoraria * 60)  as FN_INLINE1
			 OUTER APPLY dbo.[FormatarSegundosEmHoras_inline](TempoAcesso)  as FN_INLINE2
				WHERE (ISNULL(@statusConclusaoRodada, '') = '' OR StatusRodada IN (SELECT ConvertedValue FROM fnt_ConverterFiltrosParaTabela(@statusConclusaoRodada, 'statusConclusaoRodada')))



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
			SET @sql += '		SELECT UsuariosAdmin.* '
			SET @sql += '			 , TempAtributos.Atributo '
			SET @sql += '			 , TempAtributos.AtributoValor '
			SET @sql += '		  FROM #UsuariosAdmin AS UsuariosAdmin '
			SET @sql += '	 LEFT JOIN #TempAtributosUsuario AS TempAtributos'
			SET @sql += '	        ON TempAtributos.EntidadeId = UsuariosAdmin.UsuarioId '
			SET @sql += '	 LEFT JOIN BATCHMODE '
			SET @sql += '	        ON (SELECT 1) = (SELECT 0) '	
			SET @sql += ')   SELECT pvt.UsuarioID '
			SET @sql += '		  , pvt.NomeUsuario '
			SET @sql += '		  , pvt.EmailUsuario '
			SET @sql += '		  , pvt.LoginUsuario '
			SET @sql += '		  , IIF (pvt.StatusUsuario = 1, ''Ativo'', ''Inativo'') AS StatusUsuario '
			SET @sql += '		  , Resultado.GrupoPaiID '
			SET @sql += '		  , Resultado.NomeGrupoPai '
			SET @sql += '		  , Resultado.GrupoFilhoID '
			SET @sql += '		  , Resultado.NomeGrupoFilho '
			SET @sql += '		  , pvt.TodosGruposUsuario '
			SET @sql += '		  , Resultado.AmbienteID '
			SET @sql += '		  , Resultado.NomeAmbiente '
			SET @sql += '		  , Resultado.TrilhaID '
			SET @sql += '		  , Resultado.NomeTrilha '
			SET @sql += '		  , Resultado.ModuloID '
			SET @sql += '		  , Resultado.NomeModulo '
			SET @sql += '		  , Resultado.TentativaID '
			SET @sql += '		  , Resultado.PerfilNaTrilha '
			SET @sql += '		  , Resultado.CargaHorariaModulo '
			SET @sql += '		  , Resultado.DataInicioModulo '
			SET @sql += '		  , Resultado.DataConclusaoModulo '
			SET @sql += '		  , Resultado.PontuacaoSemBonus '
			SET @sql += '		  , Resultado.BonusDesafio '
			SET @sql += '		  , Resultado.BonusTempoRestante '
			SET @sql += '		  , Resultado.PontuacaoTotalModulo '
			SET @sql += '		  , Resultado.StatusModulo '
			SET @sql += '		  , Resultado.TempoAcessoModuloEmHoras '
			SET @sql += '		  , Resultado.RealizadoDentroDoPrazo '
			SET @sql += '		  , Resultado.DataCriacaoModulo '
			SET @sql += '		  , Resultado.DataUltimaAtualizacaoModulo '
			SET @sql += '		  , AtualizadoPor '
			SET @sql += '		  , NumeroTentativa '
			SET @sql += '		  , DataVinculoModulo '
			SET @sql += '		  , OrigemVinculoModulo '
			SET @sql += '		  , VinculadoPor '
			SET @sql += '		  , ' + @fieldNames+ '' -- Lista os campos adicionais
			SET @sql += '	   FROM CTE '
			SET @sql += '     PIVOT (MAX(AtributoValor) FOR Atributo IN(' + @fieldNames + ')) pvt '
			SET @sql += '      JOIN #ResultadosModulos AS Resultado '
			SET @sql += '		 ON Resultado.UsuarioId = pvt.UsuarioId '
			SET @sql += ' LEFT JOIN BATCHMODE '
			SET @sql += '	     ON (SELECT 1) = (SELECT 0) '
			SET @sql += '    OPTION (MAXDOP 6)'
	
			EXEC (@sql);
			return;
		END
		ELSE 
		BEGIN
			SELECT *
			  FROM #ResultadosModulos
		END

END


--/*

---- SELECT * fROM COMPETICAO WHERE ID_CLIENTE = 'UNIVERSOVENDAS'
---- SELECT * fROM COMPETICAO_TRILHA WHERE ID_COMPETICAO = 19277
---- SELECT * fROM USUARIO WHERE ID_CLIENTE = 'UNIVERSOVENDAS' AND TX_LOGIN = 'RICARDO.CARVALHO@ENGAGE.BZ'

--		EXEC [Report].[engagesp_RelatorioStatusModulosLinhas] @adminId = 870546
--															, @clienteID = 'universovendas'
--															, @competicaoId	= 19277
--															, @trilhaId		= 76531
--															, @rodadaId		= NULL	
--															, @dataInicio	= '2024-04-09 00:00'
--															, @dataTermino	= '2024-10-09 23:59'
--															, @perfilUsuarioTrilhaIds  = '1, 2, 3'
--															, @showUsersWhoHaveNeverAccessed = 0
--															, @userStatus	 = 1
--															, @filters		= default
--															-- , @enableReport = 1



--DECLARE 
--	  @adminId		INT = (SELECT ID FROM USUARIO WHERE ID_CLIENTE = 'fabricio' AND TX_LOGIN = 'andre.rodrigues@engage.bz')
--	, @clienteID	NVARCHAR(24) = 'fabricio'
--	, @usuario	    NVARCHAR(255) = '1427293' -- 108898 -- 78114
--	, @userStatus	NVARCHAR(MAX) = '1'
--	, @grupo NVARCHAR(MAX) =  NULL		
--	, @competicaoId	NVARCHAR(MAX) = '22120' --'22123,19473' 22120
--	, @trilhaId		NVARCHAR(MAX) = '80423'
--    , @rodadaId		NVARCHAR(MAX) = NULL
--	, @perfilUsuarioTrilhaIds NVARCHAR (20) = '1,2, 3'
--	, @roundStartDateStartDate DATETIME =  '2023-01-18T20:31:51.000Z'
--    , @roundStartDateEndDate DATETIME = GETDATE()
--	, @roundStartDateShowEmptyDates BIT = 1 
--	, @statusConclusaoRodada NVARCHAR(MAX) = NULL -- invalid_certification, valid_certification/* @todo: ainda falta tratar melhor os nomes para o compare */
--	, @roundLinkDateStartDate DATETIME = NULL
--    , @roundLinkDateEndDate DATETIME = NULL
--	, @enableReport	BIT = 1
--	, @exibirTodasTentativas BIT = 0
--	, @filterAttributes PublicoAlvoCriterio
--	, @roundCompletionDateStartDate DATETIME = NULL
--	, @roundCompletionDateEndDate datetime = null
--	, @roundCompletionDateShowEmptyDates BIT = 1

--*/
GO

