USE [prod_my_engage_autosservico]
GO

/****** Object:  StoredProcedure [Report].[engagesp_Relatorio_De_Tempo_De_Atividade_Por_Usuario]    Script Date: 10/27/2025 7:11:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE  PROCEDURE [Report].[engagesp_Relatorio_De_Tempo_De_Atividade_Por_Usuario]
      @adminId		INT = NULL
	, @clienteId	NVARCHAR(24) = NULL
	, @userStatus	NVARCHAR(20) = '1'
    , @perfilUsuarioTrilhaIds NVARCHAR(20) = '1, 2'
    , @competicaoId NVARCHAR(MAX)  = NULL
    , @trilhaId		NVARCHAR(MAX) = NULL
    , @rodadaId		NVARCHAR(MAX) = NULL
	, @atividadeId	NVARCHAR(MAX) = NULL
	, @atividadeTipo NVARCHAR(MAX) = NULL
	, @firstAccessToActivityDateStartDate DATETIME = NULL
	, @firstAccessToActivityDateEndDate	DATETIME = NULL
	, @lastActivityAccessDateStartDate DATETIME = NULL
	, @lastActivityAccessDateEndDate DATETIME = NULL
	, @usuario NVARCHAR(MAX) = NULL
	, @activityCompletionDateStartDate DATETIME = NULL
	, @activityCompletionDateEndDate DATETIME = NULL
	, @grupo NVARCHAR(MAX) = NULL 
	, @statusConclusaoAtividade NVARCHAR(124) = NULL
	, @showInactiveCompetition BIT = 0
	, @showInactiveTrack BIT = 0
	, @showInactiveRound BIT = 0
	, @showInactiveActivity BIT = 0
	, @showEmptyDates BIT = 0
AS


BEGIN

		IF OBJECT_ID ('tempdb..#UsuariosAdmin', 'U')			IS NOT NULL DROP TABLE #UsuariosAdmin
		IF OBJECT_ID ('tempdb..#CompeticoesAdmin', 'U')			IS NOT NULL DROP TABLE #CompeticoesAdmin
		--IF OBJECT_ID ('tempdb..#AtividadesAdmin', 'U')			IS NOT NULL DROP TABLE #AtividadesAdmin
		IF OBJECT_ID ('tempdb..#UsuarioTrilha', 'U')			IS NOT NULL DROP TABLE #UsuarioTrilha
		IF (OBJECT_ID (N'tempdb..#PlanoUsuario', N'U'))         IS NOT NULL DROP TABLE #PlanoUsuario
		IF (OBJECT_ID (N'tempdb..#Resultado', N'U'))			IS NOT NULL DROP TABLE #Resultado
		IF (OBJECT_ID (N'tempdb..#TempAtributosUsuario', N'U')) IS NOT NULL DROP TABLE #TempAtributosUsuario
	
	 DECLARE @subgrupos SubGrupo;

		SELECT EntidadeId AS UsuarioId
			 , ClienteID 
			 , Grupos.TodosGruposUsuario
		  INTO #UsuariosAdmin
		  FROM fnt_EntidadesEditaveis(@adminId, @clienteID, 'US', @subgrupos) AS EntidadesEditaveis		
   OUTER APPLY fnt_TodosGruposUsuarios (EntidadesEditaveis.EntidadeId, EntidadesEditaveis.ClienteID, @grupo) AS Grupos
		WHERE EXISTS (SELECT TOP 1 1
					    FROM USUARIO_PERFIL_ENTIDADE AS UsuarioPerfilEntidade (NOLOCK)
					   WHERE UsuarioPerfilEntidade.ID_USUARIO = EntidadesEditaveis.EntidadeId
						 AND UsuarioPerfilEntidade.ID_CLIENTE = EntidadesEditaveis.ClienteID
						 AND UsuarioPerfilEntidade.ID_PERFIL_JOGO IN(1,2))
	CREATE UNIQUE CLUSTERED INDEX IX_UsuarioAdmin ON #UsuariosAdmin (UsuarioId, ClienteId)

	   SELECT EntidadeId AS CompeticaoId
		    , ClienteID
		 INTO #CompeticoesAdmin
	     FROM fnt_EntidadesEditaveis (@adminId, @clienteID, 'CP', @subgrupos) AS EntidadesEditaveis;
	CREATE UNIQUE CLUSTERED INDEX IX_CompeticaoAdmin ON #CompeticoesAdmin (CompeticaoId, ClienteId)


	--   SELECT EntidadeId AS AtividadeId
	--	    , ClienteID
	--	 INTO #AtividadesAdmin
	--     FROM fnt_EntidadesEditaveis (@adminId, @clienteID, 'AT', @subgrupos) AS EntidadesEditaveis;
	--CREATE INDEX IX_AtividadeAdmin ON #AtividadesAdmin (AtividadeId, ClienteId) -- Comentado em 29/05/2025 por limitação atual do sistema: ainda não é possível definir grupos diretamente para as atividades. As permissões são herdadas dos grupos do usuário criador. Débito técnico registrado para futura implementação.



	 CREATE TABLE #UsuarioTrilha (CompeticaoId INT NOT NULL, 								  
								  ClienteId NVARCHAR(24) COLLATE LAtin1_General_CI_AI NOT NULL,
								  Competicao NVARCHAR(500) NOT NULL,
								  TrilhaId INT NOT NULL, 
								  Trilha NVARCHAR(500) NOT NULL,
								  RodadaId INT NOT NULL,
								  Rodada NVARCHAR(500) NOT NULL,
								  AtividadeId INT NOT NULL,
								  AtividadeTipoId NVARCHAR(32) NOT NULL,
								  Titulo NVARCHAR(500) NOT NULL,
								  Enunciado NVARCHAR(MAX) NULL
								  )

		;WITH Atividades
		AS
		(
			SELECT Atividade.ID AS AtividadeID
				 , Atividade.ID_CLIENTE AS ClienteId
				 , Atividade.ID_TIPO AS AtividadeTipoId
				 , Atividade.TX_TITULO AS Atividade
				 , Atividade.TX_ENUNCIADO AS Enunciado
				 , Atividade.NU_CARGA_HORARIA AS CargaHorariaAtividade
				 , Atividade.ID_TIPO AS AtividadeTipo
			  FROM ATIVIDADE (NOLOCK)
			 WHERE ATIVIDADE.ID_TIPO IN ('padrao_sou', 'scorm')
			   AND (ISNULL(@showInactiveActivity, 0) = 1 OR ATIVIDADE.FL_STATUS = 1)
			   AND ATIVIDADE.ID_CLIENTE =@clienteId
			   --AND EXISTS (SELECT *
						--	 FROM #AtividadesAdmin AS AtividadesAdmin
						--	WHERE AtividadesAdmin.AtividadeId = Atividade.Id
						--	  AND AtividadesAdmin.ClienteID = Atividade.ID_CLIENTE)
		) -- SELECT * FROM Atividades
		, RodadaAtividades
		AS
		(
				SELECT Atividades.*
					 , Competicao.ID  AS CompeticaoID
					 , Competicao.TX_NOME AS Competicao
					 , TRILHA.ID AS TrilhaID
					 , TRILHA.TX_DESCRICAO AS Trilha
					 , Rodada.ID  AS RodadaID
					 , Rodada.TX_NOME AS Rodada
				  FROM Atividades
				  JOIN RODADA_ATIVIDADE AS RA
				    ON RA.ID_ATIVIDADE = Atividades.AtividadeID
				   AND RA.ID_CLIENTE = Atividades.ClienteId
				  JOIN RODADA AS Rodada
				    ON Rodada.ID = RA.ID_RODADA
				   AND Rodada.ID_CLIENTE = RA.ID_CLIENTE
				   AND (ISNULL(@showInactiveRound, 0) = 1 OR Rodada.FL_STATUS = 1)
				  JOIN TRILHA AS Trilha (NOLOCK)
				    ON Trilha.ID = Rodada.ID_TRILHA
				   AND Trilha.ID_CLIENTE = Rodada.ID_CLIENTE
				   AND (ISNULL(@showInactiveTrack, 0) = 1 OR Trilha.FL_STATUS = 1)
				  JOIN COMPETICAO_TRILHA CT (NOLOCK)
				    ON CT.ID_TRILHA = Trilha.ID
				   AND CT.ID_CLIENTE = Trilha.ID_CLIENTE
				  JOIN COMPETICAO AS Competicao (NOLOCK)
				    ON Competicao.ID = CT.ID_COMPETICAO
				   AND Competicao.ID_CLIENTE = CT.ID_CLIENTE
				   AND (ISNULL(@showInactiveCompetition, 0) = 1 OR Competicao.FL_DISPONIVEL = 1)
			     WHERE EXISTS (SELECT *
								 FROM #CompeticoesAdmin AS CompeticoesAdmin
								WHERE CompeticoesAdmin.CompeticaoId = Competicao.ID
								  AND CompeticoesAdmin.ClienteID = Competicao.ID_CLIENTE)
					AND ISNULL(@competicaoId, '') = '' OR Competicao.ID IN (SELECT OriginalValue 
					                         FROM fnt_ConverterFiltrosParaTabela(@competicaoId, 'competicaoId'))
					AND (ISNULL(@trilhaId, '') = ''  OR Trilha.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@trilhaId, 'trilhaId'))) 
					AND (ISNULL(@rodadaId, '') = '' OR Rodada.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@rodadaId, 'rodadaId')))

		) -- SELECT * FROM RodadaAtividades end
		, PlanoUsuario
		AS 
		(
				SELECT RodadaAtividades.*
					 , UPE.ID_USUARIO AS UsuarioId
				  FROM RodadaAtividades
				  JOIN USUARIO_PERFIL_ENTIDADE AS UPE (NOLOCK)
				    ON UPE.ID_ENTIDADE = RodadaAtividades.RodadaID
				   AND UPE.ID_ENTIDADE_TIPO = 'RD'
				   AND UPE.ID_CLIENTE = RodadaAtividades.ClienteId
				   AND UPE.ID_PERFIL_JOGO IN(1,2)
		) SELECT * 
		    INTO #PlanoUsuario
			FROM PlanoUsuario
		CREATE NONCLUSTERED INDEX IX_PlanoUsuario ON #PlanoUsuario (UsuarioId, ClienteId);

		IF (ISNULL(@rodadaId, '') != '')
		BEGIN
				;WITH CTE
				AS
				(

						SELECT Competicao.ID  AS CompeticaoID
							 , Competicao.ID_CLIENTE AS ClienteId
							 , Competicao.TX_NOME AS Competicao
							 , TRILHA.ID AS TrilhaID
							 , Trilha.TX_DESCRICAO AS Trilha
							 , Rodada.ID  AS RodadaID
							 , Rodada.TX_NOME AS Rodada
							 , Atividade.ID AS AtividadeID
							 , Atividade.ID_TIPO AS AtividadeTipoId
							 , Atividade.TX_TITULO AS Titulo
							 , ISNULL(Atividade.TX_ENUNCIADO, '') AS Enunciado
						  FROM ATIVIDADE (NOLOCK)
						  JOIN RODADA_ATIVIDADE RA (NOLOCK)
							ON RA.ID_ATIVIDADE = Atividade.ID
						   AND RA.ID_CLIENTE = Atividade.ID_CLIENTE
						   AND RA.ID_CLIENTE = Atividade.ID_CLIENTE
						  JOIN RODADA AS Rodada (NOLOCK)
							ON Rodada.ID = RA.ID_RODADA
						   AND Rodada.ID_CLIENTE = RA.ID_CLIENTE
						   AND (ISNULL(@showInactiveRound, 0) = 1 OR Rodada.FL_STATUS = 1)
						  JOIN COMPETICAO_TRILHA AS CompeticaoTrilha (NOLOCK)
							ON CompeticaoTrilha.ID_TRILHA = Rodada.ID_TRILHA
						   AND CompeticaoTrilha.ID_CLIENTE = Rodada.ID_CLIENTE
						  JOIN TRILHA (NOLOCK)
							ON TRILHA.ID = CompeticaoTrilha.ID_TRILHA
						   AND TRILHA.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
						   AND (ISNULL(@showInactiveTrack, 0) = 1 OR TRILHA.FL_STATUS = 1)
						  JOIN COMPETICAO AS Competicao (NOLOCK)
							ON Competicao.ID = CompeticaoTrilha.ID_COMPETICAO
						   AND Competicao.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
						   AND (ISNULL(@showInactiveCompetition, 0) = 1 OR Competicao.FL_DISPONIVEL = 1)
						 WHERE ATIVIDADE.ID_CLIENTE = @clienteId
						   AND (ISNULL(@showInactiveActivity, 0) = 1 OR Atividade.FL_STATUS = 1)
						   AND CompeticaoTrilha.ID_CLIENTE = @clienteId
						   --AND EXISTS (SELECT *
									--	 FROM #AtividadesAdmin AS AtividadesAdmin
									--	WHERE AtividadesAdmin.AtividadeId = Atividade.ID
									--	  AND AtividadesAdmin.ClienteID = Atividade.ID_CLIENTE)
						   AND EXISTS (SELECT *
										 FROM #CompeticoesAdmin AS CompeticoesAdmin
										WHERE CompeticoesAdmin.CompeticaoId = Competicao.ID
										  AND CompeticoesAdmin.ClienteID = Competicao.ID_CLIENTE)
						   AND (ISNULL(@competicaoId, '') = '' OR Competicao.ID IN (SELECT OriginalValue 
					                         FROM fnt_ConverterFiltrosParaTabela(@competicaoId, 'competicaoId')))
						   AND (ISNULL(@trilhaId, '') = '' OR TRILHA.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@trilhaId, 'trilhaId')))
						   AND (ISNULL(@rodadaId, '') = '' OR Rodada.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@rodadaId, 'rodadaId')))
						   AND (ISNULL(@atividadeId, '') = '' OR ATIVIDADE.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@atividadeId, 'atividade')))
						   AND (ISNULL(@atividadeTipo, '') = ''  OR ATIVIDADE.ID_TIPO IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@atividadeTipo, 'atividadeTipo')))
						  
				) INSERT #UsuarioTrilha (CompeticaoId, ClienteId, Competicao , TrilhaId, Trilha, RodadaId, Rodada, AtividadeId, AtividadeTipoId, Titulo, Enunciado)			
				  SELECT CompeticaoId
					   , ClienteId 
					   , Competicao 
					   , TrilhaId
					   , Trilha
					   , RodadaId
					   , Rodada
					   , AtividadeId
					   , AtividadeTipoId
					   , Titulo
					   , Enunciado
					FROM CTE
		END
		ELSE IF (ISNULL(@trilhaId, '') != '')
		BEGIN
				;WITH CTE
				AS
				(

						SELECT Competicao.ID  AS CompeticaoID
							 , Competicao.ID_CLIENTE AS ClienteId
							 , Competicao.TX_NOME AS Competicao
							 , TRILHA.ID AS TrilhaID
							 , Trilha.TX_DESCRICAO AS Trilha
							 , Rodada.ID  AS RodadaID
							 , Rodada.TX_NOME AS Rodada
							 , Atividade.ID AS AtividadeID
							 , Atividade.ID_TIPO AS AtividadeTipoId
							 , Atividade.TX_TITULO AS Titulo
							 , ISNULL(Atividade.TX_ENUNCIADO, '') AS Enunciado
						  FROM ATIVIDADE (NOLOCK)
						  JOIN RODADA_ATIVIDADE RA (NOLOCK)
							ON RA.ID_ATIVIDADE = Atividade.ID
						   AND RA.ID_CLIENTE = Atividade.ID_CLIENTE
						   AND RA.ID_CLIENTE = Atividade.ID_CLIENTE
						  JOIN RODADA AS Rodada (NOLOCK)
							ON Rodada.ID = RA.ID_RODADA
						   AND Rodada.ID_CLIENTE = RA.ID_CLIENTE
						   AND (ISNULL(@showInactiveRound, 0) = 1 OR Rodada.FL_STATUS = 1)
						  JOIN COMPETICAO_TRILHA AS CompeticaoTrilha (NOLOCK)
							ON CompeticaoTrilha.ID_TRILHA = Rodada.ID_TRILHA
						   AND CompeticaoTrilha.ID_CLIENTE = Rodada.ID_CLIENTE
						  JOIN TRILHA (NOLOCK)
							ON TRILHA.ID = CompeticaoTrilha.ID_TRILHA
						   AND TRILHA.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
						   AND (ISNULL(@showInactiveTrack, 0) = 1 OR TRILHA.FL_STATUS = 1)
						  JOIN COMPETICAO AS Competicao (NOLOCK)
							ON Competicao.ID = CompeticaoTrilha.ID_COMPETICAO
						   AND Competicao.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
						   AND (ISNULL(@showInactiveCompetition, 0) = 1 OR Competicao.FL_DISPONIVEL = 1)
						 WHERE CompeticaoTrilha.ID_CLIENTE = @clienteId
						   AND ATIVIDADE.ID_CLIENTE = @clienteId
						   AND (ISNULL(@showInactiveActivity, 0) = 1 OR Atividade.FL_STATUS = 1)
						   --AND EXISTS (SELECT *
									--	 FROM #AtividadesAdmin AS AtividadesAdmin
									--	WHERE AtividadesAdmin.AtividadeId = Atividade.ID
									--	  AND AtividadesAdmin.ClienteID = Atividade.ID_CLIENTE)
						   AND EXISTS (SELECT *
										 FROM #CompeticoesAdmin AS CompeticoesAdmin
										WHERE CompeticoesAdmin.CompeticaoId = Competicao.ID
										  AND CompeticoesAdmin.ClienteID = Competicao.ID_CLIENTE)
						   AND (ISNULL(@competicaoId, '') = '' OR Competicao.ID IN (SELECT OriginalValue 
					                         FROM fnt_ConverterFiltrosParaTabela(@competicaoId, 'competicaoId')))
						   AND (ISNULL(@rodadaId, '') = '' OR Rodada.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@rodadaId, 'rodadaId')))
						   AND (ISNULL(@trilhaId, '') = '' OR TRILHA.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@trilhaId, 'trilhaId')))
						   AND (ISNULL(@atividadeTipo, '') = ''  OR ATIVIDADE.ID_TIPO IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@atividadeTipo, 'atividadeTipo')))
				) INSERT #UsuarioTrilha (CompeticaoId, ClienteId, Competicao , TrilhaId, Trilha, RodadaId, Rodada, AtividadeId, AtividadeTipoId, Titulo, Enunciado)			
				  SELECT CompeticaoId
					   , ClienteId 
					   , Competicao 
					   , TrilhaId
					   , Trilha
					   , RodadaId
					   , Rodada
					   , AtividadeId
					   , AtividadeTipoId
					   , Titulo
					   , Enunciado
					FROM CTE	
		END
		ELSE IF (ISNULL(@competicaoId, '') != '')
		BEGIN
				;WITH CTE
				AS
				(

						SELECT Competicao.ID  AS CompeticaoID
							 , Competicao.ID_CLIENTE AS ClienteId
							 , Competicao.TX_NOME AS Competicao
							 , TRILHA.ID AS TrilhaID
							 , Trilha.TX_DESCRICAO AS Trilha
							 , Rodada.ID  AS RodadaID
							 , Rodada.TX_NOME AS Rodada
							 , Atividade.ID AS AtividadeID
							 , Atividade.ID_TIPO AS AtividadeTipoId
							 , Atividade.TX_TITULO AS Titulo
							 , ISNULL(Atividade.TX_ENUNCIADO, '') AS Enunciado
						  FROM ATIVIDADE (NOLOCK)
						  JOIN RODADA_ATIVIDADE RA (NOLOCK)
							ON RA.ID_ATIVIDADE = Atividade.ID
						   AND RA.ID_CLIENTE = Atividade.ID_CLIENTE
						   AND RA.ID_CLIENTE = Atividade.ID_CLIENTE
						  JOIN RODADA AS Rodada (NOLOCK)
							ON Rodada.ID = RA.ID_RODADA
						   AND Rodada.ID_CLIENTE = RA.ID_CLIENTE
						   AND (ISNULL(@showInactiveRound, 0) = 1 OR Rodada.FL_STATUS = 1)
						  JOIN COMPETICAO_TRILHA AS CompeticaoTrilha (NOLOCK)
							ON CompeticaoTrilha.ID_TRILHA = Rodada.ID_TRILHA
						   AND CompeticaoTrilha.ID_CLIENTE = Rodada.ID_CLIENTE
						  JOIN TRILHA (NOLOCK)
							ON TRILHA.ID = CompeticaoTrilha.ID_TRILHA
						   AND TRILHA.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
						   AND (ISNULL(@showInactiveTrack, 0) = 1 OR TRILHA.FL_STATUS = 1)
						  JOIN COMPETICAO AS Competicao (NOLOCK)
							ON Competicao.ID = CompeticaoTrilha.ID_COMPETICAO
						   AND Competicao.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
						   AND (ISNULL(@showInactiveCompetition, 0) = 1 OR Competicao.FL_DISPONIVEL = 1)
						 WHERE ATIVIDADE.ID_CLIENTE = @clienteId
						   AND (ISNULL(@showInactiveActivity, 0) = 1 OR Atividade.FL_STATUS = 1)
						   --AND EXISTS (SELECT *
									--	 FROM #AtividadesAdmin AS AtividadesAdmin
									--	WHERE AtividadesAdmin.AtividadeId = Atividade.ID
									--	  AND AtividadesAdmin.ClienteID = Atividade.ID_CLIENTE)
						   AND EXISTS (SELECT *
										 FROM #CompeticoesAdmin AS CompeticoesAdmin
										WHERE CompeticoesAdmin.CompeticaoId = Competicao.ID
										  AND CompeticoesAdmin.ClienteID = Competicao.ID_CLIENTE)
						   AND (ISNULL(@competicaoId, '') = '' OR (Competicao.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@competicaoId, 'competicao'))))
						   AND (ISNULL(@rodadaId, '') = '' OR Rodada.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@rodadaId, 'rodadaId')))
						   AND (ISNULL(@trilhaId, '') = '' OR (TRILHA.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@trilhaId, 'trilhaId'))))
						   AND (ISNULL(@atividadeTipo, '') = ''  OR ATIVIDADE.ID_TIPO IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@atividadeTipo, 'atividadeTipo')))
				) INSERT #UsuarioTrilha (CompeticaoId, ClienteId, Competicao , TrilhaId, Trilha, RodadaId, Rodada, AtividadeId, AtividadeTipoId, Titulo, Enunciado)			
				  SELECT CompeticaoId
					   , ClienteId 
					   , Competicao 
					   , TrilhaId
					   , Trilha
					   , RodadaId
					   , Rodada
					   , AtividadeId
					   , AtividadeTipoId
					   , Titulo
					   , Enunciado
					FROM CTE

		END
		ELSE 
		BEGIN
				;WITH CTE
				AS
				(

						SELECT Competicao.ID  AS CompeticaoID
							 , Competicao.ID_CLIENTE AS ClienteId
							 , Competicao.TX_NOME AS Competicao
							 , TRILHA.ID AS TrilhaID
							 , Trilha.TX_DESCRICAO AS Trilha
							 , Rodada.ID  AS RodadaID
							 , Rodada.TX_NOME AS Rodada
							 , Atividade.ID AS AtividadeID
							 , Atividade.ID_TIPO AS AtividadeTipoId
							 , Atividade.TX_TITULO AS Titulo
							 , ISNULL(Atividade.TX_ENUNCIADO, '') AS Enunciado
						  FROM ATIVIDADE (NOLOCK)
						  JOIN RODADA_ATIVIDADE RA (NOLOCK)
							ON RA.ID_ATIVIDADE = Atividade.ID
						   AND RA.ID_CLIENTE = Atividade.ID_CLIENTE
						   AND RA.ID_CLIENTE = Atividade.ID_CLIENTE
						  JOIN RODADA AS Rodada (NOLOCK)
							ON Rodada.ID = RA.ID_RODADA
						   AND Rodada.ID_CLIENTE = RA.ID_CLIENTE
						   AND (ISNULL(@showInactiveRound, 0) = 1 OR Rodada.FL_STATUS = 1)
						  JOIN COMPETICAO_TRILHA AS CompeticaoTrilha (NOLOCK)
							ON CompeticaoTrilha.ID_TRILHA = Rodada.ID_TRILHA
						   AND CompeticaoTrilha.ID_CLIENTE = Rodada.ID_CLIENTE
						  JOIN TRILHA (NOLOCK)
							ON TRILHA.ID = CompeticaoTrilha.ID_TRILHA
						   AND Trilha.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
						   AND (ISNULL(@showInactiveTrack, 0) = 1 OR TRILHA.FL_STATUS = 1)
						  JOIN COMPETICAO AS Competicao (NOLOCK)
							ON Competicao.ID = CompeticaoTrilha.ID_COMPETICAO
						   AND Competicao.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
						   AND (ISNULL(@showInactiveCompetition, 0) = 1 OR Competicao.FL_DISPONIVEL = 1)
						 WHERE ATIVIDADE.ID_CLIENTE = @clienteId
						   AND (ISNULL(@showInactiveActivity, 0) = 1 OR Atividade.FL_STATUS = 1)
						   --AND EXISTS (SELECT *
									--	 FROM #AtividadesAdmin AS AtividadesAdmin
									--	WHERE AtividadesAdmin.AtividadeId = Atividade.ID
									--	  AND AtividadesAdmin.ClienteID = Atividade.ID_CLIENTE)
						   AND EXISTS (SELECT *
										 FROM #CompeticoesAdmin AS CompeticoesAdmin
										WHERE CompeticoesAdmin.CompeticaoId = Competicao.ID
										  AND CompeticoesAdmin.ClienteID = Competicao.ID_CLIENTE)
						   AND (ISNULL(@competicaoId, '') = '' OR Competicao.ID IN (SELECT OriginalValue 
					                         FROM fnt_ConverterFiltrosParaTabela(@competicaoId, 'competicaoId')))
						   AND (ISNULL(@trilhaId, '') = '' OR TRILHA.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@trilhaId, 'trilhaId')))
						   AND (ISNULL(@rodadaId, '') = '' OR Rodada.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@rodadaId, 'rodadaId')))
						   AND (ISNULL(@atividadeTipo, '') = ''  OR ATIVIDADE.ID_TIPO IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@atividadeTipo, 'atividadeTipo')))
				) INSERT #UsuarioTrilha (CompeticaoId, ClienteId, Competicao , TrilhaId, Trilha, RodadaId, Rodada, AtividadeId, AtividadeTipoId, Titulo, Enunciado)			
				  SELECT CompeticaoId
					   , ClienteId 
					   , Competicao 
					   , TrilhaId
					   , Trilha
					   , RodadaId
					   , Rodada
					   , AtividadeId
					   , AtividadeTipoId
					   , Titulo
					   , Enunciado
					FROM CTE	
		END
		
				 SELECT 
						USUARIO.ID AS UsuarioID
					  , USUARIO.TX_NOME_COMPLETO AS NomeUsuario
					  , USUARIO.TX_EMAIL AS EmailUsuario
					  , USUARIO.TX_LOGIN AS LoginUsuario
					  , CASE WHEN Usuario.FL_STATUS = 1 THEN 'Ativo'
							 ELSE 'Inativo' END AS StatusUsuario
					  , UsuarioGrupo.GrupoPaiId
					  , UsuarioGrupo.GrupoPai AS NomeGrupoPai
					  , UsuarioGrupo.GrupoID AS GrupoFilhoId
					  , UsuarioGrupo.Grupo AS NomeGrupoFilho
					  , UsuariosAdmin.TodosGruposUsuario
					  , UsuarioTrilha.CompeticaoID AS AmbienteID
					  , UsuarioTrilha.Competicao AS NomeAmbiente
					  , UsuarioTrilha.TrilhaID
					  , UsuarioTrilha.Trilha AS NomeTrilha
					  , UsuarioTrilha.RodadaId AS ModuloID
					  , UsuarioTrilha.Rodada AS NomeModulo
					  , UsuarioTrilha.AtividadeId
					  , UsuarioTrilha.Titulo AS NomeAtividade
					  , CASE WHEN StatusConclusaoAtividade.StatusUsuarioAtividade = 'Dispensado' THEN ''
							 ELSE CAST(UltimaTentativa.ID AS NVARCHAR(124)) END AS TentativaID
					  ,  CASE WHEN StatusConclusaoAtividade.StatusUsuarioAtividade = 'Dispensado' THEN ''
							  ELSE (CASE WHEN PlanoUsuarioCarga.CargaHorariaAtividade > 0 
							             THEN CAST(ISNULL(PlanoUsuarioCarga.CargaHorariaAtividade, '') AS NVARCHAR(124)) 
										 ELSE '' END)
							  END AS CargaHorariaAtividade
					  , RotuloAtividade.Texto AS TipoAtividades
					  , CASE WHEN StatusConclusaoAtividade.StatusUsuarioAtividade = 'Dispensado' THEN ''
							 ELSE dbo.FormatarSegundosEmHoras(AcessoAtividade.NU_TEMPO_EM_SEGUNDOS) END AS TempoAcessoNaAtividadeEmHoras
					  , CASE WHEN StatusConclusaoAtividade.StatusUsuarioAtividade = 'Dispensado' THEN ''
							 ELSE CONVERT(VARCHAR(10), AcessoAtividade.DT_CADASTRO, 103) + ' ' + CONVERT(VARCHAR(5), AcessoAtividade.DT_CADASTRO, 108) END AS DataPrimeiroAcessoNaAtividade
					  , CASE WHEN StatusConclusaoAtividade.StatusUsuarioAtividade = 'Dispensado' THEN ''
							 ELSE CONVERT(VARCHAR(10), AcessoAtividade.DT_ULTIMA_ATUALIZACAO, 103) + ' ' + CONVERT(VARCHAR(5), AcessoAtividade.DT_ULTIMA_ATUALIZACAO, 108) END  AS DataUltimoAcessoNaAtividade
					  , CASE WHEN StatusConclusaoAtividade.StatusUsuarioAtividade = 'Dispensado' THEN ''
							 ELSE CONVERT(VARCHAR(10), Resposta.DT_RESPOSTA, 103) + ' ' + CONVERT(VARCHAR(5), Resposta.DT_RESPOSTA, 108) END AS DataConclusaoAtividade
					  , CASE WHEN StatusConclusaoAtividade.StatusUsuarioAtividade = 'Dispensado' THEN ''
							 ELSE CAST(CAST(Resposta.NU_PORCENTAGEM_ACERTOS * 100 AS DECIMAL(5,2)) AS NVARCHAR(24)) 
						     END AS AproveitamentoAtividade
					  , UsuarioTrilha.Enunciado AS EnunciadoAtividade
					  , CASE WHEN StatusConclusaoAtividade.StatusUsuarioAtividade = 'Dispensado' THEN ''
							 WHEN UsuarioTrilha.AtividadeTipoId IN ('dissertativaPontuacao', 'dissertativa') THEN Resposta.TX_RESPOSTA_DISSERTATIVA
							 WHEN UsuarioTrilha.AtividadeTipoId IN ('multipla_escolha', 'verdadeiro_falso', 'multiplas_respostas', 'pesquisaMultiplaEscolha', 'pesquisaMultiplasRespostas', 'verdadeiro_falso') THEN RespostaMultiplaEscolha.TX_ALTERNATIVA
							 WHEN UsuarioTrilha.AtividadeTipoId = 'formulario' THEN RespostaAtividadeFormulario.Respostas
							 WHEN UsuarioTrilha.AtividadeTipoId = 'minigame' THEN RespostaAtividadeMinigame.Respostas
							 END AS RespostaUsuario
					  , CASE WHEN StatusConclusaoAtividade.StatusUsuarioAtividade = 'Dispensado' THEN '' 
							 ELSE CAST(COALESCE(RAU.NU_PESO, RA.NU_PESO) AS NVARCHAR(24)) END AS PesoAtividade
					  , CASE WHEN StatusConclusaoAtividade.StatusUsuarioAtividade = 'Dispensado' THEN '' 
							 ELSE ISNULL(Resposta.TX_FEEDBACK, '') END AS FeedbackAtividade
					  , StatusConclusaoAtividade.StatusUsuarioAtividade
					  , CASE WHEN UPE.ID_PERFIL_JOGO = 1 THEN 'Obrigatório'
							WHEN UPE.ID_PERFIL_JOGO = 2 THEN 'Participa'
							WHEN UPE.ID_PERFIL_JOGO = 3 THEN 'Gestor'
							ELSE 'Não Participa'
					    END AS PerfilNaTrilha
				  INTO #Resultado
				  FROM #UsuarioTrilha AS UsuarioTrilha
				  JOIN USUARIO_PERFIL_ENTIDADE AS UPE (NOLOCK)
				    ON UPE.ID_ENTIDADE = UsuarioTrilha.RodadaId
				   AND UPE.ID_CLIENTE = UsuarioTrilha.ClienteId
				   AND UPE.ID_ENTIDADE_TIPO  = 'RD'
				   AND UPE.ID_PERFIL_JOGO IN (1, 2, 3)
				  JOIN RODADA AS Rodada (NOLOCK)
				    ON Rodada.ID = UPE.ID_ENTIDADE
				   AND Rodada.ID_CLIENTE = UPE.ID_CLIENTE
				   AND (ISNULL(@showInactiveRound, 0) = 1 OR Rodada.FL_STATUS = 1)
				  JOIN USUARIO (NOLOCK)
				    ON USUARIO.ID = UPE.ID_USUARIO
				   AND USUARIO.ID_CLIENTE = UPE.ID_CLIENTE
				   AND (Usuario.FL_STATUS IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@userStatus, 'userStatus')) OR ISNULL(@userStatus, '') = '')
				   AND Usuario.DT_EXCLUSAO IS NULL
				  JOIN #UsuariosAdmin AS UsuariosAdmin								 
					ON UsuariosAdmin.UsuarioId = USUARIO.ID
				   AND UsuariosAdmin.ClienteID = USUARIO.ID_CLIENTE
				  JOIN ATIVIDADE_TIPO (NOLOCK)
				    ON ATIVIDADE_TIPO.ID COLLATE SQL_Latin1_General_CP1_CI_AS = UsuarioTrilha.AtividadeTipoId
		  CROSS APPLY fnt_rotuloIdiomaPorID(ATIVIDADE_TIPO.ID_ROTULO_NOME, @clienteID, 'pt-br') AS RotuloAtividade
		     LEFT JOIN RODADA_ATIVIDADE_USUARIO AS RAU (NOLOCK)
					ON RAU.ID_RODADA = UsuarioTrilha.RodadaId
				   AND RAU.ID_CLIENTE = UsuarioTrilha.ClienteId
				   AND RAU.ID_ATIVIDADE = UsuarioTrilha.AtividadeId
				   AND RAU.ID_USUARIO = UPE.ID_USUARIO
			 LEFT JOIN RODADA_ATIVIDADE AS RA (NOLOCK)
					ON RA.ID_RODADA = UsuarioTrilha.RodadaId
				   AND RA.ID_CLIENTE = UsuarioTrilha.ClienteId
				   AND RA.ID_ATIVIDADE = UsuarioTrilha.AtividadeId
				   AND RAU.ID_USUARIO IS NULL
		   OUTER APPLY (SELECT TOP 1 *
						  FROM TENTATIVA AS Tentativa (NOLOCK)
						 WHERE Tentativa.ID_USUARIO = RAU.ID_USUARIO
						   AND Tentativa.ID_CLIENTE = RAU.ID_CLIENTE
						   AND Tentativa.ID_RODADA = RAU.ID_RODADA
			   			   AND Tentativa.ID_STATUS != 'cancelado'
						   ORDER BY Tentativa.ID DESC) AS UltimaTentativa
			 LEFT JOIN ACESSO_ATIVIDADE AS AcessoAtividade (NOLOCK)
					ON AcessoAtividade.ID_TENTATIVA = UltimaTentativa.ID
				   AND AcessoAtividade.ID_CLIENTE = UltimaTentativa.ID_CLIENTE
				   AND AcessoAtividade.ID_ATIVIDADE = UsuarioTrilha.AtividadeId
		     LEFT JOIN RESPOSTA AS Resposta (NOLOCK)
				    ON Resposta.ID_TENTATIVA = UltimaTentativa.ID
			       AND Resposta.ID_CLIENTE = UltimaTentativa.ID_CLIENTE
			       AND Resposta.ID_ATIVIDADE = UsuarioTrilha.AtividadeId
			       AND Resposta.ID_USUARIO = UltimaTentativa.ID_USUARIO
			       AND Resposta.ID_COMPETICAO = UsuarioTrilha.CompeticaoID
			 LEFT JOIN DISPENSA_RODADA AS DispensaRodada (NOLOCK)
				    ON DispensaRodada.ID_RODADA = Rodada.ID
				   AND DispensaRodada.ID_USUARIO = USUARIO.ID
				   AND DispensaRodada.ID_CLIENTE = USUARIO.ID_CLIENTE
		   OUTER APPLY (  SELECT STRING_AGG(CAST(ATIVIDADE_ALTERNATIVA.TX_ALTERNATIVA AS NVARCHAR(MAX)), ', ') AS TX_ALTERNATIVA
						   FROM ATIVIDADE_ALTERNATIVA (NOLOCK)
						   JOIN RESPOSTA_ALTERNATIVA AS RA (NOLOCK)
						     ON RA.ID_ATIVIDADE = ATIVIDADE_ALTERNATIVA.ID_ATIVIDADE
						    AND RA.ID_CLIENTE = ATIVIDADE_ALTERNATIVA.ID_CLIENTE
						    AND RA.ID_ALTERNATIVA = ATIVIDADE_ALTERNATIVA.ID
						    AND RA.ID_USUARIO = Resposta.ID_USUARIO
						    AND RA.ID_COMPETICAO = Resposta.ID_COMPETICAO
				          WHERE ATIVIDADE_ALTERNATIVA.ID_ATIVIDADE = Resposta.ID_ATIVIDADE
						    AND ATIVIDADE_ALTERNATIVA.ID_CLIENTE = Resposta.ID_CLIENTE
							AND ATIVIDADE_ALTERNATIVA.FL_STATUS = 1
							AND RA.FL_CORRETA = 1
							AND RA.ID_TENTATIVA  = Resposta.ID_TENTATIVA
						GROUP BY ATIVIDADE_ALTERNATIVA.TX_ALTERNATIVA
					) AS RespostaMultiplaEscolha
		   OUTER APPLY (SELECT STRING_AGG(CAST(CONCAT_WS(', ', RespostaAlternativaFormulario.TX_ALTERNATIVA,
														  RespostaAtividadeFormulario.TX_RESPOSTA_DISSERTATIVA,
									RespostaAtividadeFormulario.NU_RESPOSTA_ESCALA) AS NVARCHAR(MAX)), ', ') AS Respostas
						   FROM PERGUNTA_FORMULARIO as PerguntaFormulario
						   OUTER APPLY (SELECT RespostaAtividadeFormulario.TX_RESPOSTA_DISSERTATIVA,
											   RespostaAtividadeFormulario.NU_RESPOSTA_ESCALA,
											   RespostaAtividadeFormulario.ID_CLIENTE,
											   RespostaAtividadeFormulario.ID_TENTATIVA,
											   RespostaAtividadeFormulario.DT_CADASTRO
									      FROM RESPOSTA_ATIVIDADE_FORMULARIO AS RespostaAtividadeFormulario (NOLOCK)
										 WHERE RespostaAtividadeFormulario.ID_PERGUNTA = PerguntaFormulario.ID
								           AND RespostaAtividadeFormulario.ID_CLIENTE = PerguntaFormulario.ID_CLIENTE
								           AND RespostaAtividadeFormulario.ID_TENTATIVA = UltimaTentativa.ID
								    AND PerguntaFormulario.FL_STATUS = 1) AS RespostaAtividadeFormulario
						    OUTER APPLY (SELECT RespostaAlternativaFormulario.ID_ALTERNATIVA,
												RespostaAlternativaFormulario.ID_CLIENTE,
												RespostaAlternativaFormulario.ID_PERGUNTA_FORMULARIO,
												RespostaAlternativaFormulario.ID_TENTATIVA,
												RespostaAlternativaFormulario.DT_CADASTRO,
												RespostaAlternativaFormulario.FL_SELECIONADA,
												AlternativaFormulario.TX_ALTERNATIVA,
												AlternativaFormulario.FL_CORRETA
										   FROM ALTERNATIVA_FORMULARIO AlternativaFormulario (NOLOCK)
										   JOIN RESPOSTA_ALTERNATIVA_FORMULARIO RespostaAlternativaFormulario (NOLOCK)
											 ON AlternativaFormulario.ID = RespostaAlternativaFormulario.ID_ALTERNATIVA
											AND AlternativaFormulario.ID_CLIENTE = RespostaAlternativaFormulario.ID_CLIENTE
										  WHERE RespostaAlternativaFormulario.ID_TENTATIVA = UltimaTentativa.ID
											AND RespostaAlternativaFormulario.ID_CLIENTE = PerguntaFormulario.ID_CLIENTE
											AND RespostaAlternativaFormulario.ID_PERGUNTA_FORMULARIO = PerguntaFormulario.ID
											AND RespostaAlternativaFormulario.FL_SELECIONADA = 1
											AND PerguntaFormulario.FL_STATUS = 1) AS RespostaAlternativaFormulario
							WHERE PerguntaFormulario.ID_CLIENTE = RAU.ID_CLIENTE
							  AND PerguntaFormulario.ID_ATIVIDADE = RAU.ID_ATIVIDADE
							  AND PerguntaFormulario.FL_STATUS = 1
			) RespostaAtividadeFormulario
		   OUTER APPLY (SELECT STRING_AGG(CAST(AlternativaMinigame.TX_ALTERNATIVA AS NVARCHAR(MAX)), ', ') AS Respostas
						  FROM RESPOSTA_MINIGAME AS RM
					 LEFT JOIN RESPOSTA_ALTERNATIVA_MINIGAME AS RespostaAlternativaMinigame
							ON RespostaAlternativaMinigame.ID_TENTATIVA = RM.ID_TENTATIVA
						   AND RespostaAlternativaMinigame.ID_CLIENTE = RM.ID_CLIENTE
						   AND RespostaAlternativaMinigame.ID_PERGUNTA_MINIGAME = RM.ID_PERGUNTA_MINIGAME
						   AND RespostaAlternativaMinigame.FL_SELECIONADA = 1		 
					 LEFT JOIN ALTERNATIVA_MINIGAME AS AlternativaMinigame
							ON AlternativaMinigame.ID = RespostaAlternativaMinigame.ID_ALTERNATIVA
						   AND AlternativaMinigame.ID_CLIENTE = RespostaAlternativaMinigame.ID_CLIENTE
						   AND AlternativaMinigame.FL_STATUS = 1
					    WHERE RM.ID_TENTATIVA = UltimaTentativa.ID 
						  AND RM.ID_CLIENTE = RAU.ID_CLIENTE
		   ) RespostaAtividadeMinigame
		   CROSS APPLY (SELECT TOP 1 Grupo.ID AS GrupoID, Grupo.ID_CLIENTE AS ClienteId, Grupo.TX_NOME AS Grupo, GrupoPai.Id AS GrupoPaiId, GrupoPai.TX_NOME AS GrupoPai
						  FROM USUARIO_GRUPO AS UsuarioGrupo (NOLOCK)
						  JOIN GRUPO AS Grupo (NOLOCK)
						    ON Grupo.ID = UsuarioGrupo.ID_GRUPO
						   AND Grupo.ID_CLIENTE = UsuarioGrupo.ID_CLIENTE
						   AND Grupo.FL_STATUS = 1
						  JOIN COMPETICAO_GRUPO AS CompeticaoGrupo (NOLOCK)
						    ON CompeticaoGrupo.ID_GRUPO = Grupo.Id
						   AND CompeticaoGrupo.ID_CLIENTE = Grupo.ID_CLIENTE
						   AND CompeticaoGrupo.ID_COMPETICAO = UsuarioTrilha.CompeticaoId
						   AND CompeticaoGrupo.ID_CLIENTE = UsuarioTrilha.ClienteId
					 LEFT JOIN GRUPO AS GrupoPai (NOLOCK)
						    ON GrupoPai.ID = Grupo.ID_GRUPO_PAI
						   AND GrupoPai.ID_CLIENTE = Grupo.ID_CLIENTE
						   AND GrupoPai.FL_STATUS = 1
						 WHERE UsuarioGrupo.ID_USUARIO = Usuario.ID
						   AND UsuarioGrupo.ID_CLIENTE = Usuario.ID_CLIENTE) AS UsuarioGrupo
		     LEFT JOIN #PlanoUsuario AS PlanoUsuarioCarga
					ON PlanoUsuarioCarga.UsuarioId =  Usuario.ID
				   AND PlanoUsuarioCarga.ClienteId = Usuario.ID_CLIENTE
			OUTER APPLY ( SELECT 
							CASE WHEN DispensaRodada.DT_DISPENSA IS NOT NULL THEN 'Dispensado'
								 WHEN Resposta.DT_CADASTRO IS NULL AND CAST(Rodada.DT_TERMINO AS DATETIME) < GETDATE() THEN 'Expirado'
								 WHEN Resposta.FL_CORRIGIDA = 0 AND ATIVIDADE_TIPO.FL_REQUER_CORRECAO = 1 THEN 'Aguardando Correção'
								 WHEN AcessoAtividade.DT_CADASTRO IS NOT NULL AND Resposta.DT_CADASTRO IS NULL THEN 'Em Andamento'
								 WHEN CAST(Rodada.DT_INICIO AS DATETIME) < GETDATE() AND UltimaTentativa.ID IS NULL THEN 'Não Iniciado'
								 WHEN CAST(Rodada.DT_INICIO AS DATETIME) > GETDATE() THEN 'Não Liberado' 
								 WHEN Resposta.DT_RESPOSTA IS NOT NULL OR UltimaTentativa.ID_STATUS IN ('aprovado','concluido','reprovado') THEN 'Concluído'
								 ELSE 'Não Iniciado'
							  END AS StatusUsuarioAtividade	
						) StatusConclusaoAtividade
				  WHERE  USUARIO.FL_PADRAO = 0
					AND (ISNULL(@usuario, '') = '' OR USUARIO.ID IN (SELECT ConvertedValue 
                          FROM fnt_ConverterFiltrosParaTabela(@usuario, 'usuario')))
					AND (ISNULL(@grupo, '') = '' OR (UsuarioGrupo.Grupo IN (UsuariosAdmin.TodosGruposUsuario) OR 
													 UsuarioGrupo.GrupoPai IN ((UsuariosAdmin.TodosGruposUsuario))))
					AND ((@showEmptyDates = 1 AND UltimaTentativa.ID IS NULL)
						OR (ISNULL(@firstAccessToActivityDateStartDate, '') != '' AND ISNULL(@firstAccessToActivityDateEndDate, '') != '' AND AcessoAtividade.DT_CADASTRO BETWEEN @firstAccessToActivityDateStartDate AND @firstAccessToActivityDateEndDate) 
					    OR (ISNULL(@firstAccessToActivityDateStartDate, '') = '' OR  ISNULL(@firstAccessToActivityDateEndDate, '') = ''))
					AND ((@showEmptyDates = 1 AND ISNULL(AcessoAtividade.DT_ULTIMA_ATUALIZACAO, AcessoAtividade.DT_CADASTRO) IS NULL)
						OR (ISNULL(@lastActivityAccessDateStartDate, '') != '' AND ISNULL(@lastActivityAccessDateEndDate, '') != '' AND AcessoAtividade.DT_ULTIMA_ATUALIZACAO BETWEEN @lastActivityAccessDateStartDate  AND @lastActivityAccessDateEndDate) 
						OR (ISNULL(@lastActivityAccessDateStartDate, '') = '' OR  ISNULL(@lastActivityAccessDateEndDate, '') = ''))
					AND ((@showEmptyDates = 1 AND Resposta.DT_RESPOSTA IS NULL) 
						OR (ISNULL(@activityCompletionDateStartDate, '') != '' AND ISNULL(@activityCompletionDateEndDate, '') != '' AND Resposta.DT_RESPOSTA BETWEEN @activityCompletionDateStartDate AND @activityCompletionDateEndDate) 
						OR (ISNULL(@activityCompletionDateStartDate, '') = '' OR ISNULL(@activityCompletionDateEndDate, '') = '')) 
					AND (@perfilUsuarioTrilhaIds IS NULL OR UPE.ID_PERFIL_JOGO IN (
						  SELECT OriginalValue  FROM fnt_ConverterFiltrosParaTabela(@perfilUsuarioTrilhaIds, 'perfilUsuarioTrilhaIds')))
				    AND (ISNULL(@statusConclusaoAtividade, '') = '' OR 
							StatusConclusaoAtividade.StatusUsuarioAtividade IN (SELECT ConvertedValue 
		                                                  FROM fnt_ConverterFiltrosParaTabela(@statusConclusaoAtividade, 'statusConclusaoAtividade')))

 IF EXISTS (SELECT *
             FROM ATRIBUTO
            WHERE Atributo.ID_CLIENTE = @clienteId
              AND ATRIBUTO.FL_STATUS = 1)
        BEGIN
            DECLARE @sql NVARCHAR(MAX) = '', @fieldNames NVARCHAR(MAX) = ''
            --DECLARE @clienteId nvarchar(24) = N'eucatur'

            SET @fieldNames = (SELECT ', [' + Atributo + ']' AS [text()]
            FROM (SELECT REPLACE (ATRIBUTO.TX_NOME, @clienteID + '_', '') AS Atributo
                       , Atributo.ID AS AtributoID
                    FROM Atributo (NOLOCK)
                   WHERE Atributo.FL_STATUS = 1
                     AND Atributo.ID_CLIENTE = @clienteId ) AS A ORDER BY AtributoID
                 FOR XML PATH (''))

            SELECT UsuariosAdmin.UsuarioId AS EntidadeId
				 , UsuariosAdmin.ClienteId AS CustomerId
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
			   AND Atributos.ClienteId = UsuariosAdmin.ClienteId

			CREATE INDEX IX_TempAtributosUsuario ON #TempAtributosUsuario ([EntidadeId]) INCLUDE ([Atributo], [AtributoValor])
            SET @fieldNames = (SELECT SUBSTRING(@fieldNames, 2, LEN(@fieldNames)));          
            
            SET @sql += '; WITH CTE AS ( '
            SET @sql += '       SELECT Resultado.*'
            SET @sql += '            , TempAtributos.Atributo '
            SET @sql += '            , TempAtributos.AtributoValor '
            SET @sql += '         FROM #Resultado AS Resultado '
            SET @sql += '    LEFT JOIN #TempAtributosUsuario AS TempAtributos'
            SET @sql += '           ON TempAtributos.EntidadeId = Resultado.UsuarioId '
            SET @sql += '	 LEFT JOIN BATCHMODE '
			SET @sql += '			ON (SELECT 1) = (SELECT 0) '
            SET @sql += ') SELECT pvt.*'
            SET @sql += '    FROM CTE'
            SET @sql += '   PIVOT (MAX(AtributoValor) FOR Atributo IN(' + @fieldNames + ')) pvt '
			SET @sql += '  OPTION (MAXDOP 1) '

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
	 declare 
 @adminId		INT = (select id from usuario where tx_login = 'leonardo.silva@engage.bz' AND ID_CLIENTE = 'leosilva')
				, @clienteId	NVARCHAR(24) = 'leosilva'
				, @competicaoId NVARCHAR(MAX) = '22162'
				, @trilhaId		NVARCHAR(MAX) = '80526' -- '80528'
				, @rodadaId		NVARCHAR(MAX) = '353897'
				, @atividadeId	NVARCHAR(MAX) = NULL
				, @firstAccessToActivityDateStartDate	DATETIME = ''
				, @firstAccessToActivityDateEndDate	DATETIME = ''
				, @firstAccessToActivityDateShowEmptyDates	BIT = 1
				, @atividadeTipo NVARCHAR(MAX) = NULL	
				, @lastActivityAccessDateStartDate DATETIME = NULL
				, @lastActivityAccessDateEndDate DATETIME = NULL
				, @lastActivityAccessDateShowEmptyDates BIT = 1
				, @userStatus	NVARCHAR(24) = '1,0'
				, @perfilUsuarioTrilhaIds NVARCHAR (20) = '1, 2, 3'
				, @usuario NVARCHAR(MAX) = NULL
				, @activityCompletionDateStartDate DATETIME = NULL
				, @activityCompletionDateEndDate DATETIME = NULL
				, @activityCompletionDateShowEmptyDates BIT = 1
				, @grupo NVARCHAR(MAX) = NULL -- 46267
				, @statusConclusaoAtividade NVARCHAR(124) = ''
*/
GO

