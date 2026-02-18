USE [prod_my_engage_autosservico]
GO

/****** Object:  StoredProcedure [dbo].[engagesp_RelatorioStatusModulosColuna]    Script Date: 10/27/2025 7:07:43 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[engagesp_RelatorioStatusModulosColuna]
	@adminId		 INT,
	@clienteID		 NVARCHAR (24),
	@competicaoID	 NVARCHAR(MAX) = NULL,
	@userStatus		 NVARCHAR(20) = '1',
	@usuario NVARCHAR(MAX) = NULL,
	@perfilUsuarioTrilhaIds nvarchar(20) = '1, 2',
	@statusConclusaoCompeticao NVARCHAR(124) = NULL,
	@grupo NVARCHAR(MAX) = NULL,
	@showInactiveCompetition BIT = 0
AS

BEGIN

		DECLARE @grupos SubGrupo;

		IF OBJECT_ID(N'tempdb..#CompeticoesAdmin', N'U') IS NOT NULL
		  DROP TABLE #CompeticoesAdmin
		IF OBJECT_ID(N'tempdb..#UsuariosAdmin', N'U') IS NOT NULL
		  DROP TABLE #UsuariosAdmin
		IF OBJECT_ID(N'tempdb..#TempAtributosUsuario', N'U') IS NOT NULL
		  DROP TABLE #TempAtributosUsuario
		IF OBJECT_ID('tempdb..##RelatorioRotulado','U') IS NOT NULL DROP TABLE ##RelatorioRotulado;

		SELECT EntidadeId AS UsuarioID
			 , ClienteID 
			 , Grupos.TodosGruposUsuario
		  INTO #UsuariosAdmin
		  FROM fnt_EntidadesEditaveis(@adminId, @clienteID, 'US', @grupos) AS EntidadesEditaveis
   OUTER APPLY fnt_TodosGruposUsuarios (EntidadesEditaveis.EntidadeId, EntidadesEditaveis.ClienteID, @grupo) AS Grupos
		CREATE UNIQUE CLUSTERED INDEX IX_UsuarioAdmin ON #UsuariosAdmin (UsuarioID, ClienteId)

		SELECT EntidadeId
			 , ClienteID
		  INTO #CompeticoesAdmin
		  FROM fnt_EntidadesEditaveis(@adminId, @clienteID, 'CP', @grupos) AS EntidadesEditaveis
		CREATE UNIQUE CLUSTERED INDEX IX_UsuarioAdmin ON #CompeticoesAdmin (EntidadeId, ClienteId)

		IF OBJECT_ID(N'tempdb..#Rodadas', 'U') IS NOT NULL
		  DROP TABLE #Rodadas;

		WITH CTE
		AS 
		(
				SELECT USUARIO.ID AS UsuarioID
					 , Usuario.ID_CLIENTE AS ClienteID
					 , Rodada.ID AS ModuloId
				     , Rodada.NU_ORDEM AS OrdemModulo
				     , Rodada.TX_NOME AS NomeModulo
				     , USUARIO.TX_NOME_COMPLETO AS NomeUsuario
				     , USUARIO.TX_LOGIN AS LoginUsuario
				     , USUARIO.TX_EMAIL AS EmailUsuario
					 , ROW_NUMBER() OVER (
							PARTITION BY Usuario.ID, Usuario.ID_CLIENTE, Trilha.ID
							ORDER BY
								COALESCE(DispensaRodadas.DataInicioRealizacaoDispensa, Rodada.DT_INICIO),
								Rodada.NU_ORDEM,
								Rodada.ID
						) AS RodadaIndex
				     , SUBSTRING(Trilha.TX_DESCRICAO + ' - ' + RODADA.TX_NOME, 0, 125) AS Rodada
				     , TRILHA.TX_DESCRICAO AS NomeTrilha
				     , TRILHA.ID AS TrilhaId
				     , UsuarioGrupo.GrupoFilhoID
				     , UsuarioGrupo.NomeGrupoFilho
				     , UsuarioGrupo.NomeGrupoPai
				     , UsuarioGrupo.PaiID AS GrupoPaiID
				     , UsuariosAdmin.TodosGruposUsuario
				     , Competicao.ID AS AmbienteId
				     , Competicao.TX_NOME AS Nomeambiente
					 , CASE WHEN Usuario.FL_STATUS = 1 THEN 'Ativo'
							ELSE 'Inativo'
						END AS StatusUsuario
					 , CASE WHEN UltimaTentativa.MissaoConcluida = 1 THEN Nota END AS Nota
					 , CASE WHEN Usuario.FL_STATUS = 0 THEN 'Inativo'
							WHEN Valores.StatusID = 'null' THEN 'Disp/Inat'
							WHEN UltimaTentativa.MissaoConcluida = 1 /*AND TRY_PARSE(Pontuacao AS INT) IS NOT NULL*/ THEN CONVERT(varchar, CONVERT(decimal(10, 2), Pontuacao))
						END AS Pontuacao
					 , CASE WHEN Valores.StatusID = 'null' THEN 'Disp/Inat'
							WHEN UltimaTentativa.StatusId = 'aguardando_correcao' THEN 'Aguardando Correção'
							WHEN UltimaTentativa.MissaoConcluida = 1 THEN CASE WHEN UltimaTentativa.StatusId = 'aprovado' THEN 'Aprovado'
																			   WHEN UltimaTentativa.StatusId = 'reprovado' THEN 'Reprovado'
																			   ELSE 'Concluido'
																		   END
							WHEN Valores.StatusID = 'current' THEN CASE WHEN ISNULL(UltimaTentativa.StatusId, '') = '' THEN 'Não Iniciado'
																		WHEN UltimaTentativa.StatusId = 'nao_iniciado' THEN 'Não Iniciado'
																		ELSE 'Em Andamento'
																	END
							WHEN Valores.StatusID = 'not_current' THEN CASE WHEN UltimaTentativa.StatusId = 'em_andamento' THEN 'Em Andamento'
																			ELSE 'Não Iniciado'
																		END
							WHEN Valores.StatusId = 'not_done' THEN 'Não Realizado'
					    END AS StatusRodada
				     , CASE WHEN UPE.ID_PERFIL_JOGO = 1 THEN 'Obrigatório'
							WHEN UPE.ID_PERFIL_JOGO = 2 THEN 'Participa'
							WHEN UPE.ID_PERFIL_JOGO = 3 THEN 'Gestor'
							ELSE 'Não Participa'
					    END AS PerfilNaTrilha
					 , FORMAT(UltimaTentativa.DataConclusao, 'dd/MM/yyyy HH:mm:ss') AS DataConclusao
					 , PontuacaoUsuarioPorCompeticao.NU_PONTUACAO AS PontuacaoAmbiente
					 , COUNT(CASE WHEN RODADA.DT_INICIO <= GETDATE() THEN 1 END) OVER (PARTITION BY Usuario.ID) AS TotalRodadas
					 , DataPrimeiroAcessoNaMissao
					 , DispensaRodadas.Dispensado
					 , DispensaRodadas.DataInicioRealizacaoDispensa 
	                 , DispensaRodadas.DataTerminoRealizacaoDispensa
					 , Rodada.DT_INICIO AS DataInicioRodada
					 , Rodada.DT_TERMINO AS DataTerminoRodada
					 , COUNT(CASE WHEN RODADA.ID IS NOT NULL THEN 1 END) OVER (PARTITION BY Usuario.ID) AS TotalRodadasNoAmbiente
					 , COUNT(CASE WHEN UltimaTentativa.TentativaId IS NOT NULL AND UltimaTentativa.StatusId != 'nao_iniciado' THEN 1 END) OVER (PARTITION BY Usuario.ID) AS TotalRodadasIniciadas
					 , COUNT(CASE WHEN UltimaTentativa.MissaoConcluida = 1 THEN 1 END) OVER (PARTITION BY Usuario.ID) AS TotalRodadasConcluidas
					 , COUNT(CASE WHEN UltimaTentativa.EstaAprovada = 1 THEN 1 END) OVER (PARTITION BY Usuario.ID) AS TotalRodadasAprovadas
				  FROM COMPETICAO_TRILHA AS CompeticaoTrilha (NOLOCK)
				  JOIN COMPETICAO AS Competicao (NOLOCK)
					ON Competicao.ID = CompeticaoTrilha.ID_COMPETICAO
				   AND Competicao.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
				   AND (ISNULL(@showInactiveCompetition, 0) = 1 OR Competicao.FL_DISPONIVEL = 1)
				   AND (ISNULL(@competicaoID, '') = '' OR Competicao.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@competicaoId, 'competicaoId')))
				  JOIN #CompeticoesAdmin AS CompeticoesAdmin
					ON CompeticoesAdmin.EntidadeId = Competicao.ID
				   AND CompeticoesAdmin.ClienteID = Competicao.ID_CLIENTE
				  JOIN TRILHA (NOLOCK)
				    ON TRILHA.ID = CompeticaoTrilha.ID_TRILHA
				   AND TRILHA.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
				   AND TRILHA.FL_STATUS = 1
				  JOIN RODADA AS Rodada (NOLOCK)
				    ON Rodada.ID_TRILHA = Trilha.ID
				   AND Rodada.ID_CLIENTE = Trilha.ID_CLIENTE
				   AND Rodada.FL_STATUS = 1
				  JOIN USUARIO_PERFIL_ENTIDADE AS UPE (NOLOCK)
				    ON UPE.ID_ENTIDADE = Rodada.ID
				   AND UPE.ID_CLIENTE = Rodada.ID_CLIENTE
				   AND UPE.ID_ENTIDADE_TIPO = 'RD'
				   AND ((ISNULL(@perfilUsuarioTrilhaIds, '') = '' AND UPE.ID_PERFIL_JOGO IN (1, 2, 3)) OR UPE.ID_PERFIL_JOGO IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@perfilUsuarioTrilhaIds, 'perfilUsuarioTrilhaIds')))
				  JOIN USUARIO(NOLOCK)
				    ON USUARIO.ID = UPE.ID_USUARIO
				   AND USUARIO.ID_CLIENTE = UPE.ID_CLIENTE
				   AND (ISNULL(@usuario, '') = '' OR USUARIO.ID IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@usuario, 'usuario')) OR ISNULL(@usuario, 0) = 0)
				  JOIN #UsuariosAdmin AS UsuariosAdmin
				    ON UsuariosAdmin.UsuarioID = USUARIO.ID
				   AND UsuariosAdmin.ClienteID = USUARIO.ID_CLIENTE
			 LEFT JOIN PONTUACAO_USUARIO_POR_COMPETICAO AS PontuacaoUsuarioPorCompeticao (NOLOCK)
				    ON PontuacaoUsuarioPorCompeticao.ID_COMPETICAO = CompeticaoTrilha.ID_COMPETICAO
				   AND PontuacaoUsuarioPorCompeticao.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
				   AND PontuacaoUsuarioPorCompeticao.ID_USUARIO = Usuario.ID
				   AND PontuacaoUsuarioPorCompeticao.ID_CLIENTE = Usuario.ID_CLIENTE
				   AND PontuacaoUsuarioPorCompeticao.ID_RANKING_PERSONALIZADO = 0
		   CROSS APPLY (SELECT TOP 1 GRUPO.TX_NOME AS NomeGrupoFilho,
							   GRUPO.ID AS GrupoFilhoID,
							   Pai.TX_NOME AS NomeGrupoPai,
							   Pai.ID AS PaiID,
							   Grupo.ID AS GrupoId
						 FROM USUARIO_GRUPO(NOLOCK)
						 JOIN GRUPO(NOLOCK)
						   ON GRUPO.ID = USUARIO_GRUPO.ID_GRUPO
						  AND Grupo.ID_CLIENTE = USUARIO_GRUPO.ID_CLIENTE
						  AND Grupo.FL_STATUS = 1
						 JOIN COMPETICAO_GRUPO AS COMPETICAOGRUPO (NOLOCK)
						   ON COMPETICAOGRUPO.ID_CLIENTE = CompeticaoTrilha.ID_CLIENTE
						  AND COMPETICAOGRUPO.ID_COMPETICAO = CompeticaoTrilha.ID_COMPETICAO
						  AND COMPETICAOGRUPO.ID_GRUPO = GRUPO.ID
					LEFT JOIN GRUPO AS Pai (NOLOCK)
						   ON Pai.ID = Grupo.ID_GRUPO_PAI
						  AND Pai.ID_CLIENTE = Grupo.ID_CLIENTE
						  AND Pai.FL_STATUS = 1
						WHERE USUARIO_GRUPO.ID_USUARIO = USUARIO.ID
						  AND USUARIO_GRUPO.ID_CLIENTE = Usuario.ID_CLIENTE) AS UsuarioGrupo
		   OUTER APPLY (SELECT TOP 1
							   Tentativa.DT_CADASTRO
							 , Tentativa.ID AS TentativaId
							 , Tentativa.ID_CLIENTE AS ClienteId
							 , Tentativa.NU_NOTA AS Nota
							 , Tentativa.ID_STATUS AS StatusId
							 , Tentativa.NU_PONTUACAO AS PontuacaoSemBonus
							 , Tentativa.NU_BONUS_DESAFIO AS BonusDesafio
							 , Tentativa.NU_PONTUACAO_TEMPO_RESTANTE AS PontuacaoTempoRestante
							 , Tentativa.DT_CADASTRO AS DataPrimeiroAcessoNaMissao
							 , ISNULL(Tentativa.NU_PONTUACAO, 0) + ISNULL(Tentativa.NU_BONUS_DESAFIO, 0) + ISNULL(Tentativa.NU_PONTUACAO_TEMPO_RESTANTE, 0) AS Pontuacao
							 , IIF (Tentativa.ID_STATUS IN('concluido', 'aprovado'), 1, 0) AS EstaAprovada
							 , CASE WHEN Tentativa.ID_STATUS IN ('concluido', 'aprovado', 'reprovado', 'realizado_fora_prazo') THEN 1
									ELSE 0
								END AS MissaoConcluida
							 , CASE WHEN (Tentativa.ID_STATUS IN ('concluido', 'aprovado', 'reprovado', 'realizado_fora_prazo') OR
									Rodada.ID_TIPO = 'recorrente') THEN Tentativa.DT_ULTIMA_ATUALIZACAO
								END AS DataConclusao
						  FROM TENTATIVA AS Tentativa (NOLOCK)
						 WHERE Tentativa.ID_CLIENTE = Usuario.ID_CLIENTE
						   AND Tentativa.ID_USUARIO = Usuario.ID
						   AND Tentativa.ID_RODADA = Rodada.ID
						   AND Tentativa.ID_STATUS != 'cancelado'
					  ORDER BY Tentativa.ID DESC) AS UltimaTentativa
		   OUTER APPLY (SELECT DR.DT_INICIO_PARA_REALIZACAO AS DataInicioRealizacaoDispensa
			                 , DR.DT_TERMINO_PARA_REALIZACAO AS DataTerminoRealizacaoDispensa
							 , DR.DT_DISPENSA 
			                 , CASE WHEN DR.ID_RODADA IS NOT NULL THEN 1 ELSE 0 END AS Dispensado
			              FROM DISPENSA_RODADA AS DR (NOLOCK)
					 	  WHERE DR.ID_RODADA = RODADA.ID
						    AND DR.ID_CLIENTE = RODADA.ID_CLIENTE
							AND DR.ID_USUARIO = Usuario.ID) AS DispensaRodadas
		   OUTER APPLY (SELECT CASE WHEN DispensaRodadas.DT_DISPENSA IS NOT NULL AND DispensaRodadas.DataInicioRealizacaoDispensa IS NOT NULL AND GETDATE() < DispensaRodadas.DataInicioRealizacaoDispensa THEN 'not_current'
									WHEN DispensaRodadas.DT_DISPENSA IS NOT NULL AND (DispensaRodadas.DataInicioRealizacaoDispensa IS NULL OR DispensaRodadas.DataInicioRealizacaoDispensa > GETDATE()) THEN 'null'
									WHEN GETDATE() BETWEEN Rodada.DT_INICIO AND Rodada.DT_TERMINO THEN 'current' -- Verifica apenas a data
									WHEN GETDATE() < COALESCE(DispensaRodadas.DataInicioRealizacaoDispensa, Rodada.DT_INICIO) THEN 'not_current'
									ELSE 'not_done'
								END AS StatusID) AS Valores
				 WHERE USUARIO.ID_CLIENTE = @clienteID
				   AND USUARIO.FL_PADRAO = 0
				   AND (ISNULL(@userStatus, '') = '' OR USUARIO.FL_STATUS IN (SELECT OriginalValue FROM fnt_ConverterFiltrosParaTabela(@userStatus, 'statusUsuario')))
				   AND USUARIO.DT_EXCLUSAO IS NULL
		) , SumarizacaoRodadas
		  AS ( SELECT UsuarioID
					, LoginUsuario
					, EmailUsuario
					, NomeUsuario
					, PerfilNaTrilha
					, GrupoPaiID
					, NomeGrupoPai
					, GrupoFilhoID
					, NomeGrupoFilho
					, TodosGruposUsuario
					, StatusUsuario
					, AmbienteID
					, NomeAmbiente
					, NomeModulo
					, ModuloID
					, NomeTrilha
					, TrilhaID
					, ClienteID
					, RodadaIndex
					, OrdemModulo
				    , CONVERT(nvarchar, RodadaIndex) + N'.' + Rodada AS Rodada
					, PontuacaoAmbiente
					, TotalRodadas
					, CASE WHEN Pontuacao NOT IN ('Inativo', 'Disp/Inat') THEN CONVERT(varchar, REPLACE(CONVERT(decimal(10, 2), Pontuacao), '.', ','))
						   WHEN Pontuacao IS NULL AND DataPrimeiroAcessoNaMissao IS NOT NULL THEN 'Em andamento'
						   WHEN Pontuacao IS NULL and DataPrimeiroAcessoNaMissao IS NULL THEN 'Nunca acessou'
						   ELSE StatusRodada
						END AS PontuacaoRodada 
					, Dispensado
					, DataTerminoRealizacaoDispensa
					, DataInicioRealizacaoDispensa
					, SUM(TotalRodadasNoAmbiente) AS TotalRodadasNoAmbiente
					, SUM(TotalRodadasConcluidas) AS TotalRodadasConcluidas
					, SUM(TotalRodadasAprovadas) AS TotalRodadasAprovadas
					, SUM(TotalRodadasIniciadas) AS TotalRodadasIniciadas
					, MAX(CASE WHEN Dispensado = 1 THEN DataInicioRealizacaoDispensa ELSE DataInicioRodada END) AS DataInicioRodada
					, MAX(CASE WHEN Dispensado = 1 THEN DataTerminoRealizacaoDispensa ELSE DataTerminoRodada END) AS DataTerminoRodada
				 FROM CTE
			  GROUP BY UsuarioID
					 , LoginUsuario
					 , EmailUsuario
					 , NomeUsuario
					 , PerfilNaTrilha
					 , GrupoPaiID
					 , NomeGrupoPai
					 , GrupoFilhoID
					 , NomeGrupoFilho
					 , TodosGruposUsuario
					 , StatusUsuario
					 , AmbienteID
					 , NomeAmbiente
					 , NomeModulo
					 , ModuloID
					 , NomeTrilha
					 , TrilhaID
					 , ClienteID
					 , RodadaIndex
					 , OrdemModulo
					 , RodadaIndex
					 , Pontuacao
					 , DataPrimeiroAcessoNaMissao
					 , Rodada
					 , PontuacaoAmbiente
					 , TotalRodadas
					 , StatusRodada
					 , Dispensado
					 , DataTerminoRealizacaoDispensa
					 , DataInicioRealizacaoDispensa
		  ), SumarizacaoTrilhas 
		  AS (	
		  SELECT UsuarioID
			   , LoginUsuario
			   , EmailUsuario
			   , NomeUsuario
			   , PerfilNaTrilha
			   , GrupoPaiID
			   , NomeGrupoPai
			   , GrupoFilhoID
			   , NomeGrupoFilho
			   , TodosGruposUsuario
			   , StatusUsuario
			   , AmbienteID
			   , NomeAmbiente
			   , NomeModulo
			   , ModuloID
			   , NomeTrilha
			   , TrilhaID
			   , ClienteID
			   , RodadaIndex
			   , OrdemModulo
			   , Rodada
			   , PontuacaoAmbiente
			   , TotalRodadas
			   , PontuacaoRodada 
			   , TotalRodadasNoAmbiente
			   , TotalRodadasConcluidas
			   , TotalRodadasAprovadas
			   , TotalRodadasIniciadas
			   , COUNT(CASE WHEN TrilhaIniciada = 'Sim' THEN 1 END) AS TotalTrilhasIniciadas
			   , COUNT(CASE WHEN TrilhaConcluida = 'Sim' THEN 1 END) AS TotalTrilhasConcluidas
			   , COUNT(TrilhaId) AS TotalTrilhas
			   , Dispensado
			   , DataTerminoRealizacaoDispensa
			   , DataInicioRealizacaoDispensa
			   , DataInicioRodada
			   , DataTerminoRodada
		    FROM SumarizacaoRodadas
	 OUTER APPLY (SELECT IIF (TotalRodadasIniciadas > 0, 'Sim', 'Não') AS TrilhaIniciada
					   , IIF(TotalRodadasNoAmbiente = 0, 'Não', IIF(TotalRodadasConcluidas >= TotalRodadasNoAmbiente, 'Sim', 'Não')) AS TrilhaConcluida) AS ValoresComputados
			GROUP BY UsuarioID
				   , LoginUsuario
				   , EmailUsuario
				   , NomeUsuario
				   , PerfilNaTrilha
				   , GrupoPaiID
				   , NomeGrupoPai
				   , GrupoFilhoID
				   , NomeGrupoFilho
				   , TodosGruposUsuario
				   , StatusUsuario
				   , AmbienteID
				   , NomeAmbiente
				   , NomeModulo
				   , ModuloID
				   , NomeTrilha
				   , TrilhaID
				   , ClienteID
				   , RodadaIndex
				   , OrdemModulo
				   , Rodada
				   , PontuacaoAmbiente
				   , TotalRodadas
				   , PontuacaoRodada 
				   , TotalRodadasNoAmbiente
				   , TotalRodadasConcluidas
				   , TotalRodadasAprovadas
				   , TotalRodadasIniciadas
				   , Dispensado
				   , DataTerminoRealizacaoDispensa
				   , DataInicioRealizacaoDispensa
				   , DataInicioRodada
				   , DataTerminoRodada
		  ), Resultado AS (
			SELECT UsuarioID
				 , LoginUsuario
				 , EmailUsuario
				 , NomeUsuario
				 , PerfilNaTrilha
				 , GrupoPaiID
				 , NomeGrupoPai
				 , GrupoFilhoID
				 , NomeGrupoFilho
				 , TodosGruposUsuario
				 , StatusUsuario
				 , AmbienteID
				 , NomeAmbiente
				 , NomeModulo
				 , ModuloID
				 , NomeTrilha
				 , TrilhaID
				 , ClienteID
				 , RodadaIndex
				 , OrdemModulo
				 , Rodada
				 , PontuacaoAmbiente
				 , TotalRodadas
				 , PontuacaoRodada 
				 , TotalRodadasNoAmbiente
				 , TotalRodadasConcluidas
				 , TotalRodadasAprovadas
				 , TotalRodadasIniciadas
				 ,  CASE 
			   			WHEN SUM(CASE WHEN Dispensado = 1 THEN 1 ELSE 0 END) = COUNT(*) THEN 'Dispensado' 
						WHEN SUM(TotalTrilhasConcluidas) = SUM(TotalTrilhas) AND SUM(TotalTrilhas) > 0 THEN 'Concluído'
						WHEN MAX(CASE WHEN Dispensado = 1 THEN DataTerminoRealizacaoDispensa ELSE DataTerminoRodada END) < GETDATE() 
				   		   AND SUM(TotalTrilhasConcluidas) < SUM(TotalTrilhas) THEN 'Expirado (Não Concluído)'
						WHEN SUM(TotalTrilhasIniciadas) > 0 THEN 'Em Andamento'
						WHEN MAX(CASE WHEN Dispensado = 1 THEN DataInicioRealizacaoDispensa ELSE DataInicioRodada END) > GETDATE() THEN 'Não Liberado'
					ELSE 'Não Iniciado'
					END AS StatusUsuarioAmbiente
			FROM SumarizacaoTrilhas
		GROUP BY UsuarioID
			   , LoginUsuario
			   , EmailUsuario
			   , NomeUsuario
			   , PerfilNaTrilha
			   , GrupoPaiID
			   , NomeGrupoPai
			   , GrupoFilhoID
			   , NomeGrupoFilho
			   , TodosGruposUsuario
			   , StatusUsuario
			   , AmbienteID
			   , NomeAmbiente
			   , NomeModulo
			   , ModuloID
			   , NomeTrilha
			   , TrilhaID
			   , ClienteID
			   , RodadaIndex
			   , OrdemModulo
			   , Rodada
			   , PontuacaoAmbiente
			   , TotalRodadas
			   , PontuacaoRodada 
			   , TotalRodadasNoAmbiente
			   , TotalRodadasConcluidas
			   , TotalRodadasAprovadas
			   , TotalRodadasIniciadas
		 ) SELECT UsuarioID
				, LoginUsuario
				, EmailUsuario
				, NomeUsuario
				, PerfilNaTrilha
				, GrupoPaiID
				, NomeGrupoPai
				, GrupoFilhoID
				, NomeGrupoFilho
				, TodosGruposUsuario
				, StatusUsuario
				, AmbienteID
				, NomeAmbiente
				, NomeModulo
				, ModuloID
				, NomeTrilha
				, TrilhaID
				, ClienteID
				, RodadaIndex
				, OrdemModulo
				, Rodada
				, CASE WHEN StatusUsuarioAmbiente IN ('Não Liberado', 'Expirado (Não concluído)', 'Em andamento', 'Não iniciado', 'Dispensado') THEN ''
						   ELSE REPLACE(CONVERT(varchar, IIF(PontuacaoAmbiente IS NULL, '', CONVERT(DECIMAL(18, 2), PontuacaoAmbiente))), '.', ',') END AS PontuacaoAmbiente
				, TotalRodadas
				, PontuacaoRodada 
				, TotalRodadasNoAmbiente
				, TotalRodadasConcluidas
				, TotalRodadasAprovadas
				, TotalRodadasIniciadas
				, StatusUsuarioAmbiente
			INTO #RODADAS
		   FROM Resultado
		  WHERE (ISNULL(@statusConclusaoCompeticao, '') = '' 
				OR StatusUsuarioAmbiente IN (SELECT ConvertedValue 
                                      FROM fnt_ConverterFiltrosParaTabela(@statusConclusaoCompeticao, 'statusConclusaoCompeticao')));
		/* ========= PIVOT SEGURO POR ID (SEM DUPLICAR COLUNAS) ========= */

			-- Colunas únicas: ModuloKey
			IF OBJECT_ID('tempdb..#ColsPivot','U') IS NOT NULL DROP TABLE #ColsPivot;

			SELECT DISTINCT
				   CAST(ModuloID AS nvarchar(50)) AS ModuloKey,
				   OrdemModulo
			INTO #ColsPivot
			FROM #RODADAS;

			DECLARE @keyList nvarchar(MAX) = N'';
			SELECT @keyList =
			  STUFF((
				SELECT N', ' + QUOTENAME(ModuloKey)
				FROM #ColsPivot
				ORDER BY OrdemModulo, ModuloKey
				FOR XML PATH(''), TYPE
			  ).value('.','nvarchar(max)'), 1, 2, N'');

			DECLARE @sql nvarchar(MAX) = N'';

			-- Base: sempre no mesmo batch dinâmico (cria #RelatorioColunado)
			SET @sql += N'
			SELECT *
			INTO #RelatorioColunado
			FROM (
			  SELECT
				UsuarioID, NomeUsuario, EmailUsuario, LoginUsuario, StatusUsuario,
				GrupoPaiID, NomeGrupoPai, GrupoFilhoID, NomeGrupoFilho, TodosGruposUsuario,
				PerfilNaTrilha, NomeAmbiente, AmbienteID,
				-- já aplicamos sua regra na criação de #RODADAS; aqui só propagamos
				PontuacaoAmbiente,
				CAST(ModuloID AS nvarchar(50)) AS ModuloKey,
				PontuacaoRodada,
				StatusUsuarioAmbiente
			  FROM #RODADAS
			) AS Rodadas
			';

			IF ISNULL(@keyList, N'') <> N''
			BEGIN
				SET @sql += N'PIVOT (MAX(PontuacaoRodada) FOR ModuloKey IN (' + @keyList + N')) pvt ';
			END
			ELSE
			BEGIN
				-- Sem módulos para pivotar: materializa apenas colunas fixas (sem duplicar linhas)
				SET @sql += N'/* Nenhum módulo para pivotar */ ';
				SET @sql += N';WITH BaseFix AS (
					SELECT DISTINCT
						UsuarioID, NomeUsuario, EmailUsuario, LoginUsuario, StatusUsuario,
						GrupoPaiID, NomeGrupoPai, GrupoFilhoID, NomeGrupoFilho, TodosGruposUsuario,
						PerfilNaTrilha, NomeAmbiente, AmbienteID, PontuacaoAmbiente, StatusUsuarioAmbiente
					FROM #RODADAS
				)
				SELECT * INTO #RelatorioColunado FROM BaseFix ';
			END;

			SET @sql += N' OPTION (MAXDOP 1) ';

			SET @sql += N';
				IF OBJECT_ID(''tempdb..#MapaModulo'', ''U'') IS NOT NULL DROP TABLE #MapaModulo;
				SELECT DISTINCT
					CAST(ModuloID AS nvarchar(50)) AS ModuloKey,
					Rodada,
					OrdemModulo
				INTO #MapaModulo
				FROM #RODADAS;
				';

			SET @sql += N'
				-- Renomeia as colunas pivotadas (IDs) para os rótulos da Rodada
				DECLARE @aliasSelect nvarchar(MAX) = N'''';

				;WITH L AS (
					SELECT
						ModuloKey,
						Rodada,
						OrdemModulo,
						rn  = ROW_NUMBER() OVER (PARTITION BY Rodada ORDER BY OrdemModulo, ModuloKey),
						cnt = COUNT(*)    OVER (PARTITION BY Rodada)
					FROM #MapaModulo
				)
				SELECT @aliasSelect =
					STUFF((
						SELECT
							N'', '' + QUOTENAME(ModuloKey) + N'' AS '' +
							QUOTENAME(CASE WHEN cnt = 1 THEN Rodada
										   ELSE Rodada + N'' ('' + CAST(rn AS nvarchar(10)) + N'')''
									  END)
						FROM L
						ORDER BY OrdemModulo, ModuloKey
						FOR XML PATH(''''), TYPE
					).value(''.'',''nvarchar(max)''), 1, 2, N'''');

				DECLARE @proj nvarchar(MAX) = N''
				SELECT
					UsuarioID, NomeUsuario, EmailUsuario, LoginUsuario, StatusUsuario,
					GrupoPaiID, NomeGrupoPai, GrupoFilhoID, NomeGrupoFilho, TodosGruposUsuario,
					PerfilNaTrilha, NomeAmbiente, AmbienteID, PontuacaoAmbiente, StatusUsuarioAmbiente''
					+ CASE WHEN ISNULL(@aliasSelect, N'''') = N'''' THEN N'''' ELSE N'', '' + @aliasSelect END + N''
				INTO ##RelatorioRotulado
				FROM #RelatorioColunado'';

				EXEC sp_executesql @proj;
				';

			/* ========= PIVOT DE ATRIBUTOS (se houver) ========= */

			IF EXISTS (SELECT 1 FROM ATRIBUTO (NOLOCK) WHERE ATRIBUTO.ID_CLIENTE = @clienteID AND ATRIBUTO.FL_STATUS = 1)
			BEGIN
				DECLARE @attributeNames NVARCHAR(MAX) = N'';
				SELECT @attributeNames =
				   STUFF((
					  SELECT N', ' + QUOTENAME(REPLACE(ATRIBUTO.TX_NOME, @clienteID + '_', ''))
					  FROM ATRIBUTO (NOLOCK)
					  WHERE ATRIBUTO.FL_STATUS = 1 AND ATRIBUTO.ID_CLIENTE = @clienteID
					  ORDER BY ATRIBUTO.ID
					  FOR XML PATH(''), TYPE
				   ).value('.','nvarchar(max)'), 1, 2, N'');

				-- materializa atributos em #TempAtributosUsuario
				SET @sql += N'
				IF OBJECT_ID(''tempdb..#TempAtributosUsuario'', ''U'') IS NOT NULL DROP TABLE #TempAtributosUsuario;
				SELECT UA.UsuarioId,
					   UA.ClienteId AS CustomerId,
					   REPLACE(A.TX_NOME, ''' + @clienteID + N'_'' , '''') AS Atributo,
					   CASE WHEN A.ID_ATRIBUTO_TIPO = ''4''
								THEN CONVERT(VARCHAR(10), TRY_CONVERT(DATETIME, UA.AtributoValor), 103)
							ELSE UA.AtributoValor
					   END AS AtributoValor
				INTO #TempAtributosUsuario
				FROM #UsuariosAdmin AS U
				JOIN fnt_AtributosUsuarios (''' + @clienteID + N''') AS UA
					 ON UA.UsuarioId = U.UsuarioId AND UA.ClienteId = U.ClienteId
				JOIN ATRIBUTO AS A (NOLOCK)
					 ON A.ID = UA.AtributoId AND A.ID_CLIENTE = UA.ClienteId AND UA.ClienteId = U.ClienteId;
				CREATE INDEX IX_TempAtrib_Usuario ON #TempAtributosUsuario (UsuarioId) INCLUDE (Atributo, AtributoValor);

				;WITH CTEA AS (
					SELECT R.*, TA.Atributo, TA.AtributoValor
					FROM ##RelatorioRotulado AS R
					LEFT JOIN #TempAtributosUsuario AS TA
					  ON TA.UsuarioId = R.UsuarioId
				)
				SELECT pvt.* 
				FROM CTEA
				PIVOT (MAX(AtributoValor) FOR Atributo IN (' + ISNULL(@attributeNames,N'') + N')) pvt
				OPTION (MAXDOP 1);
				';
				
			END
			ELSE
			BEGIN
				-- Sem atributos: apenas retorna a tabela pivotada e o mapa
				SET @sql += N'
				SELECT * FROM ##RelatorioRotulado OPTION (MAXDOP 1);
				';
			END

			-- Executa tudo em um batch só (garante escopo das #temp)
			EXEC sp_executesql @sql;

			DROP TABLE #RODADAS;
		END

GO

