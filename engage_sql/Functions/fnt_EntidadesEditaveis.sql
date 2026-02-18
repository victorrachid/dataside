USE [prod_my_engage_autosservico]
GO

/****** Object:  UserDefinedFunction [dbo].[fnt_EntidadesEditaveis]    Script Date: 10/28/2025 9:58:49 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- use [dev.engage.bz]
-- use [test.engage.bz]
-- use [pre.engage.bz]
CREATE FUNCTION [dbo].[fnt_EntidadesEditaveis] (@adminId INT, @clienteId NVARCHAR(24), @entidadeTipoID CHAR(2), @grupos SubGrupo READONLY) RETURNS TABLE
AS RETURN 
		-- SELECT * FROM PERFIL_GRUPO_ACESSO_TIPO
		-- SELECT * FROM USUARIO WHERE ID_CLIENTE = 'hondavendas' AND TX_LOGIN = 'admin'
		-- DECLARE @adminId INT = 100406, @clienteId NVARCHAR(24) = N'hondavendas', @entidadeTipoID CHAR(2) = 'US', @grupos SubGrupo;

			WITH PerfilUsuarioLogado
			AS
			(
					 -- Pega o perfil do usuário de acordo com o nível de "poder" de cada um deles, do mais poderoso para o menos poderoso.
					SELECT TOP 1
						   Perfil.ID AS PerfilId
					     , Perfil.TX_NOME AS Perfil
						 , Perfil.ID_CLIENTE AS ClienteId
						 , Perfil.ID_PERFIL_GRUPO_ACESSO_TIPO AS PerfilGrupoAcessoTipoId
						 , PodeVisualizarEntidadesSemGrupos.PermitirVisualizarEntidadesSemGrupos
					  FROM PERFIL_ADMIN AS Perfil (NOLOCK)
					  JOIN PERFIL_USUARIO AS PerfilUsuario (NOLOCK)
					    ON PerfilUsuario.ID_PERFIL = Perfil.ID
					   AND PerfilUsuario.ID_CLIENTE = Perfil.ID_CLIENTE
			   CROSS APPLY (SELECT MAX(CONVERT(INT, TempPerfil.FL_PERMITIR_VISUALIZAR_ENTIDADES_SEM_GRUPO)) AS PermitirVisualizarEntidadesSemGrupos
							  FROM PERFIL_USUARIO TempPerfilUsuario (NOLOCK)
							  JOIN PERFIL_ADMIN AS TempPerfil (NOLOCK)
							    ON TempPerfilUsuario.ID_PERFIL = TempPerfil.ID
							   AND TempPerfilUsuario.ID_CLIENTE = TempPerfil.ID_CLIENTE
							 WHERE TempPerfilUsuario.ID_USUARIO = PerfilUsuario.ID_USUARIO
							   AND TempPerfilUsuario.ID_CLIENTE = PerfilUsuario.ID_CLIENTE) AS PodeVisualizarEntidadesSemGrupos
				     WHERE Perfil.FL_STATUS = 1
					   AND Perfil.ID_CLIENTE = @clienteId
					   AND PerfilUsuario.ID_USUARIO = @adminId
					   AND PerfilUsuario.ID_CLIENTE = @clienteId
			 ORDER BY CASE WHEN Perfil.ID_PERFIL_GRUPO_ACESSO_TIPO = 'any_group' THEN 1
						   WHEN Perfil.ID_PERFIL_GRUPO_ACESSO_TIPO = 'group_structure_top_down' THEN 2
						   WHEN Perfil.ID_PERFIL_GRUPO_ACESSO_TIPO = 'specific_groups' THEN 3
						   ELSE 4
					   END 
			) -- SELECT * FROM PerfilUsuarioLogado
			, EntidadesDeAcesso
			AS
			(
					SELECT EntidadeGrupo.ID_ENTIDADE_TIPO AS EntidadeTipoId
						 , EntidadeGrupo.ID_ENTIDADE AS EntidadeId
						 , EntidadeGrupo.ID_CLIENTE AS ClienteID
						 , CONVERT(BIT, 1) AS Vinculado
					  FROM ENTIDADE_GRUPO AS EntidadeGrupo (NOLOCK)
					  JOIN PerfilUsuarioLogado
					    ON PerfilUsuarioLogado.PerfilGrupoAcessoTipoId = 'any_group' -- Aqui é equivalente a super usuários.
					  JOIN GRUPO AS Grupo (NOLOCK)
						ON Grupo.ID = EntidadeGrupo.ID_GRUPO
					   AND Grupo.ID_CLIENTE = EntidadeGrupo.ID_CLIENTE
					   AND Grupo.FL_STATUS = 1
					 WHERE EntidadeGrupo.ID_ENTIDADE_TIPO = @entidadeTipoID
					   AND EntidadeGrupo.ID_CLIENTE = @clienteId
				  GROUP BY EntidadeGrupo.ID_ENTIDADE_TIPO
						 , EntidadeGrupo.ID_ENTIDADE
						 , EntidadeGrupo.ID_CLIENTE
						 
					 UNION ALL

					SELECT EntidadeGrupo.ID_ENTIDADE_TIPO AS EntidadeTipoId
						 , EntidadeGrupo.ID_ENTIDADE AS EntidadeId
						 , EntidadeGrupo.ID_CLIENTE AS ClienteID
						 , CONVERT(BIT, 1) AS Vinculado
					  FROM ENTIDADE_GRUPO AS EntidadeGrupo (NOLOCK)
					  JOIN GRUPO AS Grupo (NOLOCK)
						ON Grupo.ID = EntidadeGrupo.ID_GRUPO
					   AND Grupo.ID_CLIENTE = EntidadeGrupo.ID_CLIENTE
					   AND Grupo.FL_STATUS = 1
					 WHERE EntidadeGrupo.ID_ENTIDADE_TIPO = @entidadeTipoId
					   AND EntidadeGrupo.ID_CLIENTE = @clienteId
					   AND EXISTS (SELECT *
									 FROM CACHE_ARVORE_GRUPO AS CacheArvoreGrupo (NOLOCK)
									 JOIN PerfilUsuarioLogado
									   ON PerfilUsuarioLogado.PerfilGrupoAcessoTipoId = 'group_structure_top_down'  -- Aqui, vale sempre a regra pela hierarquia TOP DOWN
									 JOIN ENTIDADE_GRUPO AS AdminGrupo (NOLOCK)
									   ON AdminGrupo.ID_GRUPO = CacheArvoreGrupo.ID_ROOT
									  AND AdminGrupo.ID_CLIENTE = CacheArvoreGrupo.ID_CLIENTE
									  AND AdminGrupo.ID_ENTIDADE = @adminId
									  AND AdminGrupo.ID_CLIENTE = @clienteId
									  AND AdminGrupo.ID_ENTIDADE_TIPO = 'US'
									WHERE EntidadeGrupo.ID_GRUPO = CacheArvoreGrupo.ID_GRUPO
									  AND EntidadeGrupo.ID_CLIENTE = CacheArvoreGrupo.ID_CLIENTE)
				  GROUP BY EntidadeGrupo.ID_ENTIDADE_TIPO
						 , EntidadeGrupo.ID_ENTIDADE
						 , EntidadeGrupo.ID_CLIENTE
					
					 UNION ALL
	
					-- Aqui, vale sempre a regra pela hierarquia TOP DOWN, mas com base nos grupos cadastrados na tabela 
					SELECT EntidadeGrupo.ID_ENTIDADE_TIPO AS EntidadeTipoId
						 , EntidadeGrupo.ID_ENTIDADE AS EntidadeId
						 , EntidadeGrupo.ID_CLIENTE AS ClienteID
						 , CONVERT(BIT, 1) AS Vinculado
					  FROM ENTIDADE_GRUPO AS EntidadeGrupo (NOLOCK)
					  JOIN GRUPO AS Grupo (NOLOCK)
						ON Grupo.ID = EntidadeGrupo.ID_GRUPO
					   AND Grupo.ID_CLIENTE = EntidadeGrupo.ID_CLIENTE
					   AND Grupo.FL_STATUS = 1
					 WHERE EntidadeGrupo.ID_ENTIDADE_TIPO = @entidadeTipoId
					   AND EntidadeGrupo.ID_CLIENTE = @clienteId
					   AND EXISTS (SELECT PerfilGrupoAcesso.ID_GRUPO
								     FROM PERFIL_ADMIN AS Perfil (NOLOCK)
								     JOIN PERFIL_USUARIO AS PerfilUsuario (NOLOCK)
									   ON PerfilUsuario.ID_PERFIL = Perfil.ID
								      AND PerfilUsuario.ID_CLIENTE = Perfil.ID_CLIENTE
									 JOIN PERFIL_GRUPO_ACESSO AS PerfilGrupoAcesso (NOLOCK)
									   ON PerfilGrupoAcesso.ID_PERFIL = Perfil.ID
									  AND PerfilGrupoAcesso.ID_CLIENTE = Perfil.ID_CLIENTE
									 JOIN PerfilUsuarioLogado
									   ON PerfilUsuarioLogado.PerfilGrupoAcessoTipoId = 'specific_groups'									  
								    WHERE Perfil.FL_STATUS = 1
								      AND Perfil.ID_CLIENTE = @clienteId
								      AND PerfilUsuario.ID_USUARIO = @adminId
								      AND PerfilUsuario.ID_CLIENTE = @clienteId
									  AND PerfilGrupoAcesso.ID_CLIENTE = EntidadeGrupo.ID_CLIENTE
								      AND PerfilGrupoAcesso.ID_GRUPO = EntidadeGrupo.ID_GRUPO)
					   AND ((@entidadeTipoID = 'CP' AND EXISTS (SELECT *
																  FROM (SELECT UPE.ID_CLIENTE
																             , UPE.ID_PERFIL_JOGO
																			 , UPE.ID_USUARIO
																			 , RD.ID_TRILHA
																          FROM USUARIO_PERFIL_ENTIDADE AS UPE (NOLOCK)
																		  JOIN RODADA AS RD (NOLOCK)
																		    ON UPE.ID_CLIENTE = RD.ID_CLIENTE
																		   AND UPE.ID_ENTIDADE = RD.ID
																		   AND UPE.ID_ENTIDADE_TIPO = 'RD'
																		   AND UPE.ID_PERFIL_JOGO IN (1, 2, 3)
																		   AND RD.FL_STATUS = 1) AS UsuarioPerfilTrilha
																  JOIN COMPETICAO_TRILHA CT (NOLOCK)
																    ON UsuarioPerfilTrilha.ID_CLIENTE = CT.ID_CLIENTE
																   AND UsuarioPerfilTrilha.ID_TRILHA = CT.ID_TRILHA
																   --AND UsuarioPerfilTrilha.ID_PERFIL_JOGO = 3 Regra comentada 20-02-25 ticket 89120. Permitindo editar ambientes independente do perfil
																 WHERE UsuarioPerfilTrilha.ID_USUARIO = @adminId
																   AND UsuarioPerfilTrilha.ID_CLIENTE = @clienteId
																   AND CT.ID_COMPETICAO = EntidadeGrupo.ID_ENTIDADE
																   AND CT.ID_CLIENTE = EntidadeGrupo.ID_CLIENTE)) OR @entidadeTipoID NOT IN('CP'))
				  GROUP BY EntidadeGrupo.ID_ENTIDADE_TIPO
						 , EntidadeGrupo.ID_ENTIDADE
						 , EntidadeGrupo.ID_CLIENTE
					
					

				     UNION ALL

				  -- Para tratar exceção, estou fazendo com que os usuários que não estejam vinculados a nenhum grupo sejam retornados pelas procs quando o tiver perfil de ADMIN, caso contrário eles não aparecerão no sistema
				    SELECT 'US' AS EntidadeTipoId
						 , Usuario.ID AS EntidadeId
						 , Usuario.ID_CLIENTE AS ClienteId
						 , CONVERT(BIT, 0) AS Vinculado
				      FROM USUARIO (NOLOCK)
				     WHERE @entidadeTipoID = 'US'
				       AND Usuario.ID_CLIENTE = @clienteId
					   AND Usuario.FL_PADRAO = 0
					   AND Usuario.ID NOT IN(SELECT vUsuarioGrupo.UsuarioID FROM vUsuarioGrupo (NOEXPAND) WHERE vUsuarioGrupo.ClienteID = @clienteId)
					   AND EXISTS (SELECT *
									 FROM PerfilUsuarioLogado
								    WHERE PerfilUsuarioLogado.PermitirVisualizarEntidadesSemGrupos = 1 -- Só retorna os usuário sem grupos se o usuário logado tiver perfil de admin, que seja superadmin.		
								   )

					 UNION ALL
					
					-- Para tratar exceção, estou fazendo com que os competições que não estejam vinculados a nenhum grupo sejam retornadas pelas procs quando o tiver perfil de ADMIN, caso contrário eles não aparecerão no sistema
				    SELECT 'CP' AS EntidadeTipoId
						 , Competicao.ID AS EntidadeId
						 , Competicao.ID_CLIENTE AS ClienteId
						 , CONVERT(BIT, 0) AS Vinculado
				      FROM COMPETICAO AS Competicao
					  JOIN PerfilUsuarioLogado
						ON PerfilUsuarioLogado.PermitirVisualizarEntidadesSemGrupos = 1 -- Só retorna as competições sem grupos se o usuário logado tiver perfil de admin
				     WHERE @entidadeTipoID = 'CP'
				       AND Competicao.ID_CLIENTE = @clienteId
					   AND NOT EXISTS (SELECT *
										 FROM COMPETICAO_GRUPO AS CG (NOLOCK)
										 JOIN GRUPO (NOLOCK)
										   ON GRUPO.ID = CG.ID_GRUPO
										  AND GRUPO.ID_CLIENTE = CG.ID_CLIENTE
										  AND Grupo.FL_STATUS = 1
										WHERE CG.ID_CLIENTE = Competicao.ID_CLIENTE
										  AND CG.ID_COMPETICAO = Competicao.ID
										  AND @entidadeTipoID = 'CP')
					   

					UNION ALL
					
					-- Para tratar exceção, estou fazendo com que os atividades que não estejam vinculados a nenhum grupo sejam retornadas pelas procs quando o tiver perfil de ADMIN, caso contrário eles não aparecerão no sistema
				    SELECT 'AT' AS EntidadeTipoId
						 , Atividade.ID AS EntidadeId
						 , Atividade.ID_CLIENTE AS ClienteId
						 , CONVERT(BIT, 0) AS Vinculado
				      FROM ATIVIDADE AS Atividade
					  JOIN PerfilUsuarioLogado
					    ON PerfilUsuarioLogado.PermitirVisualizarEntidadesSemGrupos = 1  -- Só retorna as atividades sem grupos se o usuário logado tiver perfil de admin
				     WHERE @entidadeTipoID = 'AT'
				       AND Atividade.ID_CLIENTE = @clienteId
					   AND NOT EXISTS (SELECT *
										 FROM ENTIDADE_GRUPO AS EntidadeGrupo (NOLOCK)
										 JOIN GRUPO (NOLOCK)
										   ON GRUPO.ID = EntidadeGrupo.ID_GRUPO
										  AND GRUPO.ID_CLIENTE = EntidadeGrupo.ID_CLIENTE
										  AND Grupo.FL_STATUS = 1
										WHERE EntidadeGrupo.ID_ENTIDADE_TIPO = 'AT'
										  AND EntidadeGrupo.ID_CLIENTE = @clienteId
										  AND EntidadeGrupo.ID_ENTIDADE = Atividade.Id
										  AND EntidadeGrupo.ID_CLIENTE = Atividade.ID_CLIENTE
										  AND @entidadeTipoID = 'AT')

					 UNION ALL

					-- Para tratar exceção, estou fazendo com que os conquista que não estejam vinculados a nenhum grupo sejam retornadas pelas procs quando o tiver perfil de ADMIN, caso contrário eles não aparecerão no sistema
				    SELECT 'CQ' AS EntidadeTipoId
						 , Conquista.ID AS EntidadeId
						 , Conquista.ID_CLIENTE AS ClienteId
						 , CONVERT(BIT, 0) AS Vinculado
				      FROM CONQUISTA AS Conquista
					  JOIN PerfilUsuarioLogado
						ON PerfilUsuarioLogado.PermitirVisualizarEntidadesSemGrupos = 1 -- Só retorna as conquita sem grupos se o usuário logado tiver perfil de admin
				     WHERE @entidadeTipoID = 'CQ'
				       AND Conquista.ID_CLIENTE = @clienteId
					   AND NOT EXISTS (SELECT *
										 FROM ENTIDADE_GRUPO AS EntidadeGrupo (NOLOCK)
										 JOIN GRUPO (NOLOCK)
										   ON GRUPO.ID = EntidadeGrupo.ID_GRUPO
										  AND GRUPO.ID_CLIENTE = EntidadeGrupo.ID_CLIENTE
										  AND Grupo.FL_STATUS = 1
										WHERE EntidadeGrupo.ID_ENTIDADE_TIPO = 'CQ'
										  AND EntidadeGrupo.ID_ENTIDADE = Conquista.Id
										  AND EntidadeGrupo.ID_CLIENTE = Conquista.ID_CLIENTE										  
										  AND @entidadeTipoID = 'CQ')

				
			)
			, Resultado
			AS 
			(
				SELECT EntidadesDeAcesso.*
					 , ROW_NUMBER() OVER (PARTITION BY EntidadeId, ClienteId ORDER BY CASE WHEN Vinculado = 1 THEN 1 ELSE 2 END ASC) AS RN
			      FROM EntidadesDeAcesso
			) SELECT *
				FROM Resultado
			   WHERE RN = 1




-- SELECT * FROM COMPETICAO WHERE ID_CLIENTE = 'arcelormittal' and fl_disponivel = 1




/*
			WITH ArvoreGrupos
			AS 
			(
					-- Retorna a hierarquia de grupos do admin logado com base em seus grupos de acesso.
					SELECT CacheArvoreGrupo.*
					  FROM CACHE_ARVORE_GRUPO AS CacheArvoreGrupo (NOLOCK)
					 WHERE EXISTS (SELECT *
									 FROM USUARIO_GRUPO AS UsuarioGrupo (NOLOCK)
									WHERE UsuarioGrupo.ID_GRUPO = CacheArvoreGrupo.ID_ROOT
									  AND UsuarioGrupo.ID_CLIENTE = CacheArvoreGrupo.ID_CLIENTE
									  AND UsuarioGrupo.ID_USUARIO = @usuarioId
									  AND UsuarioGrupo.ID_CLIENTE = @clienteId)					   
			) -- SELECT * FROM ArvoreGrupos
			, EntidadeFiltradas
			AS
			(
					SELECT EntidadeGrupo.ID_ENTIDADE_TIPO AS EntidadeTipoId
						 --, EntidadeGrupo.ID_ENTIDADE AS EntidadeId
						 , EntidadeGrupo.ID_CLIENTE AS ClienteId
					  FROM ArvoreGrupos
					  JOIN ENTIDADE_GRUPO AS EntidadeGrupo (NOLOCK)
					    ON EntidadeGrupo.ID_CLIENTE = ArvoreGrupos.ID_CLIENTE
					   AND EntidadeGrupo.ID_GRUPO = ArvoreGrupos.ID_GRUPO
					   AND EntidadeGrupo.ID_ENTIDADE_TIPO = @entidadeTipoID
				  GROUP BY EntidadeGrupo.ID_ENTIDADE_TIPO
						 --, EntidadeGrupo.ID_ENTIDADE
						 , EntidadeGrupo.ID_CLIENTE
			) SELECT *
				FROM EntidadeFiltradas
 */
  
/*
			SELECT EntidadeGrupo.ID_ENTIDADE AS EntidadeId
				 , EntidadeGrupo.ID_ENTIDADE_TIPO AS EntidadeTipoId
				 , EntidadeGrupo.ID_CLIENTE AS ClienteId
				 --, MAX(CASE EntidadeGrupo.FL_GRUPO_PRINCIPAL WHEN 1 THEN EntidadeGrupo.ID_GRUPO END) AS GrupoPrincipalId
			  FROM ENTIDADE_GRUPO AS EntidadeGrupo (NOLOCK)
			  JOIN GRUPO AS Grupo (NOLOCK)
			    ON EntidadeGrupo.ID_GRUPO = Grupo.ID
			   AND EntidadeGrupo.ID_CLIENTE = Grupo.ID_CLIENTE
		 LEFT JOIN (CACHE_ARVORE_GRUPO AS CacheArvoreGrupo (NOLOCK)
					JOIN USUARIO_GRUPO AS UsuarioGrupo (NOLOCK) 
					  ON UsuarioGrupo.ID_USUARIO = @usuarioID
					 AND UsuarioGrupo.ID_CLIENTE = @clienteId
					 AND UsuarioGrupo.ID_GRUPO = CacheArvoreGrupo.ID_PAI
					 AND UsuarioGrupo.ID_CLIENTE = CacheArvoreGrupo.ID_CLIENTE
			   LEFT JOIN (@grupos AS Grupos
						  JOIN CACHE_ARVORE_GRUPO AS GrupoFiltro (NOLOCK)
						    ON Grupos.GrupoId = GrupoFiltro.ID_PAI AND Grupos.ClienteId = GrupoFiltro.ID_CLIENTE AND (Grupos.IncluirSubGrupos = 1 OR GrupoFiltro.NU_SALTOS = 0))
					  ON GrupoFiltro.ID_GRUPO = CacheArvoreGrupo.ID_GRUPO AND GrupoFiltro.ID_CLIENTE = CacheArvoreGrupo.ID_CLIENTE)
			    ON EntidadeGrupo.ID_GRUPO = CacheArvoreGrupo.ID_GRUPO
			   AND EntidadeGrupo.ID_CLIENTE = CacheArvoreGrupo.ID_CLIENTE
		     WHERE EntidadeGrupo.ID_ENTIDADE_TIPO = @entidadeTipoID
		  GROUP BY EntidadeGrupo.ID_ENTIDADE
				 , EntidadeGrupo.ID_ENTIDADE_TIPO
				 , EntidadeGrupo.ID_CLIENTE
			HAVING COUNT(*) = COUNT(CacheArvoreGrupo.ID_GRUPO)
		AND (COUNT(Grupos.GrupoId) > 0 OR NOT EXISTS(SELECT * FROM @grupos))
	--SELECT @entidadeTipoID AS EntidadeTipoId
*/


/*
	  SELECT EntidadeDominio.EntidadeID
			, MAX(CASE EntidadeDominio.DominioPrincipal WHEN 1 THEN EntidadeDominio.DominioID END) AS DominioID
		FROM EntidadeDominio (NOLOCK)
			JOIN Dominio (NOLOCK)
			  ON EntidadeDominio.DominioID = Dominio.ID
			LEFT JOIN (CacheArvoreDominio (NOLOCK)
			JOIN EntidadeDominio (NOLOCK) UsuarioDominio
			  ON UsuarioDominio.EntidadeTipoID = 'U' 
			 AND UsuarioDominio.EntidadeID = @usuarioID
			 AND UsuarioDominio.DominioID = CacheArvoreDominio.PaiID
	   LEFT JOIN (@dominios Dominios
			JOIN CacheArvoreDominio DominioFiltro (NOLOCK)
			  ON Dominios.DominioID = DominioFiltro.PaiID AND (Dominios.IncluirSubDominios = 1 OR DominioFiltro.Saltos = 0))
			  ON DominioFiltro.FilhoID = CacheArvoreDominio.FilhoID)
			  ON EntidadeDominio.DominioID = CacheArvoreDominio.FilhoID
		WHERE EntidadeDominio.EntidadeTipoID = @entidadeTipoID AND (@exibirDominiosInativos = 1 OR Dominio.Ativo = 1)
		GROUP BY EntidadeDominio.EntidadeID
		HAVING COUNT(*) = COUNT(CacheArvoreDominio.FilhoID)
		AND (COUNT(Dominios.DominioID) > 0 OR NOT EXISTS(SELECT * FROM @dominios))

*/
GO

