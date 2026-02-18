USE [prod_my_engage_autosservico]
GO

/****** Object:  UserDefinedFunction [dbo].[fnt_TodosGruposUsuarios]    Script Date: 10/28/2025 10:00:58 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE  FUNCTION [dbo].[fnt_TodosGruposUsuarios] (@usuarioId INT, @clienteID NVARCHAR(24), @gruposId NVARCHAR(MAX) = NULL)
RETURNS TABLE
AS  RETURN 
--DECLARE @usuarioId INT =(SELECT ID FROM USUARIO WHERE TX_LOGIN = 'maria.marketing' AND ID_CLIENTE = 'relatorioric') ,
--@clienteID NVARCHAR(24) = 'relatorioric', 
--@gruposId NVARCHAR(255) = '83070';
	WITH CTE_GRUPOS 
		  AS 
		  (
			SELECT CAST(grupos.value AS INT) AS ID_GRUPO,
					@clienteID AS ID_CLIENTE
			  FROM STRING_SPLIT(@gruposId, ',') AS grupos
			 WHERE TRY_CAST (grupos.value as INT) IS NOT NULL
		  ),
		  --select * from GRUPO where ID  IN (SELECT ID_GRUPO FROM CTE_GRUPOS)
	     CTE
		  AS
		  (
					SELECT UG.ID_USUARIO
						 , UG.ID_CLIENTE
						 , UG.ID_GRUPO
						 , GR.TX_NOME AS NomeGrupo
						 , GR.ID_GRUPO_PAI
						 , CAG.NU_SALTOS
						 , GRP_HIERARCHY.DEPTH
						 , ROW_NUMBER() OVER (PARTITION BY UG.ID_GRUPO, UG.ID_CLIENTE ORDER BY CAG.NU_SALTOS ASC, GRP_HIERARCHY.DEPTH ASC) AS RN
				      FROM USUARIO_GRUPO UG (NOLOCK)
					  JOIN GRUPO GR (NOLOCK)
					    ON GR.ID = UG.ID_GRUPO
					   AND GR.ID_CLIENTE = UG.ID_CLIENTE
					   AND GR.FL_STATUS = 1
				-- Pegando a hierarquia completa dos grupos a partir da CACHE_ARVORE_GRUPO
				 LEFT JOIN CACHE_ARVORE_GRUPO CAG (NOLOCK)
				        ON CAG.ID_GRUPO = GR.ID
					   AND CAG.ID_CLIENTE = GR.ID_CLIENTE
				-- Subquery para determinar a profundidade da hierarquia
			   OUTER APPLY (SELECT COUNT(*) AS DEPTH
							  FROM CACHE_ARVORE_GRUPO CAG_HIER (NOLOCK)
							 WHERE CAG_HIER.ID_GRUPO = CAG.ID_GRUPO
							   AND CAG_HIER.ID_CLIENTE = CAG.ID_CLIENTE) AS GRP_HIERARCHY
				     WHERE UG.ID_USUARIO = @usuarioId
					   AND UG.ID_CLIENTE = @clienteID
					   AND (ISNULL(@gruposId, '') = '' OR  UG.ID_GRUPO IN ((SELECT ID_GRUPO FROM CTE_GRUPOS)))
		  ) SELECT STRING_AGG(CAST(NomeGrupo AS NVARCHAR(MAX)), N', ') WITHIN GROUP (ORDER BY NU_SALTOS ASC, DEPTH ASC, ID_GRUPO_PAI) AS TodosGruposUsuario
			  FROM CTE
			 WHERE RN = 1
/*
		DECLARE @clienteID NVARCHAR(24) = N'rtintelligence';
		DECLARE @usuarioId INT = (SELECT U.ID FROM USUARIO AS U WHERE U.ID_CLIENTE = @clienteId AND U.TX_LOGIN = 'ricardo.carvalho@engage.bz')
		SELECT USUARIO.ID AS UsuarioId
			 , USUARIO.ID_CLIENTE AS CienteID
		     , USUARIO.TX_NOME_COMPLETO AS NomeUsuario
		     , USUARIO.TX_LOGIN AS LoginUsuario
		     , USUARIO.TX_EMAIL AS EmailUsuario
			 , IIF (USUARIO.FL_STATUS = 1, 'Ativo', 'Inativo') AS [StatusUsuario]
			 , Grupos.TodosGruposUsuario
		  FROM USUARIO
   CROSS APPLY fnt_TodosGruposUsuarios (USUARIO.ID, USUARIO.ID_CLIENTE) AS Grupos
		 WHERE USUARIO.ID_CLIENTE = @clienteId
		   AND USUARIO.FL_PADRAO = 0
		   AND USUARIO.ID = @usuarioId

*/

GO

