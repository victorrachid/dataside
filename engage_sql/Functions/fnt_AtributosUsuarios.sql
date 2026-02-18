USE [prod_my_engage_autosservico]
GO

/****** Object:  UserDefinedFunction [dbo].[fnt_AtributosUsuarios]    Script Date: 10/28/2025 10:01:19 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- use [dev.engage.bz]
-- use [test.engage.bz]
-- use [pre.engage.bz]
CREATE FUNCTION [dbo].[fnt_AtributosUsuarios] (@clienteId NVARCHAR(50))
RETURNS TABLE
AS RETURN WITH CTE		
		  AS
		  (
				SELECT USUARIO.ID AS UsuarioId
					 , USUARIO.ID_CLIENTE AS ClienteId
					 , Atributo.ID AS AtributoId
					 , atributo.fl_status
					 , LTRIM(RTRIM(REPLACE(Atributo.TX_NOME, @clienteId + '_', ''))) AS Atributo
					 , AtributoGrupoItem.NU_ORDEM AS Ordem
					 , EntidadeAtributo.*
					 , ROW_NUMBER() OVER (PARTITION BY EntidadeAtributo.ID_ATRIBUTO
													 , EntidadeAtributo.ID_CLIENTE
													 , EntidadeAtributo.ID_ENTIDADE
													 , EntidadeAtributo.ID_ENTIDADE_TIPO
											  ORDER BY EntidadeAtributo.ID DESC) AS RN
					 , CASE WHEN AtributoTipo.TX_NOME IN('TEXT', 'PHONE', 'INTEGER', 'DECIMAL')		THEN CAST(LTRIM(RTRIM(CAST(EntidadeAtributo.TX_VALOR AS NVARCHAR(4000)))) AS sql_variant)
							WHEN AtributoTipo.TX_NOME IN('SELECT', 'MULTIPLE')	THEN CAST(LTRIM(RTRIM(CAST(AtributoValorNome.Texto AS NVARCHAR(4000)))) AS sql_variant)
							--WHEN AtributoTipo.TX_NOME = 'INTEGER'				THEN CAST(EntidadeAtributo.NU_INTEIRO	AS SQL_VARIANT)
							--WHEN AtributoTipo.TX_NOME = 'DECIMAL'				THEN CAST(EntidadeAtributo.NU_DECIMAL	AS SQL_VARIANT)
							WHEN AtributoTipo.TX_NOME = 'DATE'					THEN CAST(EntidadeAtributo.DT_VALOR		AS SQL_VARIANT)
							WHEN AtributoTipo.TX_NOME = 'TIME'					THEN CAST(EntidadeAtributo.HR_VALOR		AS SQL_VARIANT)
						END AS Valor
				  FROM USUARIO (NOLOCK)
			 LEFT JOIN (ATRIBUTO AS Atributo (NOLOCK)
						JOIN ATRIBUTO_TIPO AS AtributoTipo (NOLOCK)
						  ON AtributoTipo.Id = Atributo.ID_ATRIBUTO_TIPO
					    JOIN ATRIBUTO_GRUPO_ITEM AS AtributoGrupoItem (NOLOCK)
						  ON AtributoGrupoItem.ID_ATRIBUTO = Atributo.ID
					     AND AtributoGrupoItem.ID_CLIENTE = Atributo.ID_CLIENTE
					    JOIN ENTIDADE_ATRIBUTO AS EntidadeAtributo (NOLOCK)
						  ON EntidadeAtributo.ID_ATRIBUTO = Atributo.ID
					     AND EntidadeAtributo.ID_CLIENTE = Atributo.ID_CLIENTE
				   LEFT JOIN (ATRIBUTO_VALOR AS AtributoValor (NOLOCK)
							  CROSS APPLY fnt_RotuloIdiomaPorId (AtributoValor.ID_ROTULO_VALOR, AtributoValor.ID_CLIENTE, 'pt-BR') AS AtributoValorNome)
						  ON AtributoValor.ID = EntidadeAtributo.ID_ATRIBUTO_VALOR
						 AND AtributoValor.ID_CLIENTE = EntidadeAtributo.ID_CLIENTE)
					ON Usuario.ID = EntidadeAtributo.ID_ENTIDADE
				   AND Usuario.ID_CLIENTE = EntidadeAtributo.ID_CLIENTE
				   AND EntidadeAtributo.ID_ENTIDADE_TIPO = 'US'
				 WHERE USUARIO.ID_CLIENTE = @clienteId
				   AND Atributo.FL_STATUS = 1
				   AND Atributo.ID_CLIENTE = @clienteID
	      ) -- SELECT * FROM CTE
	      , AtributosUsuario
		  AS
		  (
				SELECT CTE.UsuarioId
					 , CTE.ClienteId
					 , CTE.Atributo
					 , CTE.Valor AS AtributoValor
					 , CTE.AtributoId
					 , CTE.ID_ATRIBUTO_VALOR AS AtributoValorId
				  FROM CTE
				 WHERE (CTE.RN = 1 OR Atributo IS NULL)
				 and cte.fl_status =1
		   ) SELECT *  FROM AtributosUsuario

/*
	SELECT * FROM fnt_AtributosUsuarios ('b3integracaoprestadores')


*/
GO

