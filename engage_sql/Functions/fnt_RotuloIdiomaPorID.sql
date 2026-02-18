USE [prod_my_engage_autosservico]
GO

/****** Object:  UserDefinedFunction [dbo].[fnt_RotuloIdiomaPorID]    Script Date: 10/28/2025 9:59:38 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- use [dev.engage.bz]
-- use [test.engage.bz]
-- use [pre.engage.bz]
-- ===================================================================================================================================
-- Esta função não pode ler por padrão do customerId=engage, porque ela também retorna rótulos que são especificos de uma instância (EX: Os skills).
-- Por esta razão que a tabela ROTULO_IDIOMA NÃO pode ser eliminada.
-- ===================================================================================================================================
CREATE FUNCTION [dbo].[fnt_RotuloIdiomaPorID] (@rotuloID INT, @clienteID NVARCHAR(24), @idiomaID VARCHAR(12))
RETURNS TABLE
AS RETURN 
	WITH CTE
	AS
	(

			SELECT Rotulo.ID AS RotuloID
				 , Rotulo.TX_CHAVE AS Chave
				 , RotuloIdioma.ID_IDIOMA AS IdiomaID
				 , ISNULL(RotuloIdiomaCliente.TX_TEXTO, RotuloIdioma.TX_TEXTO) AS Texto
				 , ROW_NUMBER()OVER(PARTITION BY RotuloIdioma.ID_ROTULO
										ORDER BY CASE WHEN ISNULL(RotuloIdiomaCliente.ID_IDIOMA, RotuloIdioma.ID_IDIOMA) = @idiomaID THEN 1
													  WHEN ISNULL(RotuloIdiomaCliente.ID_IDIOMA, RotuloIdioma.ID_IDIOMA) LIKE @idiomaID + '%' THEN 2
													  --WHEN ISNULL(RotuloIdiomaCliente.ID_IDIOMA, RotuloIdioma.ID_IDIOMA) = CulturaPadrao.Arquivo THEN 3
													  ELSE 4 END, RotuloIdioma.ID_IDIOMA) AS RowNumber
				 , Rotulo.FL_PADRAO AS IsDefault
			  FROM Rotulo (NOLOCK)
			  JOIN ROTULO_IDIOMA AS RotuloIdioma (NOLOCK)
				ON RotuloIdioma.ID_ROTULO = Rotulo.ID
		 LEFT JOIN ROTULO_IDIOMA_CLIENTE AS RotuloIdiomaCliente (NOLOCK)
				ON RotuloIdiomaCliente.ID_ROTULO = RotuloIdioma.ID_ROTULO
			   AND RotuloIdiomaCliente.ID_IDIOMA = RotuloIdioma.ID_IDIOMA
			   AND RotuloIdiomaCliente.ID_CLIENTE = @clienteID
			 WHERE Rotulo.ID = @rotuloID
	) SELECT RotuloID
		   , Chave
		   , IdiomaID
		   , Texto
		  -- , RowNumber
		   , IsDefault
	    FROM CTE
	   WHERE RowNumber = 1


/*
	DECLARE @rotuloId INT = 956, @clienteID NVARCHAR(24) = 'totvs', @idiomaId NVARCHAR(20) = 'es'
	SELECT *
	  FROM fnt_RotuloIdiomaPorID (@rotuloId, @clienteID, @idiomaId)

*/
GO

