USE [prod_my_engage_autosservico]
GO

/****** Object:  UserDefinedFunction [dbo].[fnt_Configuracoes]    Script Date: 10/28/2025 9:59:59 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[fnt_Configuracoes] (@clienteID NVARCHAR (24))
RETURNS TABLE
AS RETURN
		SELECT CONFIGURACAO.ID AS ConfiguracaoID
			 , ISNULL(ConfiguracaoCliente.ID_CLIENTE, @clienteID) AS ClienteID
			 , Configuracao.TX_DESCRICAO AS Descricao
			 , ISNULL(ConfiguracaoCliente.TX_ARQUIVO, Configuracao.TX_ARQUIVO) AS Arquivo
			 , ISNULL(ConfiguracaoCliente.FL_STATUS, Configuracao.FL_STATUS) AS [Status]
			 , ISNULL(ConfiguracaoCliente.FL_VISIVEL, Configuracao.FL_VISIVEL) AS Visivel
			 , ISNULL(CONVERT(BIT, IIF (ConfiguracaoCliente.ID_CONFIGURACAO IS NULL, 1, 0)), 0) AS EstaUtilizandoConfiguracaoPadrao
	         , '' AS TemaCliente --Cliente.TX_TEMA As TemaCliente
			 , TX_GRUPO AS RotuloGrupo
			 , NU_ORDEM AS OrdemConfiguracao
			 , TX_AJUDA AS RotuloAjuda
			 , NU_ORDEM_GRUPO AS OrdemGrupo
			 , NU_ORDEM_CONFIG AS OrdemConfig
			 , NU_ORDEM_GRUPO_CONFIG AS OrdemGrupoConfig
			 , NU_TAB_CONFIG AS TabConfig
			 , TX_GRUPO_CONFIG AS GrupoConfig
		  FROM CONFIGURACAO AS Configuracao (NOLOCK)
	 LEFT JOIN CONFIGURACAO_CLIENTE AS ConfiguracaoCliente (NOLOCK)			
		    ON ConfiguracaoCliente.ID_CONFIGURACAO = Configuracao.ID
		   AND ConfiguracaoCliente.ID_CLIENTE = @clienteID
GO

