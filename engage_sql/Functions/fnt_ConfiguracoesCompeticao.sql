USE [prod_my_engage_autosservico]
GO

/****** Object:  UserDefinedFunction [dbo].[fnt_ConfiguracoesCompeticao]    Script Date: 10/28/2025 10:00:17 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- use [dev.engage.bz]
-- use [test.engage.bz]
-- use [pre.engage.bz]
CREATE FUNCTION [dbo].[fnt_ConfiguracoesCompeticao] (@clienteID NVARCHAR(24), @competicaoID INT)
RETURNS TABLE
AS RETURN
		SELECT ConfiguracaoCompeticao.ID AS ConfiguracaoID
			 , ISNULL(ConfiguracaoCompeticaoCliente.ID_CLIENTE, @clienteID) AS ClienteID
			 , ISNULL(ConfiguracaoCompeticaoCliente.ID_COMPETICAO, @competicaoID) AS CompeticaoID
			 , ConfiguracaoCompeticao.TX_DESCRICAO AS Descricao
			 , ISNULL(ConfiguracaoCompeticaoCliente.TX_ARQUIVO, ConfiguracaoCompeticao.TX_ARQUIVO) AS Arquivo
			 , ISNULL(ConfiguracaoCompeticaoCliente.FL_STATUS, ConfiguracaoCompeticao.FL_STATUS) AS [Status]
			 , ISNULL(ConfiguracaoCompeticaoCliente.FL_VISIVEL, ConfiguracaoCompeticao.FL_VISIVEL) AS Visivel
			 , ISNULL(CONVERT(BIT, IIF(ConfiguracaoCompeticaoCliente.ID_CONFIGURACAO_COMPETICAO IS NULL, 1, 0)),0) AS EstaUtilizandoConfiguracaoPadrao
			 , TX_GRUPO AS RotuloGrupo
			 , NU_ORDEM AS OrdemConfiguracao
			 , TX_AJUDA AS RotuloAjuda
			 , NU_ORDEM_GRUPO AS OrdemGrupo
			 -- Novas colunas
			 , TX_GRUPO_CONFIG 
			 , NU_ORDEM_CONFIG 
			 , NU_ORDEM_GRUPO_CONFIG
		  FROM CONFIGURACAO_COMPETICAO AS ConfiguracaoCompeticao (NOLOCK)
     LEFT JOIN CONFIGURACAO_COMPETICAO_CLIENTE AS ConfiguracaoCompeticaoCliente (NOLOCK)
	        ON ConfiguracaoCompeticaoCliente.ID_CONFIGURACAO_COMPETICAO = ConfiguracaoCompeticao.ID
		   AND ConfiguracaoCompeticaoCliente.ID_CLIENTE = @clienteID
		   AND ConfiguracaoCompeticaoCliente.ID_COMPETICAO = @competicaoID
		 
-- SELECT * FROM fnt_ConfiguracoesCompeticao('ricardo', 2857) WHERE RotuloGrupo = 'one.admin.competitionSettings.group.ranking'
GO

