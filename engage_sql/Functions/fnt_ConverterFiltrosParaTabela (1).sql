CREATE FUNCTION dbo.fnt_ConverterFiltrosParaTabela
(
    @inputString NVARCHAR(MAX), -- String com os valores separados por vírgula
    @tipo NVARCHAR(50)          -- Tipo que define a conversão
)
RETURNS TABLE
AS
RETURN
(
    SELECT 
        VALUE AS OriginalValue, -- O valor original da string
        CASE 
		    WHEN @tipo = 'perfilUsuarioTrilhaIds' THEN
                CASE 
                    WHEN VALUE = 1 THEN 'Obrigatório'
                    WHEN VALUE = 2 THEN 'Participa'
                    WHEN VALUE = 3 THEN 'Gestor' 
                    ELSE 'Desconhecido'
                END
            WHEN @tipo = 'statusConclusaoCompeticao' THEN
                CASE 
                    WHEN LTRIM(RTRIM(VALUE)) = 'finished' THEN 'Concluído'
                    WHEN LTRIM(RTRIM(VALUE)) = 'expired' THEN 'Expirado (Não concluído)'
                    WHEN LTRIM(RTRIM(VALUE)) = 'in_progress' THEN 'Em andamento'
                    WHEN LTRIM(RTRIM(VALUE)) = 'not_started' THEN 'Não iniciado'
                    WHEN LTRIM(RTRIM(VALUE)) = 'not_released' THEN 'Não liberado'
                    WHEN LTRIM(RTRIM(VALUE)) = 'exempted' THEN 'Dispensado'
                    ELSE 'Desconhecido'
                 END
            WHEN @tipo = 'statusConclusaoRodada' THEN
                CASE 
                    WHEN LTRIM(RTRIM(VALUE)) = 'approved' THEN 'Aprovado'
                    WHEN LTRIM(RTRIM(VALUE)) = 'reproved' THEN 'Reprovado'
                    WHEN LTRIM(RTRIM(VALUE)) = 'out_of_date' THEN 'Fora do Prazo'
                    WHEN LTRIM(RTRIM(VALUE)) = 'expired' THEN 'Expirado (Não Realizado)'
                    WHEN LTRIM(RTRIM(VALUE)) = 'awaiting_approval' THEN 'Aguardando Correção'
                    WHEN LTRIM(RTRIM(VALUE)) = 'in_progress' THEN 'Em Andamento'
                    WHEN LTRIM(RTRIM(VALUE)) = 'not_started' THEN 'Não Iniciado'
                    WHEN LTRIM(RTRIM(VALUE)) = 'not_released' THEN 'Não Liberado'
                    WHEN LTRIM(RTRIM(VALUE)) = 'exempted' THEN 'Dispensado'
                    WHEN LTRIM(RTRIM(VALUE)) = 'invalid_certification' THEN 'Certificação Inválida'
                    WHEN LTRIM(RTRIM(VALUE)) = 'valid_certification' THEN 'Certificação Válida'
               ELSE 'Desconhecido'
                END
			WHEN @tipo = 'statusConclusaoAtividade' THEN
                CASE 
                    WHEN LTRIM(RTRIM(VALUE)) = 'finished' THEN 'Concluído'
                    WHEN LTRIM(RTRIM(VALUE)) = 'expired' THEN 'Expirado'
                    WHEN LTRIM(RTRIM(VALUE)) = 'awaiting_approval' THEN 'Aguardando Correção'
                    WHEN LTRIM(RTRIM(VALUE)) = 'in_progress' THEN 'Em Andamento'
                    WHEN LTRIM(RTRIM(VALUE)) = 'not_started' THEN 'Não Iniciado'
                    WHEN LTRIM(RTRIM(VALUE)) = 'not_released' THEN 'Não Liberado'
                    WHEN LTRIM(RTRIM(VALUE)) = 'exempted' THEN 'Dispensado'
               ELSE 'Desconhecido'
                END
            WHEN @tipo = 'statusConclusaoTrilha' THEN
                CASE 
                    WHEN LTRIM(RTRIM(VALUE)) = 'finished' THEN 'Concluído'
                    WHEN LTRIM(RTRIM(VALUE)) = 'expired' THEN 'Expirado (Não Concluído)'
                    WHEN LTRIM(RTRIM(VALUE)) = 'in_progress' THEN 'Em Andamento'
                    WHEN LTRIM(RTRIM(VALUE)) = 'not_started' THEN 'Não Iniciado'
                    WHEN LTRIM(RTRIM(VALUE)) = 'not_released' THEN 'Não Liberado'
                    WHEN LTRIM(RTRIM(VALUE)) = 'exempted' THEN 'Dispensado'
                    WHEN LTRIM(RTRIM(VALUE)) = 'invalid_certification' THEN 'Certificação Inválida'
                    WHEN LTRIM(RTRIM(VALUE)) = 'valid_certification' THEN 'Certificação Válida'
               ELSE 'Desconhecido'
                END

			WHEN @tipo = 'statusPergunta' THEN
                CASE 
                    WHEN LTRIM(RTRIM(VALUE)) = 'pending' THEN 'Pendente'
       WHEN LTRIM(RTRIM(VALUE)) = 'answered' THEN 'Respondida'
               ELSE 'Desconhecido'
                END

			WHEN @tipo = 'statusEnvioNotificacao' THEN
                CASE 
                    WHEN LTRIM(RTRIM(VALUE)) = 'summary' THEN 'Rascunho'
                    WHEN LTRIM(RTRIM(VALUE)) = 'wating_for_sending' THEN 'Aguardando envio'
					WHEN LTRIM(RTRIM(VALUE)) = 'failed' THEN 'Falha'
					WHEN LTRIM(RTRIM(VALUE)) = 'sent' THEN 'Enviado'
               ELSE 'Desconhecido'
                END

			WHEN @tipo = 'tipoTentativa' THEN
                CASE 
                    WHEN LTRIM(RTRIM(VALUE)) = 'recertification' THEN 'Recertificação'
                    WHEN LTRIM(RTRIM(VALUE)) = 'default' THEN 'Padrão'
               ELSE 'Desconhecido'
                END

             ELSE VALUE
        END AS ConvertedValue, -- O valor convertido com base no tipo
        @tipo AS Tipo -- O tipo associado
    FROM STRING_SPLIT(@inputString, ',')
);
