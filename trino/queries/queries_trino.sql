CREATE SCHEMA delta.trusted;


CALL delta.system.register_table(
    schema_name => 'trusted',
    table_name => 'sptrans_position',
    table_location => 's3://trusted/sptrans/position'
);

CALL delta.system.register_table(
    schema_name => 'trusted',
    table_name => 'sptrans_stops',
    table_location => 's3://trusted/sptrans/stops'
);


CALL delta.system.register_table(
    schema_name => 'trusted',
    table_name => 'sptrans_prevision_stop',
    table_location => 's3://trusted/sptrans/prevision_stop'
);


create schema delta.refined;




CREATE OR REPLACE VIEW delta.refined.vw_bi_sptrans_prevision_stop AS
SELECT
    prev.latitude_parada,
    prev.longitude_parada,
    prev.latitude_veiculo,
    prev.longitude_veiculo,
    prev.letreiro,
    prev.horario_previsto,
    -- CORREÇÃO: Construímos um timestamp completo para a previsão antes de comparar
    date_diff('minute', 
        current_timestamp AT TIME ZONE 'America/Sao_Paulo',
        (date_trunc('day', current_timestamp AT TIME ZONE 'America/Sao_Paulo') 
         + (CAST(prev.horario_previsto AS TIME) - TIME '00:00'))
    ) AS minutos_para_chegada,
    (
        CASE
            WHEN prev.sentido = 'TERMINAL SECUNDÁRIO PARA PRINCIPAL'
                THEN prev.origem
            ELSE prev.destino
        END
        || ' ' || prev.letreiro
    ) AS veiculo_sentido,
    prev.prefixo_veiculo,
    stp.nome_parada,
    stp.descricao_parada AS parada_ref
FROM delta.trusted.sptrans_prevision_stop prev
LEFT JOIN delta.trusted.sptrans_stops stp
    ON CAST(prev.codigo_parada AS VARCHAR) = CAST(stp.id_parada AS VARCHAR)
WHERE
    -- Filtro: Apenas previsões de HOJE que ainda não aconteceram
    (date_trunc('day', current_timestamp AT TIME ZONE 'America/Sao_Paulo') 
     + (CAST(prev.horario_previsto AS TIME) - TIME '00:00')) 
     >= (current_timestamp AT TIME ZONE 'America/Sao_Paulo')
AND
    -- Filtro: Posição do veículo atualizada hoje
    CAST(prev.timestamp_posicao_veiculo AT TIME ZONE 'America/Sao_Paulo' AS DATE) 
    = CAST(current_timestamp AT TIME ZONE 'America/Sao_Paulo' AS DATE);

;



