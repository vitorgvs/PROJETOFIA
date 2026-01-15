CREATE SCHEMA delta.trusted;

show catalogs;


;


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
    table_name => 'sptrans_stops',
    table_location => 's3://trusted/sptrans/stops'
);

select codigo_linha_id, prefixo, count(distinct timestamp_posicao) qtd_tempo 
from delta.trusted.sptrans_position
group by 1, 2
order by 3

;

create schema delta.refined;

select * from delta.trusted.sptrans_prevision_stop;


;

drop view delta.refined.vw_sptrans_prevision_stop

;

CREATE OR REPLACE VIEW delta.refined.vw_bi_sptrans_prevision_stop AS
SELECT
    prev.latitude_parada,
    prev.longitude_parada,
    prev.latitude_veiculo,
    prev.longitude_veiculo,
    prev.letreiro,
    prev.horario_previsto,
    (
        CASE
            WHEN prev.sentido = 'TERMINAL SECUNDÁRIO PARA PRINCIPAL'
                THEN prev.origem
            ELSE prev.destino
        END
        || ' ' || prev.letreiro
    ) AS veiculo_sentido,
    prev.prefixo_veiculo,
    stp.stop_name AS nome_parada,
    stp.stop_desc AS parada_ref
FROM delta.trusted.sptrans_prevision_stop prev
LEFT JOIN delta.trusted.sptrans_stops stp
    ON prev.codigo_parada = stp.stop_id
	where CAST(prev.horario_previsto AS TIME) >= current_time;

;
CREATE OR REPLACE VIEW delta.refined.vw_bi_sptrans_prevision_stop AS
SELECT
    prev.latitude_parada,
    prev.longitude_parada,
    prev.latitude_veiculo,
    prev.longitude_veiculo,
    prev.letreiro,
    prev.horario_previsto,
    (
        CASE
            WHEN prev.sentido = 'TERMINAL SECUNDÁRIO PARA PRINCIPAL'
                THEN prev.origem
            ELSE prev.destino
        END
        || ' ' || prev.letreiro
    ) AS veiculo_sentido,
    prev.prefixo_veiculo,
    stp.stop_name AS nome_parada,
    stp.stop_desc AS parada_ref
FROM delta.trusted.sptrans_prevision_stop prev
LEFT JOIN delta.trusted.sptrans_stops stp
    ON prev.codigo_parada = stp.stop_id
WHERE
    extract(hour from CAST(horario_previsto AS TIME)) >= (extract(hour from current_time - interval '1' hour))
    and
	extract(minute from CAST(horario_previsto AS TIME)) >= extract(minute from current_time) 
    ;

;
select * from delta.refined.vw_bi_sptrans_prevision_stop
order by horario_previsto desc
;
		describe delta.refined.vw_bi_sptrans_prevision_stop
	  
;
select * from delta.refined.vw_bi_sptrans_prevision_stop
order by horario_previsto desc
;

