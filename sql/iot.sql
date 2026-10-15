SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.source.idle-timeout' = '5 s';
SET 'pipeline.name' = 'iot-enrichment-and-detection';
CREATE TABLE iot_devices (
 device_id STRING, site_id STRING, criticality STRING,
 PRIMARY KEY (device_id) NOT ENFORCED
) WITH (
 'connector'='kafka','topic'='iot.public.devices',
 'properties.bootstrap.servers'='kafka:29092','properties.group.id'='iot-devices',
 'scan.startup.mode'='earliest-offset','format'='debezium-json'
);
CREATE TABLE iot_telemetry (
 reading_id BIGINT, device_id STRING, temperature_c INT, battery_pct INT, observed_at BIGINT,
 PRIMARY KEY (reading_id) NOT ENFORCED
) WITH (
 'connector'='kafka','topic'='iot.public.telemetry',
 'properties.bootstrap.servers'='kafka:29092','properties.group.id'='iot-state',
 'scan.startup.mode'='earliest-offset','format'='debezium-json'
);
CREATE TABLE iot_events (
 `after` ROW<reading_id BIGINT,device_id STRING,temperature_c INT,battery_pct INT,observed_at BIGINT>,
 op STRING,
 event_time AS TO_TIMESTAMP_LTZ(`after`.observed_at,3),
 WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
) WITH (
 'connector'='kafka','topic'='iot.public.telemetry',
 'properties.bootstrap.servers'='kafka:29092','properties.group.id'='iot-events',
 'scan.startup.mode'='earliest-offset','format'='json'
);
CREATE TABLE iot_enriched (
 reading_id BIGINT,device_id STRING,temperature_c INT,battery_pct INT,observed_at BIGINT,
 site_id STRING,criticality STRING,PRIMARY KEY (reading_id) NOT ENFORCED
) WITH (
 'connector'='upsert-kafka','topic'='iot.enriched',
 'properties.bootstrap.servers'='kafka:29092',
 'key.format'='avro-confluent','key.avro-confluent.url'='http://registry:8081',
 'value.format'='avro-confluent','value.avro-confluent.url'='http://registry:8081',
 'value.avro-confluent.schema'='{"type":"record","name":"TelemetryEnriched","namespace":"com.bootcamp.iot","fields":[{"name":"reading_id","type":"long"},{"name":"device_id","type":["null","string"],"default":null},{"name":"temperature_c","type":["null","int"],"default":null},{"name":"battery_pct","type":["null","int"],"default":null},{"name":"observed_at","type":["null","long"],"default":null},{"name":"site_id","type":["null","string"],"default":null},{"name":"criticality","type":["null","string"],"default":null}]}'
);
CREATE TABLE iot_velocity_alerts (
 reading_id BIGINT,device_id STRING,first_reading_id BIGINT,
 PRIMARY KEY (reading_id) NOT ENFORCED
) WITH (
 'connector'='upsert-kafka','topic'='iot.alerts.velocity',
 'properties.bootstrap.servers'='kafka:29092','key.format'='json','value.format'='json'
);
CREATE VIEW iot_created AS
 SELECT `after`.reading_id AS reading_id,`after`.device_id AS device_id,
 `after`.temperature_c AS temperature_c,`after`.battery_pct AS battery_pct,event_time
 FROM iot_events WHERE op='c';
CREATE VIEW iot_velocity AS
 SELECT * FROM iot_created MATCH_RECOGNIZE (
 PARTITION BY device_id ORDER BY event_time
 MEASURES C.reading_id AS reading_id,A.reading_id AS first_reading_id
 ONE ROW PER MATCH AFTER MATCH SKIP TO NEXT ROW
 PATTERN (A B C) WITHIN INTERVAL '60' SECOND
 DEFINE A AS TRUE,B AS TRUE,C AS TRUE
 );
EXECUTE STATEMENT SET BEGIN
 INSERT INTO iot_enriched
 SELECT t.reading_id,t.device_id,t.temperature_c,t.battery_pct,t.observed_at,d.site_id,d.criticality
 FROM iot_telemetry t JOIN iot_devices d ON t.device_id=d.device_id;
 INSERT INTO iot_velocity_alerts SELECT reading_id,device_id,first_reading_id FROM iot_velocity;
END;
