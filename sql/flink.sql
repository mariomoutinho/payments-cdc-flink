SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';
SET 'pipeline.name' = 'payments-enrichment-and-fraud';

CREATE TABLE accounts (
 account_id BIGINT, customer_name STRING, risk_level STRING,
 PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
 'connector'='kafka', 'topic'='payments.public.accounts',
 'properties.bootstrap.servers'='kafka:29092', 'properties.group.id'='flink-accounts',
 'scan.startup.mode'='earliest-offset', 'format'='debezium-json'
);
CREATE TABLE transactions (
 transaction_id BIGINT, account_id BIGINT, card_id STRING, amount_cents BIGINT,
 occurred_at BIGINT, PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
 'connector'='kafka', 'topic'='payments.public.transactions',
 'properties.bootstrap.servers'='kafka:29092', 'properties.group.id'='flink-transactions',
 'scan.startup.mode'='earliest-offset', 'format'='debezium-json'
);
-- Segunda leitura do envelope bruto: append-only para MATCH_RECOGNIZE.
CREATE TABLE transaction_events (
 `after` ROW<transaction_id BIGINT, account_id BIGINT, card_id STRING,
             amount_cents BIGINT, occurred_at BIGINT>,
 op STRING,
 event_time AS TO_TIMESTAMP_LTZ(`after`.occurred_at, 3),
 WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
) WITH (
 'connector'='kafka', 'topic'='payments.public.transactions',
 'properties.bootstrap.servers'='kafka:29092', 'properties.group.id'='flink-events',
 'scan.startup.mode'='earliest-offset', 'format'='json'
);
CREATE TABLE enriched_transactions (
 transaction_id BIGINT, account_id BIGINT, card_id STRING, amount_cents BIGINT,
 occurred_at BIGINT, customer_name STRING, risk_level STRING,
 PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
 'connector'='upsert-kafka', 'topic'='payments.enriched',
 'properties.bootstrap.servers'='kafka:29092',
 'key.format'='json', 'value.format'='json'
);
CREATE TABLE fraud_alerts (
 transaction_id BIGINT, account_id BIGINT, card_id STRING,
 first_transaction_id BIGINT, customer_name STRING, risk_level STRING,
 PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
 'connector'='upsert-kafka', 'topic'='payments.alerts',
 'properties.bootstrap.servers'='kafka:29092', 'key.format'='json',
 'value.format'='avro-confluent',
 'value.avro-confluent.url'='http://registry:8081',
 'value.avro-confluent.subject'='payments.alerts-value'
);
CREATE VIEW created_transactions AS
 SELECT `after`.transaction_id AS transaction_id, `after`.account_id AS account_id,
        `after`.card_id AS card_id, event_time
 FROM transaction_events WHERE op = 'c';
CREATE VIEW suspicious_sequences AS
 SELECT * FROM created_transactions
 MATCH_RECOGNIZE (
  PARTITION BY card_id ORDER BY event_time
  MEASURES C.transaction_id AS transaction_id, C.account_id AS account_id,
           A.transaction_id AS first_transaction_id
  ONE ROW PER MATCH
  AFTER MATCH SKIP TO NEXT ROW
  PATTERN (A B C) WITHIN INTERVAL '60' SECOND
  DEFINE A AS TRUE, B AS TRUE, C AS TRUE
 );
EXECUTE STATEMENT SET
BEGIN
 INSERT INTO enriched_transactions
 SELECT t.transaction_id, t.account_id, t.card_id, t.amount_cents, t.occurred_at,
        a.customer_name, a.risk_level
 FROM transactions t JOIN accounts a ON t.account_id = a.account_id;
 INSERT INTO fraud_alerts
 SELECT s.transaction_id, s.account_id, s.card_id, s.first_transaction_id,
        a.customer_name, a.risk_level
 FROM suspicious_sequences s JOIN accounts a ON s.account_id = a.account_id;
END;
