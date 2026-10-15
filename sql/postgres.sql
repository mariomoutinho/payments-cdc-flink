CREATE TABLE customers (
 customer_id BIGINT PRIMARY KEY, customer_name TEXT NOT NULL, email TEXT NOT NULL
);
CREATE TABLE merchants (
 merchant_id BIGINT PRIMARY KEY, merchant_name TEXT NOT NULL
);
CREATE TABLE accounts (
  account_id BIGINT PRIMARY KEY,
  customer_name TEXT NOT NULL,
  risk_level TEXT NOT NULL
);
CREATE TABLE transactions (
  transaction_id BIGINT PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(account_id),
  card_id TEXT NOT NULL,
  amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
  occurred_at BIGINT NOT NULL
);
CREATE TABLE cards (
 card_id TEXT PRIMARY KEY, account_id BIGINT NOT NULL REFERENCES accounts(account_id)
);
ALTER TABLE accounts REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;
CREATE PUBLICATION payments_pub FOR TABLE accounts, transactions;
-- Identidades de aplicacao sem login; o bootstrap cria senhas aleatorias fora do Git.
CREATE ROLE payments_writer;
CREATE ROLE payments_reader;
GRANT CONNECT ON DATABASE payments TO payments_writer, payments_reader;
GRANT USAGE ON SCHEMA public TO payments_writer, payments_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON accounts, transactions TO payments_writer;
GRANT SELECT ON accounts, transactions TO payments_reader;
CREATE ROLE payments_cdc REPLICATION;
GRANT CONNECT ON DATABASE payments TO payments_cdc;
GRANT USAGE ON SCHEMA public TO payments_cdc;
GRANT SELECT ON accounts, transactions TO payments_cdc;
GRANT SELECT, INSERT, UPDATE, DELETE ON customers, cards, merchants TO payments_writer;
GRANT SELECT ON customers, cards, merchants TO payments_reader;
