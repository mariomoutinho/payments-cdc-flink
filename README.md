# Evolução em cinco etapas — IoT

O domínio ativo das evoluções é telemetria IoT: dispositivos → CDC → Kafka → Flink SQL → dados enriquecidos Avro e detecção de anomalias. O laboratório de pagamentos abaixo permanece como regressão e histórico de evidências.

1. [Domínio IoT](docs/etapas/01-iot.md): CDC próprio, enriquecimento Avro e velocidade de leituras.
2. [Regra térmica](docs/etapas/02-regra-termica.md): média >=80°C por 5 minutos, com controles negativos.
3. [Quarentena](docs/etapas/03-quarentena.md): validação de eventos, DLQ e saída/offset em transação Kafka.
4. [Iceberg/Tableflow](docs/etapas/04-tableflow.md): configuração Cloud opt-in, validada estaticamente; ativação pendente.
5. [Modelo de risco](docs/etapas/05-ia.md): regressão logística treinada offline e inferência Avro transacional por leitura.

`./setup.sh` executa os dois laboratórios. `./teardown.sh` remove os recursos locais. Saídas da evolução ficam em `artifacts/iot-validation.txt`, disponíveis também no artefato do Actions. As descrições acima são separadas por etapa. Evidências de 1–3 estão versionadas em `evidence/steps/`; a etapa 4 possui evidência estática e não foi aplicada. A etapa 5 inclui treino e avaliação reais; a validação streaming e teardown ficam no artefato do [workflow](https://github.com/mariomoutinho/payments-cdc-flink/actions/workflows/validate.yml).

Caminho IoT final: Postgres → CDC → Kafka bruto → validador transacional/DLQ → Flink (enriquecimento + duas regras) → Avro → modelo treinado → `iot.risk.scored`. Tableflow é um ramo Cloud opt-in para `iot.enriched`, sem ativação nesta conta. Consulte [resumo das cinco etapas](docs/etapas/README.md). As evidências históricas de pagamentos abaixo não são apresentadas como prova das evoluções.

# Desafio final — pipeline de pagamentos

**Pipeline local funcionando, com evidências reais. A execução em Neon/Confluent Cloud continua pendente.** Não houve criação de recursos Cloud nem consulta de sua fatura. A [matriz de aderência ao exemplo](docs/aderencia-template.md) mostra os limites de cada camada, incluindo CDC local em JSON e join de estado atual em vez de temporal.

Caminho executado: **PostgreSQL → Debezium CDC → Kafka → Flink SQL → Kafka → consumidor SQLite**. Alertas usam **Avro + Schema Registry**; o CDC bruto usa JSON. Conta GitHub e repositório público: [mariomoutinho/payments-cdc-flink](https://github.com/mariomoutinho/payments-cdc-flink).

## Subir do zero

Pré-requisitos: Git, Python 3.12 com suporte a venv, Docker Engine/Compose v2 funcionando, aproximadamente 8 GB de RAM disponíveis e internet. No WSL, habilite a integração do Docker Desktop. Use um ambiente limpo; setup não é uma migração idempotente de um laboratório já populado.

```bash
git clone https://github.com/mariomoutinho/payments-cdc-flink.git
cd payments-cdc-flink
cp .env.example .env
# Preenche a senha local sem imprimi-la ou gravá-la no histórico:
python3 -c "import secrets; open('.env','w').write('POSTGRES_PASSWORD='+secrets.token_hex(24)+'\n')"
./setup.sh
```

`setup.sh` instala dependências em `.venv`, sobe os serviços, carrega seed=42, registra o conector e o SQL, gera compras de teste e verifica o fluxo real. O mesmo script é executado no CI. Logs e projeção SQLite ficam em `artifacts/`, ignorado pelo Git. Senhas dos logins de aplicação são geradas em memória e injetadas no Connect; a resposta de configuração não é impressa porque contém senha. `.env` não é versionado. Configuração sem secrets: [Compose](compose.yaml), [conector](connectors/postgres-cdc.json), [SQL Postgres](sql/postgres.sql), [SQL Flink](sql/flink.sql).

A interface Flink fica em `http://localhost:8082`; Kafka em `localhost:9092`; Registry em `http://localhost:8081`; Connect em `http://localhost:8083`. Portas REST limitadas a loopback. Kafka e APIs locais não possuem autenticação/TLS; mantenha este laboratório isolado.

## Execução comprovada

[Workflow aprovado](https://github.com/mariomoutinho/payments-cdc-flink/actions/runs/38758536328), concluído em **15/10/2026 12:16:28 (America/Sao_Paulo)**. Commit de código testado: `5d483f13e2470e2d4785f6497c9ceb8d1f940bcd`. Os commits seguintes apenas incorporam documentação e evidências. [Log completo](evidence/validation.txt), [serviços](evidence/services.txt), [SQL client](evidence/sql-client.txt), [metadados](evidence/run.json). As cópias de texto removem cores ANSI e espaços finais, sem mudar valores. Evidências são versionadas para não depender da retenção temporária de artefatos do Actions.

## Camada 1 — Fundação

Compose cria Postgres, Kafka KRaft, Schema Registry, Debezium Connect e Flink. São peças explícitas para reproduzir o caminho do dado sem depender de conta externa. Há escritor, leitor e replicador separados no PostgreSQL. Evidência do comando executado pelo validador:

```text
FOUNDATION: distinct writer, reader and CDC roles
     grantee     | privilege_type
-----------------+----------------
 payments_cdc    | SELECT
 payments_reader | SELECT
 payments_writer | DELETE
 payments_writer | INSERT
 payments_writer | SELECT
 payments_writer | UPDATE
(6 rows)


ACCESS: reader INSERT denied by PostgreSQL
```

O teste de acesso usa `SET ROLE payments_reader`; não representa ACL de Kafka. A [fundação Cloud opcional](infra/cloud/README.md) possui ambiente, cluster Basic, tópicos, service accounts e ACLs separados em Terraform. Só foi executado `terraform validate`, cuja [saída real](evidence/terraform-validate.txt) é `Success! The configuration is valid.`. Não se deve apresentar isso como provisionamento Cloud.

## Camada 2 — Contrato de dados

O Flink usa explicitamente [fraud-detected.avsc](schemas/fraud-detected.avsc), namespace `com.bootcamp.payments.fraud`. `money` é `bytes` com logicalType decimal, precisão 15 e escala 2. Campos `customer_name` e `card_id` têm tags inline PII/PCI; cartão é token sintético, nunca PAN ou CVV. Tags são classificação, não criptografia nem controle de acesso. No Cloud, suas definições precisam existir no catálogo antes do registro.

Evidência das chamadas reais da API de compatibilidade e registro, sob BACKWARD:

```text
COMPATIBILITY: optional default field {"is_compatible": true}
COMPATIBILITY: removing writer field under BACKWARD {"is_compatible": true}
COMPATIBILITY: adding required field without default {"is_compatible": false}
CONTRACT: optional merchant field accepted: {"id": 2}
CONTRACT: incompatible transaction_id type rejected: HTTP 409
```

**Correção do template:** remover do novo reader um campo existente no writer pode ser compatível com BACKWARD. O Registry demonstrou isso. A rejeição correta foi comprovada para campo novo obrigatório sem default e mudança de long para string. Veja [resolução de schemas Avro](https://avro.apache.org/docs/1.7.5/spec.html#Schema+Resolution) e [padrão de compatibilidade Confluent](https://developer.confluent.io/patterns/event-stream/schema-compatibility/). Os schemas capturados são [v1](schemas/alerts-v1.avsc), [v2](schemas/alerts-v2.avsc) e [incompatível](schemas/alerts-incompatible.avsc).

## Camada 3 — Ingestão CDC

Debezium usa `pgoutput`, publicação explícita, `payments_cdc` sem superuser, `REPLICA IDENTITY FULL`, heartbeat de 60s e tópico de heartbeat pré-criado. O arquivo [postgres-cdc.json](connectors/postgres-cdc.json) contém apenas configuração não sensível. Este é o conector Java local, não o gerenciado `PostgresCdcSourceV2`.

O [gerador determinístico](scripts/seed.py), seed=42, carrega cinco tabelas. Contagem obtida por SELECT no PostgreSQL antes dos eventos de teste:

```text
customers | accounts | cards | merchants | transactions
-----------+----------+-------+-----------+--------------
       200 |      240 |   320 |        60 |         2000
(1 row)
```

Os snapshots `op=r` alimentam a visão de estado, sem gerar alertas históricos. Depois, o teste insere duas contas e oito transações vivas. As dimensões customers/cards/merchants têm dados sintéticos; a conta contém o nome desnormalizado, e somente accounts/transactions entram na publicação CDC.

Eventos observados no Kafka, copiados literalmente do log:

```text
CDC EVENT c: {"key": {"transaction_id": 1}, "value": {"before": null, "after": {"transaction_id": 1, "account_id": 1, "card_id": "card-fraud", "amount_cents": 1000, "occurred_at": 1735689650000}, "source": {"version": "2.7.3.Final", "connector": "postgresql", "name": "payments", "ts_ms": 1792077376090, "snapshot": "false", "db": "payments", "sequence": "[\"26916592\",\"26916592\"]", "ts_us": 1792077376090227, "ts_ns": 1792077376090227000, "schema": "public", "table": "transactions", "txId": 758, "lsn": 26916592, "xmin": null}, "transaction": null, "op": "c", "ts_ms": 1792077376110, "ts_us": 1792077376110631, "ts_ns": 1792077376110631241}}
CDC EVENT u: {"key": {"transaction_id": 1}, "value": {"before": {"transaction_id": 1, "account_id": 1, "card_id": "card-fraud", "amount_cents": 1000, "occurred_at": 1735689650000}, "after": {"transaction_id": 1, "account_id": 1, "card_id": "card-fraud", "amount_cents": 7777, "occurred_at": 1735689650000}, "source": {"version": "2.7.3.Final", "connector": "postgresql", "name": "payments", "ts_ms": 1792077378839, "snapshot": "false", "db": "payments", "sequence": "[\"26918096\",\"27196616\"]", "ts_us": 1792077378839862, "ts_ns": 1792077378839862000, "schema": "public", "table": "transactions", "txId": 770, "lsn": 27196616, "xmin": null}, "transaction": null, "op": "u", "ts_ms": 1792077379147, "ts_us": 1792077379147370, "ts_ns": 1792077379147370054}}
CDC EVENT d: {"key": {"transaction_id": 8}, "value": {"before": {"transaction_id": 8, "account_id": 2, "card_id": "watermark", "amount_cents": 1, "occurred_at": 1735689900000}, "after": null, "source": {"version": "2.7.3.Final", "connector": "postgresql", "name": "payments", "ts_ms": 1792077378840, "snapshot": "false", "db": "payments", "sequence": "[\"27196832\",\"27196832\"]", "ts_us": 1792077378840120, "ts_ns": 1792077378840120000, "schema": "public", "table": "transactions", "txId": 771, "lsn": 27196832, "xmin": null}, "transaction": null, "op": "d", "ts_ms": 1792077379147, "ts_us": 1792077379147649, "ts_ns": 1792077379147649767}}
```

[Envelopes completos e tombstone seguinte](evidence/cdc.json). O update altera 1000 para 7777 centavos; o delete contém `after=null` e é seguido de tombstone com a mesma chave. Não houve evento inventado para preencher esta seção.

## Camada 4 — Processamento e consumo

O [SQL](sql/flink.sql) mantém a visão enriquecida com join entre estados CDC e sink upsert: updates e deletes propagam. **Esse join não é temporal AS OF**; mudanças de conta podem atualizar o resultado. Uma segunda leitura append-only do envelope seleciona apenas `op=c` para MATCH_RECOGNIZE. Isso evita tratar update como nova compra.

A regra usa `PARTITION BY card_id`, `(A B C) WITHIN INTERVAL '60' SECOND` e `AFTER MATCH SKIP TO NEXT ROW`: quatro compras podem gerar duas sequências sobrepostas. Idle timeout local é `table.exec.source.idle-timeout=5 s`; watermark permite 2s de atraso. O controle de 61s usa outro cartão; uma compra a 300s avança a watermark. Fontes ociosas e eventos mais atrasados precisam de política própria em produção.

Compras que disparam o primeiro alerta: IDs **1, 2, 3**, cartão **card-fraud**, tempos **50s, 70s, 109s** (59s, atravessando a virada do minuto). IDs **2, 3, 4** disparam o segundo alerta (70s, 109s, 110s). A [entrada determinística](data/transactions.csv) permite conferir esses tempos.

Alertas reais consumidos em Avro, resolvendo schema pelo ID do wire format. Na representação JSON abaixo, decimal é mostrado como string, preservando escala:

```json
[
  {
    "key": {
      "transaction_id": 3
    },
    "value": {
      "transaction_id": 3,
      "account_id": 1,
      "card_id": "card-fraud",
      "first_transaction_id": 1,
      "customer_name": "Ana Sintetica",
      "risk_level": "medium",
      "money": "30.00"
    }
  },
  {
    "key": {
      "transaction_id": 4
    },
    "value": {
      "transaction_id": 4,
      "account_id": 1,
      "card_id": "card-fraud",
      "first_transaction_id": 2,
      "customer_name": "Ana Sintetica",
      "risk_level": "medium",
      "money": "40.00"
    }
  }
]
```

O teste também confirma a atualização e a exclusão na [saída enriquecida](evidence/enriched.json). Correções/deletes posteriores não revogam alertas: eles descrevem o histórico de compras observado.

[Consumidor SQLite](scripts/consume_alerts.py): UPSERT pela chave `transaction_id`, commit do efeito antes do offset Kafka. Replay real dos mesmos dois registros em dois grupos, no mesmo banco de efeitos:

```text
CONSUMER: payments-reader-idempotence-a {"processed": 2, "effect_rows": 2}
CONSUMER: payments-reader-idempotence-b {"processed": 2, "effect_rows": 2}
```

Para consumir continuamente após setup:

```bash
.venv/bin/python scripts/consume_alerts.py --group payments-reader-demo --db artifacts/demo.sqlite
```

## Camada 5 — Operação: quatro respostas

### 1. Observabilidade

```text
OPERATIONS: Flink jobs RUNNING; checkpoint counts {"restored": 0, "total": 1, "in_progress": 0, "completed": 1, "failed": 0}
OPERATIONS: replication slot
   slot_name   | active | lag_bytes
---------------+--------+-----------
 payments_slot | t      |    280848
(1 row)


OPERATIONS: committed consumer group lag

Consumer group 'payments-reader-idempotence-b' has no active members.

GROUP                         TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
payments-reader-idempotence-b payments.alerts 0          2               2               0               -               -               -

PASS: real Postgres -> Debezium -> Kafka -> Flink SQL -> Kafka/Avro/Schema Registry
```

[Jobs](evidence/jobs.json), [checkpoints](evidence/checkpoints.json), [lag do grupo](evidence/consumer-lag.txt), [status do conector](evidence/connect-status.json) e [diagnóstico de disparos de checkpoint](evidence/initial-checkpoint.txt). O teste exige pelo menos um checkpoint concluído, além de job e conector RUNNING. O lag é uma fotografia, não pico histórico. **Pico de received_bytes e lag pela Metrics API Confluent não foram medidos.**

### 2. Segurança

| Identidade | Permissões | Motivo |
|---|---|---|
| postgres local | Administração do laboratório | Bootstrap/validação |
| payments_writer | SELECT/INSERT/UPDATE/DELETE | Seed e mudanças de teste |
| payments_reader | SELECT | Leitura; INSERT negado |
| payments_cdc | REPLICATION + SELECT em accounts/transactions | CDC sem superuser |
| Cloud payments-writer | WRITE em payments.public. | Terraform; não testado no serviço |
| Cloud payments-reader | READ em payments. e grupo payments-reader- | Terraform; não testado no serviço |
| Cloud payments-admin | CloudClusterAdmin | Provisionamento; não usar em consumidores |

Senhas e chaves não foram versionadas. APIs locais não devem ser expostas na internet. [Detalhes e aderência](docs/aderencia-template.md).

### 3. Custos

A API do GitHub informou **132 segundos** de duração total do workflow e **0 ms faturáveis** nos jobs Ubuntu. Runner padrão em repositório público é [gratuito](https://docs.github.com/en/billing/concepts/product-billing/github-actions). [Resposta real da API](evidence/timing.json), obtida com:

```bash
gh api repos/mariomoutinho/payments-cdc-flink/actions/runs/38758536328/timing
```

Custo de computação Actions desta execução: **US$ 0**. Recursos Confluent criados: **nenhum**. Não executamos `billing cost list` Confluent nem consultamos fatura de armazenamento de artefatos; não há custo Cloud medido para declarar. Energia e hardware local não foram estimados.

Duas alavancas para futura execução Cloud: remover connector/statements/compute pool imediatamente após a avaliação; limitar CFU, retenção e partições ao volume do laboratório. A cobrança real precisa ser coletada no período e comparada com essas escolhas.

### 4. Confiabilidade

| Trecho | Garantia/risco | Tratamento |
|---|---|---|
| Postgres → WAL | Slot parado retém WAL | Medir lag/retenção; não apagar slot ativo |
| WAL → Kafka | Retomada pode reemitir eventos | PK estável e projeção upsert |
| Kafka → Flink | Checkpoints; estado local efêmero | Produção exige storage durável e restauração testada |
| Flink → Kafka | Não declaramos exactly-once ponta a ponta | Chaves determinísticas e tolerância a replay |
| Kafka → SQLite | Crash entre persistência e commit reentrega | UPSERT por PK e commit manual; replay testado |
| SQLite → efeito externo | Não coberto pela garantia | Usar outbox/chave idempotente no destino |

O elo fraco é o efeito final do consumidor. A idempotência comprovada é da projeção SQLite, não de um pagamento ou envio externo. Join regular mantém estado sem TTL; uma execução de produção exige estratégia explícita de retenção.

## Teardown e repetição

```bash
./teardown.sh
# Opcional, depois de inspecionar os logs:
rm -f .env
```

O script remove somente containers/volumes do projeto e verifica listas vazias. Preserva `.env`, logs, imagens e cache para inspeção; não remove recursos Cloud. Para repetir setup, execute teardown antes e mantenha uma senha válida em `.env`.

Evidência do teardown executado pelo CI:

```text
PASS: project containers=[] volumes=[]
```

[Saída completa com volume removido](evidence/teardown.txt). No CI, a remoção roda mesmo se o teste falha. Não há saída `confluent ... None found` porque não houve provisionamento Cloud.

## O que falta para a entrega gerenciada do exemplo

Conta Neon/Postgres com TLS e acesso de rede do conector, conta Confluent com orçamento e credenciais disponibilizadas com segurança. Será necessário aplicar a fundação, usar identidades próprias para CDC/Flink, registrar contratos dos envelopes Avro, configurar `PostgresCdcSourceV2`, adaptar SQL/catálogo e temporal join ao serviço, medir Metrics/Billing APIs e comprovar teardown Cloud. O [módulo Cloud](infra/cloud/README.md) cobre apenas a fundação. A execução local não deve ser submetida como prova de execução no Confluent Cloud.
