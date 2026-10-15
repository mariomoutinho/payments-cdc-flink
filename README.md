# Payments CDC + Flink SQL

Laboratório reproduzível: **PostgreSQL → Debezium CDC → Kafka → Flink SQL → Kafka + Avro/Schema Registry**.

**Estado: pipeline local executado e aprovado no [GitHub Actions](https://github.com/mariomoutinho/payments-cdc-flink/actions/runs/38756328476).** **Não executado no Confluent Cloud**: nenhuma conta/chave foi fornecida. Não apresentar esta versão como evidência de execução na nuvem. As identidades locais são roles PostgreSQL; Kafka local usa PLAINTEXT em rede de laboratório, sem RBAC. A exigência de identidades Kafka distintas na nuvem permanece pendente.

## Subir do zero

Pré-requisitos: Git, Docker Engine com Compose v2, Python 3.12, aproximadamente 8 GB de RAM disponíveis e internet para baixar imagens/JARs. No WSL habilite a integração do Docker Desktop.

```bash
git clone https://github.com/mariomoutinho/payments-cdc-flink.git
cd payments-cdc-flink
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python3 -c "import secrets; open('.env','w').write('POSTGRES_PASSWORD='+secrets.token_hex(24)+'\n')"
docker compose up -d --build --wait --wait-timeout 240
mkdir -p artifacts
set -o pipefail
python scripts/validate.py 2>&1 | tee artifacts/validation.txt
```

Execute a validação uma vez por ambiente limpo: ela cria usuários, conector e dados com chaves fixas. Para repetir, derrube os volumes e suba novamente. `POSTGRES_PASSWORD` é usado só pelo laboratório. `.env` está ignorado; `.env.example` documenta a variável. Senhas das três identidades são aleatórias e ficam em memória e no estado local do Connect, nunca no repositório. Não exponha as portas de Connect/Registry na internet: não têm autenticação neste laboratório.

O GitHub Actions executa exatamente esse caminho em runner Ubuntu, verifica o processamento real, guarda evidências e remove os recursos mesmo em falhas. Consulte a aba Actions e baixe `pipeline-evidence`. O log não imprime a resposta de configuração do Connect, que contém senha.

## Cinco camadas e decisões

1. **Fundação:** Compose isola os serviços. Kafka KRaft evita ZooKeeper. PostgreSQL cria escritor, leitor e replicador separados. O teste confirma que o leitor recebe `permission denied` em INSERT. Não existe isolamento de identidade Kafka nesta versão local.
2. **Contrato:** alertas são serializados em Avro pelo próprio Flink com registro no Schema Registry. O teste registra uma evolução com `merchant` opcional, aceita sob BACKWARD, e tenta mudar `transaction_id` de long para string, rejeitada com HTTP 409. CDC bruto é JSON sem validação de contrato pelo Registry; o contrato aplicado é o de saída. Os três schemas são capturados da execução.
3. **Ingestão:** Debezium lê WAL com `pgoutput`, publicação explícita e role de replicação com SELECT. `REPLICA IDENTITY FULL` permite antes/depois completos. O teste observa `c`, `u`, `d`, sem converter deletes em novos inserts.
4. **Processamento:** Flink SQL mantém visão enriquecida via join com contas e `upsert-kafka`, propagando atualização e tombstone. Uma segunda leitura append-only do envelope filtra somente `op=c` para MATCH_RECOGNIZE: três compras consecutivas no mesmo cartão em até 60 segundos geram alerta. `AFTER MATCH SKIP TO NEXT ROW` permite sequências sobrepostas; quatro compras produzem dois alertas. O conjunto 50s/70s/109s atravessa a virada do minuto e comprova que não é janela tumbling. O controle 120s/150s/181s excede 60s. Snapshots `r` não geram alertas históricos. Atualizações/deletes posteriores não revogam um alerta já emitido; são o histórico de compras observadas, enquanto a visão enriquecida acompanha o estado atual.
5. **Operação:** teste consulta jobs Flink, estado do conector, slot e bytes de atraso do WAL. Checkpoints a cada 5s são locais/efêmeros: este laboratório não garante recuperação durável ou exactly-once após destruir containers. Observe retenção de WAL se o conector parar. Join regular retém estado sem TTL; em produção precisa de estratégia de retenção conforme as regras do domínio. Watermark tolera 2s; eventos mais atrasados podem ser descartados. Um evento sintético a 300s avança a watermark no teste; em produção defina política para fontes ociosas.

## Operar e consumir

```bash
docker compose ps
curl -fsS localhost:8083/connectors/payments-cdc/status
curl -fsS localhost:8082/jobs/overview
curl -fsS localhost:8081/subjects/payments.alerts-value/versions
docker compose exec -T postgres psql -U postgres -d payments -c "SELECT slot_name,active,pg_wal_lsn_diff(pg_current_wal_lsn(),confirmed_flush_lsn) AS lag_bytes FROM pg_replication_slots;"
```

Kafka: `localhost:9092`; Flink UI: `http://localhost:8082`; Registry: `http://localhost:8081`; Connect: `http://localhost:8083`. Tópicos `payments.public.accounts` e `payments.public.transactions` guardam CDC; `payments.enriched` serve consumidores de estado; `payments.alerts` serve consumidores de alertas. O teste contém um consumidor Avro que resolve o schema pelo ID do wire format e exemplos de tombstones.

## Confluent Cloud: o que falta executar

O módulo opcional [infra/cloud](infra/cloud/README.md) descreve e configura a fundação Cloud via Terraform, sem aplicar recursos. A arquitetura local usa os mesmos conceitos, mas não substitui o provisionamento/execução no serviço gerenciado. Para a entrega na nuvem será necessário:

- Conta Confluent Cloud, região, orçamento e PostgreSQL acessível ao conector via TLS. `localhost`/hostname `postgres` do Compose não são acessíveis pela nuvem.
- Ambiente, cluster Basic, Schema Registry e compute pool Flink na mesma região, com service accounts separados para escrita/leitura e ACLs por tópico e consumer group; identidades próprias para conector e processamento.
- Conector gerenciado `PostgresCdcSourceV2`; não usar o nome da classe Java local na API Cloud. Registrar contrato dos envelopes CDC em formato suportado pelo serviço.
- Adaptar o catálogo e os conectores do SQL à sintaxe gerenciada. Não executar o DDL local com `connector=kafka` diretamente no Cloud. Usar uma visão append-only de eventos para a regra e uma visão CDC para o estado atual.
- Capturar criação, compatibilidade, eventos CDC, alertas, negação de acesso, métricas e fatura/custo real. Excluir statements, connector, compute pool, chaves, service accounts, tópicos, cluster e ambiente ao concluir.

Referências oficiais: [CDC Cloud V2](https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2-debezium/cc-postgresql-cdc-source-v2-debezium.html), [MATCH_RECOGNIZE no Cloud](https://docs.confluent.io/cloud/current/flink/reference/queries/match_recognize.html), [Kafka no Flink 1.20](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/kafka/).

## Custo e encerramento

Nenhum recurso Confluent Cloud foi criado: custo de serviço Confluent desta execução **US$ 0**. Isso não é uma cotação para executar na nuvem. O laboratório consome CPU, RAM, disco e rede da máquina ou runner; não há fatura de infraestrutura disponível. Nesta execução, os dois jobs usaram runner padrão Ubuntu em repositório público: a API de tempo do Actions informou **0 ms faturáveis** e duração total de **129 segundos**. O uso desses runners públicos é [gratuito segundo o GitHub](https://docs.github.com/en/billing/concepts/product-billing/github-actions). Não foi consultada uma fatura de armazenamento de artefatos; estes números não incluem energia da máquina do usuário nem estimam uma futura implantação Cloud. Confira a [resposta original da API](evidence/timing.json).

Para remover o laboratório e os dados, execute **dentro deste repositório**:

```bash
docker compose down -v --remove-orphans
rm -f .env
```

Esse comando remove o volume PostgreSQL do projeto. Imagens Docker e cache de build continuam no host. Nenhum recurso de outros projetos deve ser removido. Na automação o teardown ocorre em `if: always()`.

## Evidências reais da execução

Execução de **15/10/2026, concluída às 12:01:37 (America/Sao_Paulo)**. Commit de código testado: `35516066b5a9695a72413ebb0c514d97a62b4e9e`. [Workflow aprovado com integração e Terraform](https://github.com/mariomoutinho/payments-cdc-flink/actions/runs/38756328476). Documentação e cópias das evidências foram adicionadas após esse teste, sem alterar o código testado.

O comando executado no runner foi `python scripts/validate.py 2>&1 | tee artifacts/validation.txt`. Abaixo estão trechos literais da saída, salvo a remoção das sequências ANSI de cor e espaços finais de linha. O [log completo](evidence/validation.txt), [serviços](evidence/services.txt), [SQL client](evidence/sql-client.txt) e [metadados](evidence/run.json) estão versionados; não dependem da retenção temporária de artefatos do Actions.

### 1. Fundação e acesso

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
CDC: connector and task RUNNING
```

O teste de negação usa `SET ROLE payments_reader` no PostgreSQL; não comprova ACL de Kafka. O módulo Cloud teve esta saída real, sem `apply`:

```text
$ terraform -chdir=infra/cloud validate -no-color
Success! The configuration is valid.
```

### 2. Contrato em uso

```text
CONTRACT: optional merchant field accepted: {"id": 2}
CONTRACT: incompatible transaction_id type rejected: HTTP 409
```

A mensagem de alerta foi lida de Kafka, seu ID de schema foi resolvido no Registry e o corpo foi desserializado como Avro pelo consumidor do teste. Os schemas reais estão em [v1](schemas/alerts-v1.avsc), [v2 compatível](schemas/alerts-v2.avsc) e [mudança rejeitada](schemas/alerts-incompatible.avsc).

### 3. CDC: insert, update e delete

```text
CDC EVENT c: {"key": {"transaction_id": 1}, "value": {"before": null, "after": {"transaction_id": 1, "account_id": 1, "card_id": "card-fraud", "amount_cents": 1000, "occurred_at": 1735689650000}, "source": {"version": "2.7.3.Final", "connector": "postgresql", "name": "payments", "ts_ms": 1792076483725, "snapshot": "false", "db": "payments", "sequence": "[\"26355296\",\"26355296\"]", "ts_us": 1792076483725375, "ts_ns": 1792076483725375000, "schema": "public", "table": "transactions", "txId": 752, "lsn": 26355296, "xmin": null}, "transaction": null, "op": "c", "ts_ms": 1792076483937, "ts_us": 1792076483937447, "ts_ns": 1792076483937447363}}
CDC EVENT u: {"key": {"transaction_id": 1}, "value": {"before": {"transaction_id": 1, "account_id": 1, "card_id": "card-fraud", "amount_cents": 1000, "occurred_at": 1735689650000}, "after": {"transaction_id": 1, "account_id": 1, "card_id": "card-fraud", "amount_cents": 7777, "occurred_at": 1735689650000}, "source": {"version": "2.7.3.Final", "connector": "postgresql", "name": "payments", "ts_ms": 1792076485801, "snapshot": "false", "db": "payments", "sequence": "[\"26356896\",\"26635416\"]", "ts_us": 1792076485801163, "ts_ns": 1792076485801163000, "schema": "public", "table": "transactions", "txId": 764, "lsn": 26635416, "xmin": null}, "transaction": null, "op": "u", "ts_ms": 1792076485962, "ts_us": 1792076485962407, "ts_ns": 1792076485962407267}}
CDC EVENT d: {"key": {"transaction_id": 8}, "value": {"before": {"transaction_id": 8, "account_id": 2, "card_id": "watermark", "amount_cents": 1, "occurred_at": 1735689900000}, "after": null, "source": {"version": "2.7.3.Final", "connector": "postgresql", "name": "payments", "ts_ms": 1792076485801, "snapshot": "false", "db": "payments", "sequence": "[\"26635632\",\"26635632\"]", "ts_us": 1792076485801425, "ts_ns": 1792076485801425000, "schema": "public", "table": "transactions", "txId": 765, "lsn": 26635632, "xmin": null}, "transaction": null, "op": "d", "ts_ms": 1792076485962, "ts_us": 1792076485962810, "ts_ns": 1792076485962810144}}
```

Os eventos completos também estão em [cdc.json](evidence/cdc.json). O update levou `amount_cents` de 1000 a 7777; o delete tem `after: null`.

### 4. Enriquecimento e fraude

```text
CDC: schemaless Debezium envelope verified
FRAUD: overlapping rolling 60s alerts crossing clock-minute boundary
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
      "risk_level": "medium"
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
      "risk_level": "medium"
    }
  }
]
ENRICHMENT: account joined; UPDATE propagated; DELETE tombstone propagated
```

As compras 1/2/3 ocorreram a 50s, 70s e 109s (59s entre primeira e terceira); 2/3/4 ocorreram a 70s, 109s e 110s. Foram observados os alertas 3 e 4, com nome e risco da conta. O cenário de controle usa outro cartão a 120s/150s/181s (61s). Veja [alertas Avro desserializados](evidence/alerts.json) e [eventos enriquecidos com update/tombstone](evidence/enriched.json).

### 5. Operação e remoção

```text
OPERATIONS: Flink jobs RUNNING; checkpoint counts {"restored": 0, "total": 2, "in_progress": 0, "completed": 1, "failed": 1}
OPERATIONS: replication slot
   slot_name   | active | lag_bytes
---------------+--------+-----------
 payments_slot | t      |    281096
(1 row)


PASS: real Postgres -> Debezium -> Kafka -> Flink SQL -> Kafka/Avro/Schema Registry
```

O job estava `RUNNING`; o checkpoint concluído levou 75 ms e confirmou os oito subtasks. **O contador também registra uma tentativa inicial falha**: o primeiro disparo ocorreu antes de todos os tasks estarem rodando, conforme o [diagnóstico real](evidence/initial-checkpoint.txt). Não houve restauração nesse teste. Consulte [checkpoints](evidence/checkpoints.json), [jobs](evidence/jobs.json) e [teardown com volume removido](evidence/teardown.txt).

Para verificar o tempo faturável registrado, foi executado:

```bash
gh api repos/mariomoutinho/payments-cdc-flink/actions/runs/38756328476/timing
```

```json
{"billable":{"UBUNTU":{"total_ms":0,"jobs":2,"job_runs":[{"job_id":116495633747,"duration_ms":0},{"job_id":116495634231,"duration_ms":0}]}},"run_duration_ms":129000}
```

Este resultado comprova as cinco camadas do **laboratório local**. A exigência específica de executar CDC e Flink no **Confluent Cloud**, testar ACLs lá e medir sua cobrança real continua pendente de uma conta com acesso e orçamento. A validação estática de Terraform não substitui essa execução.
