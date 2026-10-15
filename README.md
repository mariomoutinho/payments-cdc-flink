# Payments CDC + Flink SQL

Laboratório reproduzível: **PostgreSQL → Debezium CDC → Kafka → Flink SQL → Kafka + Avro/Schema Registry**.

Estado: implementação local em validação pelo GitHub Actions. **Não executado no Confluent Cloud**: nenhuma conta/chave foi fornecida. Não apresentar esta versão como evidência de execução na nuvem. As identidades locais são roles PostgreSQL; Kafka local usa PLAINTEXT em rede de laboratório, sem RBAC. A exigência de identidades Kafka distintas na nuvem permanece pendente.

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

Nenhum recurso Confluent Cloud foi criado: custo de serviço Confluent desta execução **US$ 0**. Isso não é uma cotação para executar na nuvem. O laboratório consome CPU, RAM, disco e rede da máquina ou runner; não há fatura de infraestrutura disponível. O uso de Actions pode consumir a franquia da conta: consulte Billing/Usage para o valor efetivamente cobrado. Não se deve inventar um custo Cloud sem medir.

Para remover o laboratório e os dados, execute **dentro deste repositório**:

```bash
docker compose down -v --remove-orphans
rm -f .env
```

Esse comando remove o volume PostgreSQL do projeto. Imagens Docker e cache de build continuam no host. Nenhum recurso de outros projetos deve ser removido. Na automação o teardown ocorre em `if: always()`.

## Evidências

As saídas reais serão adicionadas após o término da execução integrada. Se o workflow falhar, consulte os artefatos de diagnóstico: falha não é evidência de pipeline funcionando. Esta seção não declara execução bem-sucedida antecipadamente.
