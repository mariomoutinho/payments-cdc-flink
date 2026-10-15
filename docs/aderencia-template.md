# Aderência ao exemplo fornecido

O exemplo define o alvo Confluent Cloud. Este repositório demonstra uma execução local e mantém essa diferença explícita. Não existe evidência de Neon, conector gerenciado, Metrics API ou Billing API Confluent. O módulo Terraform foi validado estaticamente e não aplicado.

| Item do exemplo | Implementação neste repositório | Limite |
|---|---|---|
| `./setup.sh` e `.env.example` | Mesmo entrypoint usado no CI; configura laboratório e executa validação | Precisa de Docker funcional e ambiente novo |
| Neon → Confluent Cloud | PostgreSQL 16 local → Debezium → Kafka local | Não comprova execução gerenciada |
| Identidades produtor/consumidor | Roles locais distintas e Terraform com service accounts/ACLs separados | ACLs Kafka Cloud ainda não testadas |
| Namespace, decimal, PII/PCI | Contrato de alertas `com.bootcamp.payments.fraud`, `money` como bytes decimal(15,2), tags inline | CDC bruto continua JSON; tags classificam, não criptografam |
| Evolução BACKWARD | Campo novo com default; campo obrigatório sem default e mudança de tipo incompatíveis | Remover campo do leitor pode ser BACKWARD compatível |
| Seed=42, cinco tabelas | Gerador determinístico com contagens 200/240/320/60/2000 antes dos testes vivos | Contas contêm nome desnormalizado; customers/cards/merchants não são fontes separadas do Flink |
| CDC c/u/d + tombstone | Validador captura envelopes completos e exige tombstone após delete | Conector local Java, não `PostgresCdcSourceV2` |
| Heartbeat 60 segundos | `heartbeat.interval.ms=60000` no conector | No Cloud será necessária configuração específica do serviço |
| Temporal join | Join de estado atual entre transações CDC e contas CDC | Não é enriquecimento histórico AS OF; mudança da conta pode atualizar a projeção |
| MATCH_RECOGNIZE | Três compras consecutivas em 60s, sequências sobrepostas, controle de 61s | Usa `(A B C)`, em vez de `(T{3,}?)`; snapshots não geram alertas |
| Idle timeout | `table.exec.source.idle-timeout=5 s`, configuração Apache Flink local | `sql.tables.scan.idle-timeout` é configuração do serviço gerenciado |
| Consumo com efeito idempotente | SQLite PRIMARY KEY + UPSERT, persistência antes de commit do offset | Não cobre efeitos externos como envio de dinheiro ou notificações |
| Métricas e custo Cloud | Capturas locais de jobs/checkpoints, WAL e lag por grupo; tempo faturável Actions | Pico de received_bytes e billing Confluent não medidos |
| Teardown e listas vazias | Script verifica containers e volumes do projeto vazios | Não lista/exclui recursos Cloud, pois não os criou |

## Compatibilidade: correção necessária no exemplo

BACKWARD significa que um leitor com schema novo consegue ler dados escritos com o schema anterior. Campos do writer que não existem no reader podem ser ignorados. Por isso, não se deve fabricar uma rejeição de remoção de campo obrigatório sob BACKWARD. O teste consulta a API real: remoção de `transaction_id` no novo reader é compatível; adicionar campo obrigatório sem default não é. A mudança de long para string também é rejeitada.

Referências: [resolução de schemas Avro](https://avro.apache.org/docs/1.7.5/spec.html#Schema+Resolution), [padrão oficial de compatibilidade](https://developer.confluent.io/patterns/event-stream/schema-compatibility/), [tags inline](https://docs.confluent.io/platform/current/schema-registry/fundamentals/data-contracts.html). No Cloud, definições de tags precisam existir no catálogo antes do registro. Tokens sintéticos de cartão não contêm PAN ou CVV.

## Operação: quatro respostas

### 1. Observabilidade

`artifacts/validation.txt` registra o estado dos jobs e dos checkpoints, atraso do slot WAL e `kafka-consumer-groups --describe` para o grupo `payments-reader-idempotence-b`. O consumidor confirma offsets depois de persistir o efeito. Isso mede lag no instante da coleta; não é pico de lag durante toda a execução. Não coletamos `received_bytes` da Metrics API Confluent. Em produção, acompanhar taxa de bytes recebidos, lag por grupo, falhas de checkpoint e retenção de WAL, correlacionando timestamps e identificadores do ambiente.

### 2. Segurança

| Identidade | Permissões | Por quê |
|---|---|---|
| `postgres` local | Administração, apenas bootstrap/validação | Cria schema e grants; não é o login do CDC |
| `payments_writer` local | SELECT/INSERT/UPDATE/DELETE nas cinco tabelas | Carrega seed e demonstra mudanças |
| `payments_reader` local | SELECT | Consumo sem mutação; INSERT negado no teste |
| `payments_cdc` local | LOGIN/REPLICATION e SELECT em accounts/transactions | Leitura da publicação explícita sem superuser |
| Cloud `payments-writer` | WRITE em `payments.public.` (PREFIXED) | Produção apenas no namespace de origem |
| Cloud `payments-reader` | READ em `payments.` e grupo `payments-reader-` | Leitura dos tópicos e dos offsets do seu grupo |
| Cloud `payments-admin` | CloudClusterAdmin | Só provisionamento; nenhum consumidor deve usá-la |

As três últimas linhas descrevem Terraform, não permissões exercitadas no Cloud. Kafka local usa PLAINTEXT e não autentica essas service accounts. As tags PII/PCI não substituem ACL, TLS, mascaramento ou criptografia.

### 3. Custos

O README mantém o tempo faturável informado pela API do GitHub e distingue essa medição de um `billing cost list` Confluent, que não foi executado. Duas alavancas para Cloud: (1) parar statements, remover conector e compute pool logo após a validação; (2) limitar CFU, retenção e número de partições, ajustando volume e capacidade ao laboratório. As alavancas devem ser verificadas contra o faturamento real, não usadas para declarar economia não medida.

### 4. Confiabilidade

| Trecho | Garantia e risco | Tratamento |
|---|---|---|
| Postgres → WAL | Mudanças confirmadas têm posição de log; slot parado retém WAL | Monitorar slot e bytes; não apagar slot ativo arbitrariamente |
| WAL → Debezium → Kafka | Retomada por offsets pode reemitir mudanças | PKs estáveis e projeções de estado com UPSERT |
| Kafka → Flink | Checkpoints permitem retomada; estado local é efêmero | Checkpoints observados; produção exige storage durável e restauração testada |
| Flink → Kafka | Sinks atuais não estabelecem exactly-once ponta a ponta | Chave determinística `transaction_id`; consumidor aceita replay |
| Kafka → SQLite | Efeito é persistido antes do commit Kafka; falha entre passos reentrega evento | UPSERT por PK e commit manual do offset; teste aplica o mesmo par de alertas duas vezes e mantém duas linhas |
| SQLite → efeito externo | Fora da garantia deste consumidor | Pagamento/notificação precisaria de outbox ou chave idempotente no destino |

O elo fraco continua sendo o efeito final do consumidor. O código comprova idempotência da projeção SQLite, não de qualquer efeito arbitrário. A janela de fraude é baseada em compras observadas e não revoga alertas após uma correção/delete.
