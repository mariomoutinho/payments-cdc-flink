# Etapa 4 — materialização Apache Iceberg com Tableflow

Configuração real de provider em `infra/cloud/tableflow.tf`: habilita `confluent_tableflow_topic` para `iot.enriched`, formato ICEBERG, storage gerenciado e colunas de metadados PORTABLE. Registra previamente o contrato Avro usado no laboratório. Identidades e chaves de bootstrap são gerenciadas no state privado; permissões de aplicação continuam separadas.

**Status: configuração validada estaticamente, não aplicada.** Não há conta/chaves Confluent disponíveis; nenhuma tabela Iceberg Cloud foi criada, consultada ou faturada. O Kafka local não oferece Tableflow. Esta etapa não simula Tableflow com outro produto para fabricar evidência.

## Aplicar em uma conta autorizada

É necessário Confluent Cloud com APIs de provisionamento e Tableflow habilitadas na região escolhida. Disponibilize `CONFLUENT_CLOUD_API_KEY` e `CONFLUENT_CLOUD_API_SECRET` pelo gerenciador de segredos. A opção é desligada por padrão e cria recursos cobrados quando aplicada.

```bash
terraform -chdir=infra/cloud init
terraform -chdir=infra/cloud validate
terraform -chdir=infra/cloud plan -var=enable_tableflow=true -out=tableflow.tfplan
terraform -chdir=infra/cloud apply tableflow.tfplan
terraform -chdir=infra/cloud output iot_iceberg
```

Se o tópico já existe fora deste state, importe-o e reconcilie configuração antes do apply. Produza dados Avro reais em `iot.enriched` no cluster Cloud e acompanhe o status de Tableflow. **Este módulo não transporta dados do Kafka local para Cloud, nem implanta os workers ou o Flink gerenciado.** O tópico vazio e a criação de recurso não comprovam dados materializados.

Para concluir a evidência: capture o recurso pronto, `table_path`/`write_mode`, um snapshot Iceberg com registros conhecidos e consulta desses registros por um cliente do catálogo. Meça também o faturamento do período. O provider 2.91 expõe write_mode como campo calculado, não configurável; confirme o modo efetivo antes de interpretar updates/deletes. Não assuma que uma tabela de histórico equivale ao estado upsert. Descartar tombstones e contar todas as versões geraria resultados incorretos; reconcilie chaves e metadados de acordo com o modo retornado.

## Remover

```bash
terraform -chdir=infra/cloud plan -destroy -out=destroy.tfplan
terraform -chdir=infra/cloud apply destroy.tfplan
```

Isso remove a fundação gerenciada por este state. Remova antes statements/conectores que foram criados fora dele. Confirme Tableflow/cluster/ambiente removidos e consulte Billing/Usage. `./teardown.sh` só remove o laboratório local, não recursos Cloud.

Referências oficiais: [quickstart de managed storage](https://docs.confluent.io/cloud/current/topics/tableflow/get-started/quick-start-managed-storage.html), [exemplo Terraform oficial](https://github.com/confluentinc/terraform-provider-confluent/tree/master/examples/configurations/tableflow/confluent-managed-storage), [semântica CDC](https://docs.confluent.io/cloud/current/topics/tableflow/concepts/materialize-cdc.html).

Evidência estática real: [terraform-validate.txt](../../evidence/steps/04/terraform-validate.txt). Nenhum apply foi executado.

Workflow de regressão aprovado: https://github.com/mariomoutinho/payments-cdc-flink/actions/runs/38762327362 (commit `9f66423`). Esse resultado inclui validação Terraform e pipeline local, não ativação Tableflow.
