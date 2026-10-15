# Fundação opcional no Confluent Cloud

Este módulo cria ambiente, cluster Basic, tópicos, service accounts/admin/escritor/leitor, chaves e ACLs. Schema Registry é descoberto no ambiente. O compute pool de até 5 CFU é opcional. **Não implanta o conector CDC ou os statements Flink e não foi aplicado nesta entrega.** A versão local comprovada fica na raiz.

Baseado nos exemplos oficiais [basic-kafka-acls](https://github.com/confluentinc/terraform-provider-confluent/tree/master/examples/configurations/basic-kafka-acls) e [flink-quickstart](https://github.com/confluentinc/terraform-provider-confluent/tree/master/examples/configurations/flink-quickstart), com provider fixado em 2.91.0.

Precisa de Terraform >=1.6, conta com permissão de provisionamento, Cloud API key disponibilizada pelas variáveis `CONFLUENT_CLOUD_API_KEY` e `CONFLUENT_CLOUD_API_SECRET`, região habilitada e orçamento. Use seu gerenciador de segredos para definir essas variáveis; nunca coloque os valores no Git.

```bash
cd infra/cloud
terraform init
terraform validate
terraform plan -out=payments.tfplan
terraform apply payments.tfplan
terraform output endpoints
# Opcional: cria recurso cobrável adicional.
# terraform apply -var=create_compute_pool=true
```

Plan, state e outputs de credenciais contêm segredos; são ignorados pelo Git. Não envie logs de `terraform output -json` ou `terraform output -raw kafka_credentials` para evidências públicas. Validação estática não comprova permissões reais: só após apply teste que writer produz, reader consome com grupo `payments-reader-*`, e reader não produz.

Os tópicos estão pré-criados. O writer pode escrever no prefixo `payments.public.` e o reader pode ler `payments.`; não podem administrar cluster. A conta admin serve só para provisionamento e não deve ser usada pelo consumidor. CDC gerenciado e Flink precisam de identidades próprias e permissões para seus recursos; não usar as chaves admin em aplicações.

Após remover manualmente qualquer conector/statement criado fora deste módulo, remova os recursos gerenciados por ele:

```bash
terraform plan -destroy -out=destroy.tfplan
terraform apply destroy.tfplan
```

Não apague o state antes de destruir recursos. `docker compose down` não remove recursos Cloud. Verifique Billing/Usage por ambiente após o encerramento; a região e a duração afetam o custo. Nunca declarar custo de nuvem medido sem executar e consultar o faturamento.
