# Resumo da entrega — pipeline de pagamentos

Repositório público: https://github.com/mariomoutinho/payments-cdc-flink

Workflow aprovado: https://github.com/mariomoutinho/payments-cdc-flink/actions/runs/38758536328

Código testado: `5d483f13e2470e2d4785f6497c9ceb8d1f940bcd`.

## Entregue e executado

Setup/teardown automatizados; seed=42 com 200 clientes, 240 contas, 320 cartões, 60 estabelecimentos e 2000 transações; CDC com insert/update/delete/tombstone; enriquecimento de estado; fraude em sequências sobrepostas de 60s; alerta Avro com namespace com.bootcamp.payments.fraud, decimal(15,2), tags PII/PCI; testes de compatibilidade e consumidor SQLite com replay idempotente. Evidências completas e quatro pilares operacionais no README.

## Custo e encerramento

Computação do workflow público: US$ 0; tempo faturável informado pela API: 0 ms. Nenhum recurso Confluent criado. Teardown local verificou containers=[] volumes=[]. Não existe fatura Confluent deste laboratório.

## Limites em relação ao exemplo

Pipeline comprovado é local, CDC bruto JSON e join de estado atual. Neon/Confluent Cloud, CDC Avro gerenciado, temporal join e métricas/faturamento/teardown do serviço continuam pendentes. Terraform foi validado, não aplicado. Consultar `docs/aderencia-template.md` antes de submeter.

## Correção do template

Sob BACKWARD, remover campo no novo reader pode ser compatível; a API real comprovou isso. Foram rejeitados campo obrigatório novo sem default e mudança incompatível de tipo. Não foi fabricada uma rejeição para reproduzir uma expectativa incorreta.
