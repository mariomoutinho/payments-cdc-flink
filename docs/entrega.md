# Resumo da entrega

Repositório público: https://github.com/mariomoutinho/payments-cdc-flink

## O que foi entregue

Pipeline local PostgreSQL → Debezium CDC → Kafka → Flink SQL → Kafka, com saída de alertas Avro e Schema Registry. Dados sintéticos reproduzíveis, configurações Compose e conector, scripts SQL, teste integrado, CI e fundação opcional Terraform para Confluent Cloud.

## Prova de execução

Workflow aprovado: https://github.com/mariomoutinho/payments-cdc-flink/actions/runs/38756328476

Commit testado: `35516066b5a9695a72413ebb0c514d97a62b4e9e`.

O README contém saídas reais de permissões, eventos insert/update/delete, propagação de update e tombstone, dois alertas em sequências sobrepostas de 60s, evolução compatível aceita e incompatível rejeitada (HTTP 409), checkpoint concluído e lag de replicação. As evidências completas estão na pasta `evidence/`.

## Reprodução e encerramento

Seguir os comandos do README a partir de um clone limpo. O teste gera senhas aleatórias fora do Git. A remoção usa `docker compose down -v --remove-orphans` dentro do projeto. O CI comprovou a remoção do volume PostgreSQL ao terminar.

## Custo observado

129 segundos de duração total do workflow; a API do Actions informou 0 ms faturáveis para os dois jobs Ubuntu em repositório público. Nenhum recurso Confluent Cloud foi criado. Não há cobrança Cloud a apresentar nem estimativa inventada de implantação Cloud.

## Limitação da entrega

A versão comprovada é local. O módulo Cloud passou em `terraform validate`, mas não foi aplicado. Execução gerenciada, conector Cloud, statements Cloud, verificação de ACL e custo real Cloud exigem acesso à conta. Este resumo não deve ser apresentado como comprovação de execução no Confluent Cloud.
