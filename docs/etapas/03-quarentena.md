# Etapa 3 — eventos defeituosos em quarentena

O worker lê `iot.public.telemetry`, valida chave/envelope, limites físicos (-80..200°C), bateria (0..100%) e timestamp. Encaminha dados válidos para `iot.validated.telemetry`, preservando c/u/d/tombstones; erros de conteúdo vão para `iot.dlq`, com motivo, tópico, partição, offset e bytes originais em base64. Base64 não é criptografia: limite acesso e retenção da DLQ em produção.

A saída e o offset do worker são confirmados na mesma transação Kafka. Consumidores usam read_committed; o ID transacional é estável e limita o serviço a uma instância. Isso protege esta fronteira de replay, sem declarar exactly-once de Postgres até efeitos externos. Erros operacionais de Kafka/DB não são descartados como registros defeituosos.

O teste injeta JSON inválido, insere temperatura -300°C pelo Postgres, observa ambos os motivos na DLQ, reinicia o worker e exige que uma leitura válida posterior chegue enriquecida, com jobs Flink e CDC RUNNING. Registros rejeitados não devem ser reproduzidos automaticamente: corrigir causa e planejar replay, especialmente se uma atualização tiver imagem anterior inválida.

Execução: `./setup.sh`; testes de contrato: `python -m unittest discover -s tests -v`. Artefatos: `iot-dlq.json`, `iot-validation.txt` e log do worker. O teardown inclui os serviços do perfil iot-workers. Esta proteção é entre CDC/Kafka e Flink; não faz um conector de origem continuar quando a conexão com Postgres falha.
