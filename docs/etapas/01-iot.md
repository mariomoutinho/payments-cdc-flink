# Etapa 1 — domínio IoT

Adiciona dispositivos e telemetria com publicação e identidade CDC próprias. Leituras são enriquecidas por site e criticidade, com chave e valor Avro no tópico `iot.enriched`. A regra de velocidade detecta três leituras do mesmo dispositivo em 60s. O cenário inclui controle de 61s, atualização e exclusão de leitura.

O projeto conserva o laboratório original de pagamentos para regressão; o domínio ativo das cinco evoluções é IoT. Execute `./setup.sh` em ambiente limpo. Configuração: `connectors/iot-cdc.json`; SQL: `sql/iot.sql`; teste real: `scripts/validate_iot.py`. O mesmo teardown remove ambos os domínios.

Validações desta etapa: sintaxe Python e shell e execução integrada pelo workflow `Validate payment pipeline`, artefato `pipeline-evidence/iot-validation.txt`. O log precisa terminar em `PASS: IoT CDC, Avro enrichment and velocity detection`. Não há execução Cloud nesta etapa.

Validação real aprovada: https://github.com/mariomoutinho/payments-cdc-flink/actions/runs/38760323465 (commit `8cd802e`). Saída versionada: [iot-validation.txt](../../evidence/steps/01/iot-validation.txt).
