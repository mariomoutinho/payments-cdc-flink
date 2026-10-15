# Cinco etapas, cinco commits e cinco pushes

| Etapa | Descrição própria | Validação |
|---|---|---|
| 1 — IoT | [01-iot.md](01-iot.md) | CDC, enriquecimento Avro, velocidade, update/delete; Actions aprovado |
| 2 — segunda regra | [02-regra-termica.md](02-regra-termica.md) | Média 90°C em 5 min; controles 23°C e 60°C sem alerta; Actions aprovado |
| 3 — DLQ | [03-quarentena.md](03-quarentena.md) | JSON inválido e -300°C em quarentena; restart; leitura 101 passa; Actions aprovado |
| 4 — Iceberg/Tableflow | [04-tableflow.md](04-tableflow.md) | Provider validado; **não aplicado/materializado no Cloud** |
| 5 — IA | [05-ia.md](05-ia.md) | Treinamento e holdout reais; worker streaming testado pelo workflow final |

A sequência dos commits começa após `2917f84`. Cada etapa usa um único commit e um push normal; não há reescrita de histórico. Evidências de etapas anteriores são incorporadas no push seguinte, após o workflow terminar. A etapa final usa os artefatos e logs do workflow para preservar a contagem de cinco pushes.

Commit 1: `8cd802e`; commit 2: `a1dc4b0`; commit 3: `dfc0965`; commit 4: `9f66423`. O commit 5 é o último commit `feat(ai): etapa 5...` no histórico. Documentos não antecipam um hash ainda inexistente.

## Reproduzir

Preencha `.env` conforme o README e execute `./setup.sh`. São testados o laboratório original e as evoluções IoT. Ao terminar: `./teardown.sh`. Para testes sem infraestrutura: `python3 -m unittest discover -s tests -v`. Para Tableflow: siga a etapa 4, que exige conta, região, orçamento e credenciais Cloud.

No Actions, baixe `pipeline-evidence`: `iot-validation.txt`, `iot-thermal.json`, `iot-dlq.json`, `iot-risk.json`, logs dos workers e `teardown.txt`. O último deve terminar com `PASS: project containers=[] volumes=[]`. O custo de computação é o do runner público padrão; não há inferência paga externa nem Tableflow ativo. Não foram medidos custos de um serviço Cloud que não foi criado.
