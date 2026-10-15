# Etapa 2 — segunda regra, janela e critério diferentes

Além da velocidade (três leituras em 60 segundos), adiciona média de temperatura >=80°C com pelo menos duas leituras em janela tumbling de 5 minutos. Saída: `iot.alerts.thermal`, com dispositivo, início/fim da janela, contagem e média. O critério detecta calor sustentado, sem sinalizar um pico isolado como média alta.

O teste real usa sensor-hot (85/90/95°C → média 90°C), sensor-safe (22/24/23°C → 23°C) e sensor-spike (100/20°C → 60°C). Um evento de relógio fecha as janelas por event time. Após observar o alerta positivo, o consumidor continua coletando por 3s e exige ausência de alertas dos controles. Updates posteriores não reescrevem o histórico append-only da regra.

Execução: `./setup.sh`; evidências: `iot-thermal.json` e `iot-validation.txt` no artefato `pipeline-evidence` do Actions. Valores de limiar são sintéticos para demonstrar processamento; não representam uma especificação de segurança de equipamento real.

Validação real aprovada: https://github.com/mariomoutinho/payments-cdc-flink/actions/runs/38760916538 (commit `a1dc4b0`). Saída: [iot-validation.txt](../../evidence/steps/02/iot-validation.txt).
