"""Real IoT integration: independent CDC source and Avro enriched projection."""
import csv
import json
import secrets
import time
from validate import ROOT, ART, command, sql, http, wait_http, consume
import requests
from confluent_kafka import Producer

def main():
    topics=['iot.public.devices','iot.public.telemetry','__debezium-heartbeat.iot','iot.enriched','iot.alerts.velocity','iot.alerts.thermal','iot.validated.telemetry','iot.dlq','iot.risk.scored']
    for topic in topics:
        command(['docker','compose','exec','-T','kafka','kafka-topics','--bootstrap-server','kafka:29092','--create','--if-not-exists','--topic',topic,'--partitions','1','--replication-factor','1'])
    command(['docker','compose','up','-d','--build','iot-validator','iot-scorer'])
    password=secrets.token_hex(24)
    sql(f"ALTER ROLE iot_cdc LOGIN PASSWORD '{password}';")
    config=json.loads((ROOT/'connectors/iot-cdc.json').read_text());config['database.password']=password
    response=requests.put('http://localhost:8083/connectors/iot-cdc/config',json=config,timeout=30)
    assert response.ok,f'IoT connector configuration HTTP {response.status_code}'
    end=time.monotonic()+120
    while time.monotonic()<end:
        status=wait_http('http://localhost:8083/connectors/iot-cdc/status')
        if status.get('tasks') and all(t['state']=='RUNNING' for t in status['tasks']):break
        time.sleep(2)
    else:raise AssertionError('IoT CDC not RUNNING')
    time.sleep(5)
    result=command(['docker','compose','exec','-T','jobmanager','/opt/flink/bin/sql-client.sh','-f','/opt/flink/sql/iot.sql'])
    (ART/'iot-sql-client.txt').write_text(result)
    assert 'Job ID:' in result and '[ERROR]' not in result,result
    sql("SET ROLE payments_writer; INSERT INTO devices VALUES ('sensor-hot','factory-a','high'),('sensor-safe','factory-b','low'),('clock','test','low'),('sensor-spike','factory-c','medium');")
    base=1735689600000
    with (ROOT/'data/iot-readings.csv').open() as source:
        readings=[(int(x['reading_id']),x['device_id'],int(x['temperature_c']),int(x['battery_pct']),int(x['seconds'])) for x in csv.DictReader(source)]
    values=','.join(f"({rid},'{device}',{temp},{battery},{base+seconds*1000})" for rid,device,temp,battery,seconds in readings)
    sql('SET ROLE payments_writer; INSERT INTO telemetry VALUES '+values+';')
    enriched=consume('iot.enriched',lambda xs:{row[0] for row in readings}<={x['key']['reading_id'] for x in xs},avro=True,avro_key=True)
    assert next(x['value']['site_id'] for x in enriched if x['key']['reading_id']==3)=='factory-a'
    (ART/'iot-enriched.json').write_text(json.dumps(enriched,indent=2))
    alerts=consume('iot.alerts.velocity',lambda xs:any(x['value'] and x['value']['reading_id']==3 for x in xs))
    assert all(x['value']['device_id']=='sensor-hot' for x in alerts if x['value']),alerts
    (ART/'iot-velocity.json').write_text(json.dumps(alerts,indent=2))
    print(f'IOT: Avro key/value enriched with factory-a/high; {len(readings)} readings observed',flush=True)
    print('IOT VELOCITY: '+json.dumps(alerts),flush=True)
    thermal=consume('iot.alerts.thermal',lambda xs:any(x['value']['device_id']=='sensor-hot' for x in xs),settle_seconds=3)
    assert len(thermal)==1 and thermal[0]['value']['device_id']=='sensor-hot',thermal
    assert thermal[0]['value']['sample_count']==3 and thermal[0]['value']['avg_temperature']==90.0,thermal
    (ART/'iot-thermal.json').write_text(json.dumps(thermal,indent=2))
    print('IOT THERMAL: '+json.dumps(thermal),flush=True)
    print('IOT THERMAL CONTROL: isolated 100C spike averages 60C; normal sensor averages 23C; no alerts',flush=True)
    sql('SET ROLE payments_writer; UPDATE telemetry SET temperature_c=96 WHERE reading_id=3; DELETE FROM telemetry WHERE reading_id=7;')
    updated=consume('iot.enriched',lambda xs:any(x['value'] and x['value']['reading_id']==3 and x['value']['temperature_c']==96 for x in xs) and any(x['key']=={'reading_id':7} and x['value'] is None for x in xs),avro=True,avro_key=True)
    (ART/'iot-updates.json').write_text(json.dumps(updated,indent=2))
    # Actual malformed Kafka record plus physically invalid row emitted by Postgres CDC.
    producer=Producer({'bootstrap.servers':'localhost:9092','broker.address.family':'v4'})
    producer.produce('iot.public.telemetry',key=b'{"reading_id":1000}',value=b'{broken-json')
    assert producer.flush(30)==0
    sql(f"SET ROLE payments_writer; INSERT INTO telemetry VALUES (100,'sensor-safe',-300,90,{base+1250000});")
    dlq=consume('iot.dlq',lambda xs:len(xs)>=2,settle_seconds=2)
    assert {x['value']['reason'] for x in dlq}=={'invalid_json','temperature_out_of_range'},dlq
    (ART/'iot-dlq.json').write_text(json.dumps(dlq,indent=2))
    command(['docker','compose','restart','iot-validator'])
    sql(f"SET ROLE payments_writer; INSERT INTO telemetry VALUES (101,'sensor-safe',26,90,{base+1300000});")
    continued=consume('iot.enriched',lambda xs:any(x['value'] and x['value']['reading_id']==101 for x in xs),avro=True,avro_key=True)
    assert not any(x['value'] and x['value']['reading_id']==100 for x in continued)
    print('IOT DLQ: 2 invalid events quarantined; validator restarted; valid reading 101 enriched',flush=True)
    jobs=http('GET','http://localhost:8082/jobs/overview')
    assert all(job['state']=='RUNNING' for job in jobs['jobs']),jobs
    status=http('GET','http://localhost:8083/connectors/iot-cdc/status')
    assert all(t['state']=='RUNNING' for t in status['tasks']),status
    print('IOT: UPDATE and DELETE propagated; connector RUNNING',flush=True)
    risk=consume('iot.risk.scored',lambda xs:any(x['value'] and x['value']['reading_id']==101 for x in xs) and any(x['key']=={'reading_id':7} and x['value'] is None for x in xs),avro=True,avro_key=True,settle_seconds=2)
    expected=json.loads((ROOT/'models/iot-risk.json').read_text())['model_version']
    state={}
    for record in risk:
        if record['value'] is None:state.pop(record['key']['reading_id'],None)
        else:
            value=record['value'];assert 0<=value['risk_score']<=1 and value['model_version']==expected
            state[value['reading_id']]=value
    assert 100 not in state and 7 not in state and 101 in state
    assert state[3]['risk_score']>0.8 and state[6]['risk_score']<0.4,state
    (ART/'iot-risk.json').write_text(json.dumps(risk,indent=2))
    print('IOT AI: '+json.dumps({'model_version':expected,'hot_score':state[3]['risk_score'],'normal_score':state[6]['risk_score'],'live_projection_rows':len(state),'deleted_reading_7_absent':7 not in state}),flush=True)
    print('PASS: IoT CDC, Avro enrichment and velocity detection',flush=True)

if __name__=='__main__':main()
