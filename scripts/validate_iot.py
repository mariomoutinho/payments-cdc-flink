"""Real IoT integration: independent CDC source and Avro enriched projection."""
import json
import secrets
import time
from validate import ROOT, ART, command, sql, http, wait_http, consume
import requests

def main():
    topics=['iot.public.devices','iot.public.telemetry','__debezium-heartbeat.iot','iot.enriched','iot.alerts.velocity']
    for topic in topics:
        command(['docker','compose','exec','-T','kafka','kafka-topics','--bootstrap-server','kafka:29092','--create','--if-not-exists','--topic',topic,'--partitions','1','--replication-factor','1'])
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
    sql("SET ROLE payments_writer; INSERT INTO devices VALUES ('sensor-hot','factory-a','high'),('sensor-safe','factory-b','low'),('clock','test','low');")
    base=1735689600000
    readings=[(1,'sensor-hot',85,30,50),(2,'sensor-hot',90,25,70),(3,'sensor-hot',95,20,109),(4,'sensor-safe',22,95,120),(5,'sensor-safe',24,95,150),(6,'sensor-safe',23,94,181),(7,'clock',20,100,1200)]
    values=','.join(f"({rid},'{device}',{temp},{battery},{base+seconds*1000})" for rid,device,temp,battery,seconds in readings)
    sql('SET ROLE payments_writer; INSERT INTO telemetry VALUES '+values+';')
    enriched=consume('iot.enriched',lambda xs:{1,2,3,4,5,6,7}<={x['key']['reading_id'] for x in xs},avro=True,avro_key=True)
    assert next(x['value']['site_id'] for x in enriched if x['key']['reading_id']==3)=='factory-a'
    (ART/'iot-enriched.json').write_text(json.dumps(enriched,indent=2))
    alerts=consume('iot.alerts.velocity',lambda xs:any(x['value'] and x['value']['reading_id']==3 for x in xs))
    assert all(x['value']['device_id']=='sensor-hot' for x in alerts if x['value']),alerts
    (ART/'iot-velocity.json').write_text(json.dumps(alerts,indent=2))
    print('IOT: Avro key/value enriched with factory-a/high; 7 readings observed',flush=True)
    print('IOT VELOCITY: '+json.dumps(alerts),flush=True)
    sql('SET ROLE payments_writer; UPDATE telemetry SET temperature_c=96 WHERE reading_id=3; DELETE FROM telemetry WHERE reading_id=7;')
    updated=consume('iot.enriched',lambda xs:any(x['value'] and x['value']['reading_id']==3 and x['value']['temperature_c']==96 for x in xs) and any(x['key']=={'reading_id':7} and x['value'] is None for x in xs),avro=True,avro_key=True)
    (ART/'iot-updates.json').write_text(json.dumps(updated,indent=2))
    status=http('GET','http://localhost:8083/connectors/iot-cdc/status')
    assert all(t['state']=='RUNNING' for t in status['tasks']),status
    print('IOT: UPDATE and DELETE propagated; connector RUNNING',flush=True)
    print('PASS: IoT CDC, Avro enrichment and velocity detection',flush=True)

if __name__=='__main__':main()
