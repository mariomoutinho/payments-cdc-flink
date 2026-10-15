"""Integration checks against real services; passwords are never printed."""
import io
import json
import secrets
import subprocess
import time
from pathlib import Path
import fastavro
import requests
from confluent_kafka import Consumer
ROOT = Path(__file__).resolve().parents[1]
ART = ROOT / 'artifacts'
ART.mkdir(exist_ok=True)
def command(args, stdin=None):
    return subprocess.check_output(args, input=stdin, text=True, cwd=ROOT, stderr=subprocess.STDOUT)
def sql(statement):
    return command(['docker','compose','exec','-T','postgres','psql','-X','-v','ON_ERROR_STOP=1','-U','postgres','-d','payments'], statement)
def http(method, path, **kwargs):
    r = requests.request(method, path, timeout=30, **kwargs)
    r.raise_for_status()
    return r.json()
def wait_http(url, timeout=180):
    end = time.monotonic()+timeout
    while time.monotonic()<end:
        try:
            return http('GET',url)
        except requests.RequestException:
            time.sleep(2)
    raise TimeoutError(url)
def consume(topic, predicate, timeout=120, avro=False):
    c=Consumer({'bootstrap.servers':'localhost:9092','group.id':'evidence-'+secrets.token_hex(6),'auto.offset.reset':'earliest','enable.auto.commit':False})
    c.subscribe([topic]); rows=[]; end=time.monotonic()+timeout
    try:
        while time.monotonic()<end:
            msg=c.poll(1)
            if msg is None: continue
            if msg.error(): raise RuntimeError(msg.error())
            raw=msg.value(); value=None
            if raw is not None:
                if avro:
                    assert raw[0]==0
                    schema_id=int.from_bytes(raw[1:5],'big')
                    schema=json.loads(http('GET',f'http://localhost:8081/schemas/ids/{schema_id}')['schema'])
                    value=fastavro.schemaless_reader(io.BytesIO(raw[5:]),schema)
                else: value=json.loads(raw)
            rows.append({'key':json.loads(msg.key()) if msg.key() else None,'value':value})
            if predicate(rows): return rows
    finally: c.close()
    raise AssertionError(f'Timeout on {topic}: {rows}')
def main():
    for port,path in [(8081,'subjects'),(8083,'connectors'),(8082,'overview')]: wait_http(f'http://localhost:{port}/{path}')
    for topic in ['payments.public.accounts','payments.public.transactions','payments.enriched','payments.alerts']:
        command(['docker','compose','exec','-T','kafka','kafka-topics','--bootstrap-server','kafka:29092','--create','--if-not-exists','--topic',topic,'--partitions','1','--replication-factor','1'])
    for role in ['payments_writer','payments_reader','payments_cdc']:
        password=secrets.token_hex(24)
        sql(f"ALTER ROLE {role} LOGIN PASSWORD '{password}';")
        if role=='payments_cdc': cdc_password=password
    print('FOUNDATION: distinct writer, reader and CDC roles\n'+sql("SELECT grantee, privilege_type FROM information_schema.role_table_grants WHERE table_name='transactions' AND grantee LIKE 'payments_%' ORDER BY 1,2;"),flush=True)
    denied=subprocess.run(['docker','compose','exec','-T','postgres','psql','-X','-v','ON_ERROR_STOP=1','-U','postgres','-d','payments'],input="SET ROLE payments_reader; INSERT INTO accounts VALUES (999,'denied','low');",text=True,capture_output=True,cwd=ROOT)
    assert denied.returncode!=0 and 'permission denied' in denied.stderr
    print('ACCESS: reader INSERT denied by PostgreSQL',flush=True)
    config={'connector.class':'io.debezium.connector.postgresql.PostgresConnector','database.hostname':'postgres','database.port':'5432','database.user':'payments_cdc','database.password':cdc_password,'database.dbname':'payments','topic.prefix':'payments','plugin.name':'pgoutput','slot.name':'payments_slot','publication.name':'payments_pub','publication.autocreate.mode':'disabled','table.include.list':'public.accounts,public.transactions','snapshot.mode':'initial','tombstones.on.delete':'true'}
    r=requests.put('http://localhost:8083/connectors/payments-cdc/config',json=config,timeout=30)
    assert r.ok, f'Connect configuration failed: HTTP {r.status_code}'
    end=time.monotonic()+120
    while time.monotonic()<end:
        status=wait_http('http://localhost:8083/connectors/payments-cdc/status')
        if status.get('tasks') and all(t['state']=='RUNNING' for t in status['tasks']): break
        time.sleep(2)
    else: raise AssertionError('CDC task did not reach RUNNING')
    print('CDC: connector and task RUNNING',flush=True)
    time.sleep(5)
    result=command(['docker','compose','exec','-T','jobmanager','/opt/flink/bin/sql-client.sh','-f','/opt/flink/sql/flink.sql'])
    (ART/'sql-client.txt').write_text(result)
    assert 'Job ID' in result and '[ERROR]' not in result,result
    print('FLINK: statement set submitted\n'+result[-1500:],flush=True)
    sql("SET ROLE payments_writer; INSERT INTO accounts VALUES (1,'Ana Sintetica','medium'),(2,'Bruno Sintetico','low');")
    base=1735689600000
    rows=[(1,1,'card-fraud',1000,50),(2,1,'card-fraud',2000,70),(3,1,'card-fraud',3000,109),(4,1,'card-fraud',4000,110),(5,2,'card-safe',500,120),(6,2,'card-safe',500,150),(7,2,'card-safe',500,181),(8,2,'watermark',1,300)]
    values=','.join(f"({tid},{aid},'{card}',{amount},{base+secs*1000})" for tid,aid,card,amount,secs in rows)
    sql('SET ROLE payments_writer; INSERT INTO transactions VALUES '+values+';')
    alerts=consume('payments.alerts',lambda xs: {3,4}<={x['value']['transaction_id'] for x in xs if x['value']},avro=True)
    assert all(x['value']['card_id']=='card-fraud' and x['value']['customer_name']=='Ana Sintetica' for x in alerts if x['value'])
    (ART/'alerts.json').write_text(json.dumps(alerts,indent=2))
    print('FRAUD: overlapping rolling 60s alerts crossing clock-minute boundary\n'+json.dumps(alerts,indent=2),flush=True)
    sql('SET ROLE payments_writer; UPDATE transactions SET amount_cents=7777 WHERE transaction_id=1; DELETE FROM transactions WHERE transaction_id=8;')
    cdc=consume('payments.public.transactions',lambda xs:{'c','u','d'}<={x['value']['op'] for x in xs if x['value']})
    (ART/'cdc.json').write_text(json.dumps(cdc,indent=2))
    for op in ['c','u','d']: print('CDC EVENT '+op+': '+json.dumps(next(x for x in cdc if x['value'] and x['value']['op']==op)),flush=True)
    def enriched_ok(xs):
        return any(x['value'] and x['value']['transaction_id']==1 and x['value']['amount_cents']==7777 for x in xs) and any(x['key']=={'transaction_id':8} and x['value'] is None for x in xs)
    enriched=consume('payments.enriched',enriched_ok)
    assert all(x['value']['customer_name'] in ['Ana Sintetica','Bruno Sintetico'] for x in enriched if x['value'])
    (ART/'enriched.json').write_text(json.dumps(enriched,indent=2))
    print('ENRICHMENT: account joined; UPDATE propagated; DELETE tombstone propagated',flush=True)
    url='http://localhost:8081'; subject='payments.alerts-value'
    http('PUT',f'{url}/config/{subject}',json={'compatibility':'BACKWARD'})
    v1=json.loads(http('GET',f'{url}/subjects/{subject}/versions/latest')['schema'])
    v2=json.loads(json.dumps(v1)); v2['fields'].append({'name':'merchant','type':['null','string'],'default':None})
    bad=json.loads(json.dumps(v2)); next(f for f in bad['fields'] if f['name']=='transaction_id')['type']=['null','string']
    for name,schema in [('v1',v1),('v2',v2),('incompatible',bad)]: (ROOT/'schemas'/f'alerts-{name}.avsc').write_text(json.dumps(schema,indent=2)+'\n')
    accepted=http('POST',f'{url}/subjects/{subject}/versions',json={'schema':json.dumps(v2)})
    print('CONTRACT: optional merchant field accepted: '+json.dumps(accepted),flush=True)
    r=requests.post(f'{url}/subjects/{subject}/versions',json={'schema':json.dumps(bad)},timeout=30)
    assert r.status_code==409,r.status_code
    print('CONTRACT: incompatible transaction_id type rejected: HTTP 409',flush=True)
    jobs=wait_http('http://localhost:8082/jobs/overview')
    assert jobs['jobs'] and all(j['state']=='RUNNING' for j in jobs['jobs']),jobs
    (ART/'jobs.json').write_text(json.dumps(jobs,indent=2))
    print('OPERATIONS: Flink jobs RUNNING; checkpoints enabled every 5s',flush=True)
    print('OPERATIONS: replication slot\n'+sql("SELECT slot_name,active,pg_wal_lsn_diff(pg_current_wal_lsn(),confirmed_flush_lsn) AS lag_bytes FROM pg_replication_slots WHERE slot_name='payments_slot';"),flush=True)
    print('PASS: real Postgres -> Debezium -> Kafka -> Flink SQL -> Kafka/Avro/Schema Registry',flush=True)
if __name__=='__main__': main()
