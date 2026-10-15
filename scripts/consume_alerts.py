"""SQLite projection: persist effect, then commit Kafka offset; replay is idempotent."""
import argparse
import io
import json
import sqlite3
import fastavro
import requests
from confluent_kafka import Consumer

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--db',default='artifacts/alerts.sqlite')
    parser.add_argument('--group',default='payments-reader-sqlite')
    parser.add_argument('--max-records',type=int,default=0)
    args=parser.parse_args()
    db=sqlite3.connect(args.db)
    db.execute('CREATE TABLE IF NOT EXISTS alerts (transaction_id INTEGER PRIMARY KEY, payload TEXT NOT NULL)')
    consumer=Consumer({'bootstrap.servers':'localhost:9092','group.id':args.group,'enable.auto.commit':False,'auto.offset.reset':'earliest','broker.address.family':'v4'})
    consumer.subscribe(['payments.alerts']); schemas={}; processed=0
    try:
        while args.max_records==0 or processed<args.max_records:
            msg=consumer.poll(30)
            if msg is None:
                if args.max_records: raise TimeoutError('No alert received in 30s')
                continue
            if msg.error(): raise RuntimeError(msg.error())
            key=json.loads(msg.key()); raw=msg.value()
            with db:
                if raw is None: db.execute('DELETE FROM alerts WHERE transaction_id=?',(key['transaction_id'],))
                else:
                    assert raw[0]==0
                    schema_id=int.from_bytes(raw[1:5],'big')
                    if schema_id not in schemas:
                        r=requests.get(f'http://localhost:8081/schemas/ids/{schema_id}',timeout=10);r.raise_for_status()
                        schemas[schema_id]=json.loads(r.json()['schema'])
                    value=fastavro.schemaless_reader(io.BytesIO(raw[5:]),schemas[schema_id])
                    db.execute('INSERT INTO alerts VALUES (?,?) ON CONFLICT(transaction_id) DO UPDATE SET payload=excluded.payload',(key['transaction_id'],json.dumps(value,default=str)))
            # Crash between these steps replays an already persisted UPSERT.
            consumer.commit(message=msg,asynchronous=False)
            processed+=1
        print(json.dumps({'processed':processed,'effect_rows':db.execute('SELECT count(*) FROM alerts').fetchone()[0]}))
    finally:
        consumer.close(); db.close()
if __name__=='__main__': main()
