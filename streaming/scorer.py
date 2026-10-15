"""Score every enriched IoT reading with a pinned, trained local model."""
import base64
import io
import json
import os
from pathlib import Path
import fastavro
import requests
from ai.model import fingerprint,predict
from .worker import clients,publish

ROOT=Path(__file__).resolve().parents[1]

def main():
    model=json.loads((ROOT/'models/iot-risk.json').read_text())
    if model['model_version']!=fingerprint(model):raise ValueError('model_fingerprint_mismatch')
    # Configuration/model failure must stop startup, not quarantine every source event.
    predict(model,{'temperature_c':25,'battery_pct':100,'criticality':'low'})
    schema=json.loads((ROOT/'schemas/iot-risk.avsc').read_text())
    registry=os.environ.get('SCHEMA_REGISTRY_URL','http://localhost:8081')
    response=requests.post(registry+'/subjects/iot.risk.scored-value/versions',json={'schema':json.dumps(schema)},timeout=30)
    response.raise_for_status();schema_id=response.json()['id'];cache={}
    consumer,producer=clients('iot-risk-model','iot-risk-model-v1')
    consumer.subscribe(['iot.enriched'])
    def decode(raw):
        if len(raw)<5 or raw[0]!=0:raise ValueError('invalid_avro_wire_format')
        writer_id=int.from_bytes(raw[1:5],'big')
        if writer_id not in cache:
            result=requests.get(f'{registry}/schemas/ids/{writer_id}',timeout=10);result.raise_for_status()
            cache[writer_id]=json.loads(result.json()['schema'])
        return fastavro.schemaless_reader(io.BytesIO(raw[5:]),cache[writer_id])
    try:
        while True:
            message=consumer.poll(1)
            if message is None:continue
            if message.error():raise RuntimeError(message.error())
            target='iot.risk.scored';key=message.key();output=None
            try:
                if message.value() is not None:
                    row=decode(message.value());row_key=decode(key)
                    if row_key['reading_id']!=row['reading_id']:raise ValueError('key_row_mismatch')
                    score=predict(model,row)
                    result={'reading_id':row['reading_id'],'device_id':row['device_id'],'risk_score':score,'risk_level':'high' if score>=0.8 else 'medium' if score>=0.4 else 'low','model_version':model['model_version'],'observed_at':row['observed_at'],'site_id':row['site_id']}
                    buffer=io.BytesIO();fastavro.schemaless_writer(buffer,schema,result)
                    output=b'\x00'+schema_id.to_bytes(4,'big')+buffer.getvalue()
            except (ValueError,KeyError,TypeError,EOFError) as error:
                target='iot.dlq';key=json.dumps(f'{message.topic()}:{message.partition()}:{message.offset()}').encode()
                output=json.dumps({'reason':'risk_invalid_record:'+type(error).__name__,'source_topic':message.topic(),'partition':message.partition(),'offset':message.offset(),'key_base64':base64.b64encode(message.key() or b'').decode(),'value_base64':base64.b64encode(message.value() or b'').decode()}).encode()
            publish(producer,consumer,message,target,key,output)
            print(json.dumps({'target':target,'source_offset':message.offset(),'model_version':model['model_version']}),flush=True)
    finally:
        consumer.close()
if __name__=='__main__':main()
