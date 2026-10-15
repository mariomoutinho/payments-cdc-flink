"""Quarantine malformed source data and preserve CDC envelopes for valid records."""
import base64
import json
from .records import InvalidRecord,validate_telemetry
from .worker import clients,publish

def main():
    consumer,producer=clients('iot-validator','iot-validator-v1')
    consumer.subscribe(['iot.public.telemetry'])
    try:
        while True:
            message=consumer.poll(1)
            if message is None:continue
            if message.error():raise RuntimeError(message.error())
            topic='iot.validated.telemetry';key=message.key();value=message.value()
            try:
                validate_telemetry(key,value)
            except InvalidRecord as error:
                topic='iot.dlq'
                key=json.dumps(f'{message.topic()}:{message.partition()}:{message.offset()}').encode()
                value=json.dumps({'reason':str(error),'source_topic':message.topic(),'partition':message.partition(),'offset':message.offset(),'key_base64':base64.b64encode(message.key() or b'').decode(),'value_base64':base64.b64encode(message.value()).decode() if message.value() is not None else None}).encode()
            publish(producer,consumer,message,topic,key,value)
            print(json.dumps({'target':topic,'source_offset':message.offset()}),flush=True)
    finally:
        consumer.close()
if __name__=='__main__':main()
