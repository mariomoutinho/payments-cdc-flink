"""Atomic Kafka output+offset commits. A stable transactional ID fences stale workers."""
import os
from confluent_kafka import Consumer,Producer,TopicPartition

def clients(group,transactional_id):
    bootstrap=os.environ.get('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
    consumer=Consumer({'bootstrap.servers':bootstrap,'group.id':group,'auto.offset.reset':'earliest','enable.auto.commit':False,'enable.auto.offset.store':False,'isolation.level':'read_committed','broker.address.family':'v4'})
    producer=Producer({'bootstrap.servers':bootstrap,'transactional.id':transactional_id,'transaction.timeout.ms':30000,'enable.idempotence':True,'acks':'all','broker.address.family':'v4'})
    producer.init_transactions(60)
    return consumer,producer

def publish(producer,consumer,message,topic,key,value):
    producer.begin_transaction()
    try:
        errors=[]
        def delivered(error,_message):
            if error is not None:errors.append(error)
        producer.produce(topic,key=key,value=value,on_delivery=delivered)
        if producer.flush(30):raise TimeoutError('Kafka delivery did not complete')
        if errors:raise RuntimeError(errors[0])
        producer.send_offsets_to_transaction([TopicPartition(message.topic(),message.partition(),message.offset()+1)],consumer.consumer_group_metadata(),30)
        producer.commit_transaction(30)
    except Exception:
        producer.abort_transaction(30)
        raise
