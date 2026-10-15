"""IoT CDC contract checks; operational failures are not record errors."""
import json

class InvalidRecord(ValueError):
    pass

def integer(value):
    return isinstance(value,int) and not isinstance(value,bool)

def validate_telemetry(key_bytes,value_bytes):
    try:
        key=json.loads(key_bytes)
        if not isinstance(key,dict) or not integer(key.get('reading_id')) or key['reading_id']<=0:
            raise InvalidRecord('invalid_key')
        if value_bytes is None:return
        value=json.loads(value_bytes)
    except (ValueError,TypeError,UnicodeDecodeError) as error:
        if isinstance(error,InvalidRecord):raise
        raise InvalidRecord('invalid_json') from error
    if not isinstance(value,dict) or value.get('op') not in ['c','r','u','d']:
        raise InvalidRecord('invalid_operation')
    op=value['op']
    images=[value.get('before')] if op=='d' else [value.get('after')]
    if op=='u':images.append(value.get('before'))
    if op=='d' and value.get('after') is not None:raise InvalidRecord('delete_after_not_null')
    for row in images:
        if not isinstance(row,dict):raise InvalidRecord('missing_row_image')
        if row.get('reading_id')!=key['reading_id'] or not integer(row.get('reading_id')):
            raise InvalidRecord('key_row_mismatch')
        if not isinstance(row.get('device_id'),str) or not row['device_id'].strip():
            raise InvalidRecord('invalid_device')
        if not integer(row.get('temperature_c')) or not -80<=row['temperature_c']<=200:
            raise InvalidRecord('temperature_out_of_range')
        if not integer(row.get('battery_pct')) or not 0<=row['battery_pct']<=100:
            raise InvalidRecord('battery_out_of_range')
        if not integer(row.get('observed_at')) or not 0<=row['observed_at']<=253402300799999:
            raise InvalidRecord('invalid_timestamp')
