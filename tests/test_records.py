import json
import unittest
from streaming.records import InvalidRecord,validate_telemetry

class TelemetryContractTest(unittest.TestCase):
    def setUp(self):
        self.key=b'{"reading_id":1}'
        self.row={'reading_id':1,'device_id':'sensor-1','temperature_c':25,'battery_pct':90,'observed_at':1735689600000}
    def test_valid_create_update_and_tombstone(self):
        for op in ['c','r','u']:
            validate_telemetry(self.key,json.dumps({'op':op,'before':self.row,'after':self.row}).encode())
        validate_telemetry(self.key,json.dumps({'op':'d','before':self.row,'after':None}).encode())
        validate_telemetry(self.key,None)
    def test_rejects_malformed_and_wrong_key(self):
        for key,value in [(self.key,b'{bad'),(b'null',b'{}'),(b'{"reading_id":true}',None)]:
            with self.assertRaises(InvalidRecord):validate_telemetry(key,value)
    def test_business_errors_are_not_silently_accepted(self):
        for field,value in [('temperature_c',-300),('temperature_c',True),('battery_pct',101),('observed_at',-1),('device_id',' '),('reading_id',2)]:
            row=self.row|{field:value}
            with self.assertRaises(InvalidRecord,msg=field):validate_telemetry(self.key,json.dumps({'op':'c','after':row}).encode())

if __name__=='__main__':unittest.main()
