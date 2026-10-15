import json
import unittest
from pathlib import Path
from ai.model import predict
from ai.train import dataset,evaluate

class RiskModelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.model=json.loads(Path('models/iot-risk.json').read_text())
    def test_reproduces_held_out_metrics(self):
        _,held_out=dataset()
        self.assertEqual(evaluate(self.model,held_out),self.model['held_out_metrics'])
        self.assertGreater(self.model['held_out_metrics']['roc_auc'],0.8)
    def test_hot_sensor_ranks_above_normal(self):
        hot=predict(self.model,{'temperature_c':95,'battery_pct':20,'criticality':'high'})
        normal=predict(self.model,{'temperature_c':23,'battery_pct':94,'criticality':'low'})
        self.assertGreater(hot,0.8);self.assertLess(normal,0.4)
    def test_invalid_features_and_changed_model_rejected(self):
        for value in [float('nan'),float('inf'),True,None]:
            with self.assertRaises(ValueError):predict(self.model,{'temperature_c':value,'battery_pct':90,'criticality':'low'})
        changed=self.model|{'weights':[0,0,0,0]}
        with self.assertRaises(ValueError):predict(changed,{'temperature_c':20,'battery_pct':90,'criticality':'low'})

if __name__=='__main__':unittest.main()
