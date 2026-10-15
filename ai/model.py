"""Logistic regression inference with explicit, versioned feature engineering."""
import hashlib
import json
import math

def sigmoid(value):
    if value>=0:return 1/(1+math.exp(-value))
    exp=math.exp(value)
    return exp/(1+exp)

def features(row):
    for name,lo,hi in [('temperature_c',-80,200),('battery_pct',0,100)]:
        value=row.get(name)
        if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or not lo<=value<=hi:
            raise ValueError('invalid_model_feature:'+name)
    criticality={'low':0.0,'medium':0.5,'high':1.0}
    if row.get('criticality') not in criticality:raise ValueError('unknown_criticality')
    return [(row['temperature_c']-25)/25,(100-row['battery_pct'])/100,criticality[row['criticality']]]

def fingerprint(model):
    material={key:model[key] for key in ['weights','feature_version','dataset_sha256']}
    return 'iot-logreg-v1-'+hashlib.sha256(json.dumps(material,sort_keys=True).encode()).hexdigest()[:12]

def predict(model,row):
    if model['feature_version']!=1 or model['model_version']!=fingerprint(model):raise ValueError('unsupported_or_modified_model')
    x=features(row);weights=model['weights']
    if len(weights)!=4 or not all(math.isfinite(w) for w in weights):raise ValueError('invalid_model_weights')
    return sigmoid(weights[0]+sum(a*b for a,b in zip(weights[1:],x)))
