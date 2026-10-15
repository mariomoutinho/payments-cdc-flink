"""Deterministic training on synthetic labels, with an independent held-out split."""
import argparse
import hashlib
import json
import random
from pathlib import Path
from .model import features,fingerprint,predict,sigmoid

def dataset(seed=42):
    rng=random.Random(seed);rows=[]
    for _ in range(3000):
        row={'temperature_c':rng.randint(-20,140),'battery_pct':rng.randint(0,100),'criticality':rng.choice(['low','medium','high'])}
        x=features(row)
        probability=sigmoid(-3.5+2.7*x[0]+2.0*x[1]+0.7*x[2])
        rows.append((row,int(rng.random()<probability)))
    rng.shuffle(rows)
    return rows[:2400],rows[2400:]

def evaluate(model,rows):
    scored=[(predict(model,row),label) for row,label in rows]
    tp=sum(p>=0.5 and y==1 for p,y in scored);fp=sum(p>=0.5 and y==0 for p,y in scored);fn=sum(p<0.5 and y==1 for p,y in scored)
    positives=[p for p,y in scored if y];negatives=[p for p,y in scored if not y]
    auc=sum((a>b)+0.5*(a==b) for a in positives for b in negatives)/(len(positives)*len(negatives))
    return {'accuracy':round(sum((p>=0.5)==bool(y) for p,y in scored)/len(scored),6),'f1':round(2*tp/(2*tp+fp+fn),6),'roc_auc':round(auc,6)}

def train():
    training,testing=dataset();examples=[([1.0]+features(row),label) for row,label in training];weights=[0.0]*4
    for _ in range(2000):
        gradient=[0.0]*4
        for x,y in examples:
            error=sigmoid(sum(a*b for a,b in zip(weights,x)))-y
            for i in range(4):gradient[i]+=error*x[i]
        weights=[w-0.2*(g/len(examples)+(0.001*w if i else 0)) for i,(w,g) in enumerate(zip(weights,gradient))]
    model={'algorithm':'logistic_regression','feature_version':1,'weights':[round(w,12) for w in weights],'seed':42,'training_samples':2400,'held_out_samples':600,'dataset_sha256':hashlib.sha256(json.dumps(training+testing,sort_keys=True).encode()).hexdigest(),'scope':'Synthetic IoT demo only; not validated on real equipment or payment fraud'}
    model['model_version']=fingerprint(model);model['held_out_metrics']=evaluate(model,testing)
    return model

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',default='models/iot-risk.json');args=parser.parse_args()
    model=train();path=Path(args.output);path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(model,indent=2)+'\n')
    print(json.dumps({'model_version':model['model_version'],'training_samples':2400,'held_out_samples':600,'metrics':model['held_out_metrics']},sort_keys=True))
if __name__=='__main__':main()
