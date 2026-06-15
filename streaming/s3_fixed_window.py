import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

def run():
    options = PipelineOptions(flags=[], streaming=True) 
    sub_path = "projects/gcp-learnings-498010/subscriptions/transactions-input-sub"
    
    print(f"🎬 Window Pipeline Active... Listening to {sub_path}...")
    
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'ReadFromPubSub'  >> beam.io.ReadFromPubSub(subscription=sub_path)
            | 'DecodeBytes'     >> beam.Map(lambda b: b.decode('utf-8'))
            | 'ParseJson'       >> beam.Map(json.loads)
            
            # 1. Bucket data into strict, fixed 10-second intervals
            | 'ApplyFixedWindow' >> beam.WindowInto(beam.window.FixedWindows(10))
            
            # 2. Extract country code as a key, and 1 as the count value
            | 'MapToPairs'       >> beam.Map(lambda data: (data.get('country'), 1))
            
            # 3. Sum up the occurrences inside each 10-second bucket
            | 'CountPerCountry'  >> beam.CombinePerKey(sum)
            
            | 'PrintWindowOut'   >> beam.Map(lambda res: print(f"📊 Window Closed! Result: {res}"))
        )

if __name__ == '__main__':
    run()