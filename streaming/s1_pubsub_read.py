import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

def run():
    options = PipelineOptions(flags=[], streaming=True) 
    subscription_path = "projects/gcp-learnings-498010/subscriptions/transactions-input-sub"
    
    print(f"🎬 Starting Beam Pipeline... Listening to {subscription_path}")
    print("Press Ctrl+C to stop.\n" + "─"*65)
    
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'ReadFromPubSub'  >> beam.io.ReadFromPubSub(subscription=subscription_path)
            | 'DecodeBytes'     >> beam.Map(lambda bytes_data: bytes_data.decode('utf-8'))
            | 'ParseJson'       >> beam.Map(json.loads)
            | 'PrintLiveStream' >> beam.Map(lambda data: print(f"📥 Pipeline Caught: {data}"))
        )

if __name__ == '__main__':
    run()