import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

def run():
    options = PipelineOptions(flags=[], streaming=True)
    sub_path = "projects/gcp-learnings-498010/subscriptions/transactions-input-sub"
    
    print("🚀 Sliding Window Pipeline Active (Size: 30s, Slide: 5s)...")
    
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=sub_path)
            | 'ParseJson'      >> beam.Map(lambda msg: json.loads(msg.decode('utf-8')))
            
            # --- SLIDING WINDOWS SPECIFIED HERE ---
            # size=30: Look back at 30 seconds of data
            # period=5: Emit a fresh evaluation every 5 seconds
            | 'ApplySliding'   >> beam.WindowInto(beam.window.SlidingWindows(size=30, period=5))
            
            | 'MapToPairs'     >> beam.Map(lambda data: (data.get('country'), 1))
            | 'SumPerCountry'  >> beam.CombinePerKey(sum)
            | 'PrintMetrics'   >> beam.Map(lambda res: print(f"📈 [Sliding 30s View] Country Total: {res}"))
        )

if __name__ == '__main__':
    run()