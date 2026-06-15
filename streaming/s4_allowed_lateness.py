import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

def run():
    options = PipelineOptions(flags=[], streaming=True) 
    sub_path = "projects/gcp-learnings-498010/subscriptions/transactions-input-sub"
    
    print(f"🎬 Grace Period Pipeline Active... Listening to {sub_path}...")
    
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'ReadFromPubSub'   >> beam.io.ReadFromPubSub(subscription=sub_path)
            | 'DecodeBytes'      >> beam.Map(lambda b: b.decode('utf-8'))
            | 'ParseJson'        >> beam.Map(json.loads)
            
            # Here is your design rule in action:
            | 'WindowWithGrace'  >> beam.WindowInto(
                                        beam.window.FixedWindows(10),
                                        allowed_lateness=10  # 10 seconds of extra safety time
                                    )
            
            | 'MapToPairs'       >> beam.Map(lambda data: (data.get('country'), 1))
            | 'CountPerCountry'  >> beam.CombinePerKey(sum)
            | 'PrintOutput'      >> beam.Map(lambda res: print(f"✨ Window Result: {res}"))
        )

if __name__ == '__main__':
    run()