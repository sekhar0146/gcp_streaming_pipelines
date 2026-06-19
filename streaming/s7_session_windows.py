import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

def run():
    options = PipelineOptions(flags=[], streaming=True)
    sub_path = "projects/gcp-learnings-498010/subscriptions/transactions-input-sub"
    
    # Define the inactivity gap (15 seconds)
    inactivity_gap_seconds = 15
    
    print(f"🚀 Session Window Pipeline Active (Inactivity Gap: {inactivity_gap_seconds}s)...")
    
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=sub_path)
            | 'ParseJson'      >> beam.Map(lambda msg: json.loads(msg.decode('utf-8')))
            
            # --- SESSION WINDOWS SPECIFIED HERE ---
            # Automatically groups elements by Key (country/user) based on time proximity
            | 'ApplySessions'  >> beam.WindowInto(beam.window.Sessions(gap_size=inactivity_gap_seconds))
            
            # Form KV pairs to allow session grouping per country/user
            | 'MapToPairs'     >> beam.Map(lambda data: (data.get('country'), data.get('amount', 0)))
            
            # Combine elements within the same session window
            | 'SumPerSession'  >> beam.CombinePerKey(sum)
            | 'PrintSessions'  >> beam.Map(lambda res: print(f"👤 [Session Window Closed] Data: {res}"))
        )

if __name__ == '__main__':
    run()