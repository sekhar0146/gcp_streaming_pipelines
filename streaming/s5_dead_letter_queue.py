import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

DLQ_TAG = 'dead_letter_queue'

class SafeParseJsonFn(beam.DoFn):
    def process(self, element):
        try:
            decoded_str = element.decode('utf-8')
            parsed_json = json.loads(decoded_str)
            
            if 'txn_id' not in parsed_json or 'country' not in parsed_json:
                raise ValueError("Missing mandatory fields: 'txn_id' or 'country'")
                
            yield parsed_json
            
        except Exception as e:
            error_info = {
                "raw_payload": str(element),
                "error_reason": str(e)
            }
            yield beam.pvalue.TaggedOutput(DLQ_TAG, error_info)

def run():
    options = PipelineOptions(flags=[], streaming=True) 
    sub_path = "projects/gcp-learnings-498010/subscriptions/transactions-input-sub"
    
    print(f"🚀 DLQ Protected Pipeline Active... Listening to {sub_path}...")
    
    with beam.Pipeline(options=options) as pipeline:
        results = (
            pipeline
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=sub_path)
            | 'SafeParseData'  >> beam.ParDo(SafeParseJsonFn()).with_outputs(DLQ_TAG, main='main_clean')
        )
        
        # 1. MAIN PATH: We added a 10-second window here so GroupByKey can run safely!
        (
            results.main_clean
            | 'ApplyFixedWindow' >> beam.WindowInto(beam.window.FixedWindows(10))
            | 'MapToPairs'       >> beam.Map(lambda data: (data.get('country'), 1))
            | 'CountPerCountry'  >> beam.CombinePerKey(sum)
            | 'PrintCleanOut'    >> beam.Map(lambda res: print(f"✅ Main Processing Total: {res}"))
        )
        
        # 2. DLQ PATH: Stays exactly the same (No windows needed here because we just print/dump it immediately)
        (
            results[DLQ_TAG]
            | 'PrintDQLOut'      >> beam.Map(lambda bad: print(f"❌ DLQ ALERT! Corrupted Message Sent to Lost-And-Found: {bad}"))
        )

if __name__ == '__main__':
    run()