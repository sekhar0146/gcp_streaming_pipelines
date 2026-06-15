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
            yield beam.pvalue.TaggedOutput(DLQ_TAG, json.dumps(error_info))

def run():
    options = PipelineOptions(flags=[], streaming=True) 
    sub_path = "projects/gcp-learnings-498010/subscriptions/transactions-input-sub"
    gcs_dlq_path = "gs://dlq_bucket-498010/failed_transactions"
    
    print(f"🚀 GCS-DLQ Pipeline Active... Writing failures to {gcs_dlq_path}...")
    
    with beam.Pipeline(options=options) as pipeline:
        results = (
            pipeline
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=sub_path)
            | 'SafeParseData'  >> beam.ParDo(SafeParseJsonFn()).with_outputs(DLQ_TAG, main='main_clean')
        )
        
        # 1. MAIN PATH (Clean data aggregated per country)
        (
            results.main_clean
            | 'ApplyMainWindow' >> beam.WindowInto(beam.window.FixedWindows(10))
            | 'MapToPairs'      >> beam.Map(lambda data: (data.get('country'), 1))
            | 'CountPerCountry' >> beam.CombinePerKey(sum)
            | 'PrintCleanOut'   >> beam.Map(lambda res: print(f"✅ Main Processing Total: {res}"))
        )
        
        # 2. DLQ PATH FIXED: Added a window here so the file sink knows when to flush!
        (
            results[DLQ_TAG]
            | 'ApplyDLQWindow'   >> beam.WindowInto(beam.window.FixedWindows(10))
            | 'WriteFailuresToGCS' >> beam.io.WriteToText(
                                        file_path_prefix=gcs_dlq_path,
                                        file_name_suffix='.json',
                                        shard_name_template='-SSSSS-of-NNNNN'
                                      )
        )

if __name__ == '__main__':
    run()