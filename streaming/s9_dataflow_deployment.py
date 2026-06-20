import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import json
import logging

# Ensure standard logging is routed up to Cloud Logging properly
logging.getLogger().setLevel(logging.INFO)

# 1. Moving the DoFn definition outside and adding explicit Cloud Logging.
# This prevents the "--no_save_main_session" closure break.
class EnrichTransactionFn(beam.DoFn):
    def process(self, element, metadata_dict):
        try:
            # Pub/Sub raw strings come down as byte payloads
            payload_str = element.decode('utf-8')
            logging.info(f"📥 Received raw message from Pub/Sub: {payload_str}")
            
            payload = json.loads(payload_str)
            country_code = payload.get('country', 'UNKNOWN')
            
            country_info = metadata_dict.get(country_code, {
                "full_name": "Unknown Country", 
                "currency": "USD"
            })
            
            payload['country_full_name'] = country_info['full_name']
            payload['currency'] = country_info['currency']
            
            # Using logging instead of print guarantees visibility in Cloud Logs (Logs Explorer)
            logging.info(f"💎 Successfully Enriched Element: {payload}")
            yield payload
            
        except Exception as e:
            # Do not pass silently during debugging! Output the problem to the error logger.
            logging.error(f"❌ Error processing transaction element: {str(e)}", exc_info=True)

def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    
    gcp_options = options.view_as(GoogleCloudOptions)
    print(f"📡 Preparing deployment for project: {gcp_options.project} on Google Cloud Dataflow...")
    
    with beam.Pipeline(options=options) as pipeline:
        
        # Static enrichment dictionary
        country_lookup = (
            pipeline
            | 'CreateLookupData' >> beam.Create([
                ('IN', {'full_name': 'India', 'currency': 'INR'}),
                ('US', {'full_name': 'United States', 'currency': 'USD'}),
                ('UK', {'full_name': 'United Kingdom', 'currency': 'GBP'})
            ])
        )
        lookup_view = beam.pvalue.AsDict(country_lookup)
        
        sub_path = "projects/gcp-learnings-498010/subscriptions/transactions-input-sub"
        
        # Main stream ingestion, enrichment, and explicit logger termination step
        (
            pipeline
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=sub_path)
            | 'EnrichStream'   >> beam.ParDo(EnrichTransactionFn(), metadata_dict=lookup_view)
        )

if __name__ == '__main__':
    run()