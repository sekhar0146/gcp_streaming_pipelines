import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import json
import logging

logging.getLogger().setLevel(logging.INFO)

class EnrichTransactionFn(beam.DoFn):
    def process(self, element, metadata_dict):
        try:
            payload = json.loads(element.decode('utf-8'))
            country_code = payload.get('country', 'UNKNOWN')
            
            country_info = metadata_dict.get(country_code, {
                "full_name": "Unknown Country", 
                "currency": "USD"
            })
            
            payload['country_full_name'] = country_info['full_name']
            payload['currency'] = country_info['currency']
            
            logging.info(f"💎 Enriched element internally: {payload['txn_id']}")
            yield payload
        except Exception as e:
            logging.error(f"❌ Processing error: {str(e)}")

def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    
    table_spec = 'gcp-learnings-498010:txns_dataset.enriched_transactions'

    with beam.Pipeline(options=options) as pipeline:
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
        (
            pipeline
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=sub_path)
            | 'EnrichStream'   >> beam.ParDo(EnrichTransactionFn(), metadata_dict=lookup_view)
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                table=table_spec,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

if __name__ == '__main__':
    run()