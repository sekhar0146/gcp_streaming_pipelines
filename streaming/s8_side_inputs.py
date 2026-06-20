import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

class EnrichTransactionFn(beam.DoFn):
    # The side input (metadata_dict) is passed directly as an extra argument in process()
    def process(self, element, metadata_dict):
        try:
            payload = json.loads(element.decode('utf-8'))
            country_code = payload.get('country', 'UNKNOWN')
            
            # Perform a lightning-fast in-memory lookup using our side input
            country_info = metadata_dict.get(country_code, {
                "full_name": "Unknown Country", 
                "currency": "USD"
            })
            
            # Enrich our payload with the metadata
            payload['country_full_name'] = country_info['full_name']
            payload['currency'] = country_info['currency']
            
            yield payload
        except Exception as e:
            print(f"❌ Error enriching record: {e}")

def run():
    options = PipelineOptions(flags=[], streaming=True)
    sub_path = "projects/gcp-learnings-498010/subscriptions/transactions-input-sub"
    
    print("🚀 Side Input Enrichment Pipeline Active...")
    
    with beam.Pipeline(options=options) as pipeline:
        
        # 1. Define our auxiliary metadata source (The Side Input data)
        # In production, this could be read from a file or database using beam.pvalue.AsDict()
        country_lookup = (
            pipeline
            | 'CreateLookupData' >> beam.Create([
                ('IN', {'full_name': 'India', 'currency': 'INR'}),
                ('US', {'full_name': 'United States', 'currency': 'USD'}),
                ('UK', {'full_name': 'United Kingdom', 'currency': 'GBP'})
            ])
        )
        
        # Convert our collection into a View dict that can be globally referenced by workers
        lookup_view = beam.pvalue.AsDict(country_lookup)
        
        # 2. Process our main stream and inject the side input view
        (
            pipeline
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=sub_path)
            
            # Pass the side input view using keyword arguments (or positional args)
            | 'EnrichStream'   >> beam.ParDo(EnrichTransactionFn(), metadata_dict=lookup_view)
            
            | 'PrintEnriched'  >> beam.Map(lambda res: print(f"💎 Enriched Payload: {res}"))
        )

if __name__ == '__main__':
    run()