import apache_beam as beam

# 1. Define your custom processing machine using beam.DoFn
class CleanAndFilterTransactions(beam.DoFn):
    
    # The process method is required. Beam passes every element through here.
    def process(self, element):
        # Clean the data: convert uppercase
        status = element.get('status', 'UNKNOWN').upper()
        amount = element.get('amount', 0)
        
        # Custom Logic: Only forward SUCCESS transactions that are greater than $100
        if status == 'SUCCESS' and amount > 100:
            # We use 'yield' instead of 'return' in a DoFn.
            # This allows you to output 0, 1, or multiple items per input!
            yield {
                'id': element['id'],
                'amount': amount,
                'status': status
            }

def run():
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'CreateData' >> beam.Create([
                {'id': 'T1', 'amount': 150, 'status': 'success'},   # Kept (Success & > 100)
                {'id': 'T2', 'amount': 50,  'status': 'success'},   # Dropped (Too small)
                {'id': 'T3', 'amount': 500, 'status': 'failed'},    # Dropped (Failed)
                {'id': 'T4', 'amount': 300, 'status': 'SUCCESS'}    # Kept (Success & > 100)
            ])
            
            # 2. Apply your custom machine using beam.ParDo
            | 'CustomFilterClean' >> beam.ParDo(CleanAndFilterTransactions())
            
            | 'PrintResults' >> beam.Map(print)
        )

if __name__ == '__main__':
    run()