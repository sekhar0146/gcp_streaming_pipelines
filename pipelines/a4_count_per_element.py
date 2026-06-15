import apache_beam as beam

def run():
    with beam.Pipeline() as pipeline:
        (
            pipeline
            # 1. Create a PCollection of text lines
            | 'CreateLines' >> beam.Create([
                'beam', 'dataflow', 'beam', 'spark', 'beam', 'spark'
            ])
            
            # 2. countperelement
            | 'CountPerElement' >> beam.combiners.Count.PerElement()
            
            # 3. Print results to console
            | 'PrintOutput' >> beam.Map(print)
        )

if __name__ == '__main__':
    run()