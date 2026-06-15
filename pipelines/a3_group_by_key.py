import apache_beam as beam

def run():
    with beam.Pipeline() as pipeline:
        (
            pipeline
            # 1. Create a PCollection of text lines
            | 'CreateLines' >> beam.Create([
                'beam', 'dataflow', 'beam', 'spark', 'beam', 'spark'
            ])
            
            # 2. Map each word to a key-value pair (word, 1)
            | 'MapToKeyValue' >> beam.Map(lambda word: (word, 1))

            # 3. Group by key (word) and sum the counts
            | 'GroupByKey' >> beam.CombinePerKey(sum)
            
            # 5. Print results to console
            | 'PrintOutput' >> beam.Map(print)
        )

if __name__ == '__main__':
    run()