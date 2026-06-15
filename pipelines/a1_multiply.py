import apache_beam as beam

def run():
    # 1. Create the Pipeline object
    with beam.Pipeline() as pipeline:
        (
            pipeline
            # 2. Extract: Create a PCollection from a local Python list
            | 'CreateNumbers' >> beam.Create([1, 2, 3, 4, 5])
            
            # 3. Transform: Multiply each number by 2 using beam.Map
            | 'MultiplyByTwo' >> beam.Map(lambda x: x * 2)
            
            # 4. Load: Print each item to the console
            | 'PrintResults' >> beam.Map(print)
        )

if __name__ == '__main__':
    run()