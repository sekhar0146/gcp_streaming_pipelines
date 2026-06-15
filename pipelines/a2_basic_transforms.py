import apache_beam as beam

def split_words(text_line):
    # Splits a sentence into a list of individual words
    return text_line.split(' ')

def run():
    with beam.Pipeline() as pipeline:
        (
            pipeline
            # 1. Create a PCollection of text lines
            | 'CreateLines' >> beam.Create([
                'Apache Beam is powerful',
                'Learning Beam is fun and simple'
            ])
            
            # 2. FlatMap (1-to-Many): Splits lines into individual words
            # Input: 'Apache Beam is powerful' -> Output: 'Apache', 'Beam', 'is', 'powerful'
            | 'ExtractWords' >> beam.FlatMap(split_words)
            
            # 3. Filter (1-to-0 or 1): Keep only words longer than 3 characters
            | 'FilterShortWords' >> beam.Filter(lambda word: len(word) > 3)
            
            # 4. Map (1-to-1): Convert remaining words to UPPERCASE
            | 'ToUpperCase' >> beam.Map(lambda word: word.upper())
            
            # 5. Print results to console
            | 'PrintOutput' >> beam.Map(print)
        )

if __name__ == '__main__':
    run()