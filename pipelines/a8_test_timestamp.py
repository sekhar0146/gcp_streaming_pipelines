import apache_beam as beam
import time

def add_timestamp(element):
    current_time = time.time()
    # We return a special Beam object here
    return beam.window.TimestampedValue(element, current_time)

def run():
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'StreamInput'  >> beam.Create(['click'])
            
            # 1. This step ATTACHES the time, but does not print it
            | 'AddEventTime' >> beam.Map(add_timestamp)
            
            # 2. This step tries to PRINT what happened in step 1
            | 'DirectPrint'  >> beam.Map(print)
        )

if __name__ == '__main__':
    run()