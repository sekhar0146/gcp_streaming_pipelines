import apache_beam as beam
import time

def add_timestamp(element):
    # Attaches current system time to the element's metadata
    return beam.window.TimestampedValue(element, time.time())

def run():
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'StreamInput'  >> beam.Create(['click', 'login'])
            | 'AddEventTime' >> beam.Map(add_timestamp)
            
            # THE SIMPLEST WAY: Pass beam.DoFn.TimestampParam into your lambda function
            | 'PrintWithTime' >> beam.Map(
                lambda element, ts=beam.DoFn.TimestampParam: print(f"Data: {element} | Time: {ts.to_utc_datetime().strftime('%H:%M:%S.%f')[:-3]}")
            )
        )

if __name__ == '__main__':
    run()