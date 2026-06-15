import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.combiners import Count
import time

def add_timestamp(element):
    return beam.window.TimestampedValue(element, time.time())

# Custom print function to show the Window boundaries
class PrintWithWindowFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        # Format the window start/end times into readable strings
        start = window.start.to_utc_datetime().strftime('%H:%M:%S')
        end = window.end.to_utc_datetime().strftime('%H:%M:%S')
        
        word, count = element
        print(f"Time Window [{start} to {end}] ──> Result: {word} happened {count} times")

def run():
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'StreamInput' >> beam.Create(['click', 'click', 'login', 'click', 'login'])
            | 'AddEventTime' >> beam.Map(add_timestamp)
            | 'WindowIntoTenSecs' >> beam.WindowInto(FixedWindows(10))
            | 'CountPerWindow' >> Count.PerElement()
            # Use our custom printer instead of plain print
            | 'PrintWithWindow' >> beam.ParDo(PrintWithWindowFn())
        )

if __name__ == '__main__':
    run()