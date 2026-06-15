import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.combiners import Count
import time

def add_timestamp(element):
    current_time = time.time()
    return beam.window.TimestampedValue(element, current_time)

# Custom transparent print functions
def print_stage2(element):
    print(f"[Stage 2: Added Timestamp] Data: {element}")
    return element

def print_stage3(element):
    print(f"[Stage 3: Inside Window]   Data: {element}")
    return element

def run():
    with beam.Pipeline() as pipeline:
        (
            pipeline
            # 1. Simulating a live stream of incoming events
            | 'StreamInput' >> beam.Create(['click', 'click', 'login', 'click', 'login'])
            
            # 2. Add timestamps so Beam knows *when* each event happened
            | 'AddEventTime' >> beam.Map(add_timestamp)
            
            # Print after Stage 2 using the TimestampParam trick
            | 'PrintStage2'  >> beam.Map(
                lambda element, ts=beam.DoFn.TimestampParam: (
                    print(f"[Stage 2] Data: {element} | Event Time: {ts.to_utc_datetime().strftime('%H:%M:%S.%f')[:-3]}"),
                    element
                )[1] # We return 'element' to pass it down the conveyor belt
            )
            
            # 3. Chop the continuous stream into strict 10-second windows
            | 'WindowIntoTenSecs' >> beam.WindowInto(FixedWindows(10))
            
            # Print after Stage 3 using BOTH TimestampParam and WindowParam tricks
            | 'PrintStage3'       >> beam.Map(
                lambda element, ts=beam.DoFn.TimestampParam, win=beam.DoFn.WindowParam: (
                    print(f"[Stage 3] Data: {element} | Window Block: [{win.start.to_utc_datetime().strftime('%H:%M:%S')} - {win.end.to_utc_datetime().strftime('%H:%M:%S')}]"),
                    element
                )[1]
            )
            
            # 4. Count elements *per window*
            | 'CountPerWindow' >> Count.PerElement()
            
            # 5. Print out the final aggregated results with its Window block
            | 'PrintWindowCounts' >> beam.Map(
                lambda result, win=beam.DoFn.WindowParam: print(f"[Stage 5 Final Aggregation] Window [{win.start.to_utc_datetime().strftime('%H:%M:%S')} - {win.end.to_utc_datetime().strftime('%H:%M:%S')}] Result ──> {result}\n" + "─"*90)
            )
        )

if __name__ == '__main__':
    run()