import apache_beam as beam
import os

INPUT_FILE = '../data/transactions.csv'
OUTPUT_FILE = '../data/output/success_transactions'

print("Current dir:", os.getcwd())
print("Input exists:", os.path.exists(INPUT_FILE))

def parse_csv_line(line):
    fields = line.split(',')
    return {
        'transaction_id': fields[0],
        'account_id': fields[1],
        'customer_id': fields[2],
        'transaction_type': fields[3],
        'amount': fields[4],
        'currency': fields[5],
        'transaction_timestamp': fields[6],
        'merchant_name': fields[7],
        'branch_code': fields[8],
        'status': fields[9],
    }

def run():
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'ReadCSV' >> beam.io.ReadFromText(INPUT_FILE, skip_header_lines=1)
            #| 'print1-readcsv' >> beam.Map(lambda x: print(x) or x)
            | 'ParseCSV' >> beam.Map(parse_csv_line)
            #| 'print2-parsecsv' >> beam.Map(lambda x: print(x) or x)
            | 'FilterSuccess' >> beam.Filter(lambda row: row['status'] == 'SUCCESS')
            #| 'print3-filtersuccess' >> beam.Map(lambda x: print(x) or x)
            | 'FormatOutput' >> beam.Map(lambda row: str(row))
            | 'WriteOutput' >> beam.io.WriteToText(OUTPUT_FILE, file_name_suffix='.txt')
        )

if __name__ == '__main__':
    run()