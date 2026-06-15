import apache_beam as beam

INPUT_FILE = '../data/transactions.csv'
OUTPUT_DIR = '../data/output/'

HIGH_VALUE_TAG = 'high_value'
FAILED_TAG = 'failed'

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
        'status': fields[9].strip(),
    }

class SplitTransactionsFn(beam.DoFn):
    def process(self, row):
        # Skip bad records (non-numeric amount)
        try:
            amount = float(row['amount'])
        except ValueError:
            return  # drop silently for now (Phase 7 will handle properly)

        if row['status'] != 'SUCCESS':
            yield beam.pvalue.TaggedOutput(FAILED_TAG, row)
        elif amount > 50000:
            yield beam.pvalue.TaggedOutput(HIGH_VALUE_TAG, row)
        else:
            yield row  # main output = NORMAL

def run():
    with beam.Pipeline() as pipeline:
        parsed = (
            pipeline
            | 'ReadCSV' >> beam.io.ReadFromText(INPUT_FILE, skip_header_lines=1)
            | 'ParseCSV' >> beam.Map(parse_csv_line)
        )

        results = (
            parsed
            | 'SplitTransactions' >> beam.ParDo(SplitTransactionsFn()).with_outputs(
                HIGH_VALUE_TAG, FAILED_TAG, main='normal'
            )
        )

        normal = results['normal']
        high_value = results[HIGH_VALUE_TAG]
        failed = results[FAILED_TAG]

        normal | 'WriteNormal' >> beam.Map(str) | 'WriteNormalFile' >> beam.io.WriteToText(
            OUTPUT_DIR + 'normal_transactions', file_name_suffix='.txt')

        high_value | 'WriteHighValue' >> beam.Map(str) | 'WriteHighValueFile' >> beam.io.WriteToText(
            OUTPUT_DIR + 'high_value_transactions', file_name_suffix='.txt')

        failed | 'WriteFailed' >> beam.Map(str) | 'WriteFailedFile' >> beam.io.WriteToText(
            OUTPUT_DIR + 'failed_transactions', file_name_suffix='.txt')

if __name__ == '__main__':
    run()