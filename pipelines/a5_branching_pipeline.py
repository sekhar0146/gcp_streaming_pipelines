import apache_beam as beam

def run():
    with beam.Pipeline() as pipeline:
        # 1. Load the shared/base PCollection
        all_transactions = (
            pipeline
            | 'CreateTransactions' >> beam.Create([
                {'id': 'TXN1', 'status': 'SUCCESS', 'amount': 100},
                {'id': 'TXN2', 'status': 'FAILED', 'amount': 0},
                {'id': 'TXN3', 'status': 'SUCCESS', 'amount': 250},
                {'id': 'TXN4', 'status': 'PENDING', 'amount': 50}
            ])
        )

        # 2. Branch Path A: Filter and print only SUCCESS transactions
        (
            all_transactions
            | 'FilterSuccess' >> beam.Filter(lambda txn: txn['status'] == 'SUCCESS')
            | 'PrintSuccess' >> beam.Map(lambda txn: print(f"✔ Success: {txn}"))
        )

        # 3. Branch Path B: Filter and print only FAILED transactions
        (
            all_transactions
            | 'FilterFailed' >> beam.Filter(lambda txn: txn['status'] == 'FAILED')
            | 'PrintFailed' >> beam.Map(lambda txn: print(f"❌ Failed: {txn}"))
        )

        # 4. Pending transactions can be handled similarly if needed, or you can add more branches for other statuses.
        (
            all_transactions
            | 'FilterPending' >> beam.Filter(lambda txn: txn['status'] == 'PENDING')
            | 'PrintPending' >> beam.Map(lambda txn: print(f"⏳ Pending: {txn}"))
        )


if __name__ == '__main__':
    run()