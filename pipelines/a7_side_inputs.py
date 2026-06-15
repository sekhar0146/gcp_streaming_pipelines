import apache_beam as beam

def convert_currency(transaction, rates):
    # 'rates' is our Side Input passed into the function
    currency = transaction['currency']
    rate = rates.get(currency, 1.0)
    
    transaction['amount_usd'] = transaction['amount'] * rate
    return transaction

def run():
    with beam.Pipeline() as pipeline:
        
        # 1. This is our small side data (Conversion rates to USD)
        exchange_rates = {'INR': 0.012, 'EUR': 1.10, 'GBP': 1.25}
        
        # 2. Main PCollection pipeline
        (
            pipeline
            | 'CreateTransactions' >> beam.Create([
                {'id': 'T1', 'amount': 1000, 'currency': 'INR'},
                {'id': 'T2', 'amount': 50, 'currency': 'EUR'},
                {'id': 'T3', 'amount': 100, 'currency': 'USD'}
            ])
            
            # 3. Pass the standard dictionary as an extra argument (Side Input)
            | 'ToUSD' >> beam.Map(convert_currency, rates=exchange_rates)
            
            | 'PrintResults' >> beam.Map(print)
        )

if __name__ == '__main__':
    run()