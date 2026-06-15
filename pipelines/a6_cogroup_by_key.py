import apache_beam as beam

def run():
    with beam.Pipeline() as pipeline:
        
        # 1. Dataset A: Customer Names -> (id, name)
        names = (
            pipeline
            | 'CreateNames' >> beam.Create([
                ('CUST1', 'Alice'),
                ('CUST2', 'Bob'),
                ('CUST3', 'Charlie')
            ])
        )
        
        # 2. Dataset B: Customer Locations -> (id, city)
        locations = (
            pipeline
            | 'CreateLocations' >> beam.Create([
                ('CUST1', 'New York'),
                ('CUST2', 'London'),
                ('CUST1', 'Tokyo')  # CUST1 has two locations
            ])
        )
        
        # 3. Join datasets together into a dictionary format
        joined_data = (
            {'name_list': names, 'city_list': locations}
            | 'JoinDatasets' >> beam.CoGroupByKey()
        )
        
        # 4. Print the joined result
        (
            joined_data
            | 'PrintJoined' >> beam.Map(lambda result: print(f"Joined: {result}"))
        )

if __name__ == '__main__':
    run()