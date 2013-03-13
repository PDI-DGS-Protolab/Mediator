import riak

# Connect to Riak.
client = riak.RiakClient()


# Create buckets to store data in.
depa = riak.RiakBucket(client, 'department')
prof = riak.RiakBucket(client, 'professors')
cour = riak.RiakBucket(client, 'courses')


# Create department objects
for d in ['datsi', 'dlsiis', 'dia', 'dma']:
    depa.new(d, data={'name': d, 'jefe': 'fulano-de-tal'}).store()


# Create courses objects
cour.new('1', data={'name': 'estructura',   'creditos': '6', 'department': 'datsi'}).store()
cour.new('2', data={'name': 'ssoo',         'creditos': '6', 'department': 'datsi'}).store()
cour.new('3', data={'name': 'pps',          'creditos': '3', 'department': 'datsi'}).store()


# Query with first param as the bucket and second param as the criteria
query = client.search('department', 'creditos: 6')


# Print data from 'department' bucket
for result in query.run():
    course = result.get().get_data()
    print 'Course %s %s' % (course['name'], course['last_name'])
