
from riak_cs_connector import RiakCSConnector
import riak


class RiakConnector():

    def __init__(self, host="localhost", port="8098"):
        self.client = riak.RiakClient(host, port)
        self.riakcs = RiakCSConnector()

    def upload_data(self, key, bucket_name, data):
        bucket = self.client.bucket(bucket_name)
        bucket.set_allow_multiples(1)

        new_key = key.split('|')[0]

        sdr = bucket.get(key)

        # Comprobamos si el fichero no existia
        if not sdr:
            obj = bucket.new(new_key, data=data)
            obj.store()
        else:
            content = sdr.get_data()
            content.append(data)
            obj = bucket.new(new_key, data=content)
            obj.store()

    def query_bucket(self, user_id, bucket_name):
        query = self.client.add(bucket_name)
        query.map(
            """
            function(v) {
                var data = JSON.parse(v.values[0].data);

                var res = [];
                if (v.key === '""" + user_id + """') {
                    res = [[v.key, data]];
                }
                return res;

                // return [[v.key, data]];
            }
            """
        )

        query.reduce(
            """
            function(values) {
                return values;
            }
            """
        )

        # TODO Filtrar los datos segun el user_id
        for result in query.run():
            yield str(result)

    def query_buckets(self, user_id):
        data = []
        buckets = self.client.list_buckets_names()

        for b in buckets[1:]:
            for q in self.query_bucket(user_id, b):
                data.append(q)

        return data

    def download_from_riakcs(self, bucket_name):
        files = self.riakcs.get_filenames_from_bucket(bucket_name)
        for f in files:
            self.riakcs.get_file(f, bucket_name)
        pass

    def process_sdr(self):
        pass
