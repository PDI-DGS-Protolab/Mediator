
import riak


class RiakConnector():

    def __init__(self, host="localhost", port="8098"):
        self.client = riak.RiakClient(host, port)

    def upload_data(self, user_id, bucket_name, sdrs):
        bucket = self.client.bucket(bucket_name)
        bucket.set_allow_multiples(1)

        # TODO Comprobar que si el id existe dentro de este bucket, que meta los datos
        obj = bucket.new(user_id, data=sdrs)
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
        buckets = self.client.get_buckets()

        for b in buckets[1:]:
            for q in self.query_bucket(user_id, b):
                data.append(q)

        return data
