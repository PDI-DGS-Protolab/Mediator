
import datetime
import random
import riak
import httplib
import requests


class SDR_Generator():

    def generate_sdr(self, user_id, sdr):
        eventsList = []
        for event in sdr['events']:
            data = {
                'eventId': event['id'],
                'timestamp': event['timestamp'],
                'description': event['description'] or None,
                'additionalData': None,
                'details': event['details'] or None
            }
            eventsList.append(data)

        return {
            'serviceId': sdr['serviceId'],
            'accountId': user_id,
            'requestId': sdr['requestId'] or None,
            'requestOrder': sdr['requestOrder'] or None,
            'country': sdr['countryId'],
            'timestamp': str(datetime.datetime.now()),
            'events': eventsList
        }


class RiakConnector():

    def __init__(self, host="localhost", port="8098"):
        self.sdr_gen = SDR_Generator()
        self.client = riak.RiakClient(host, port)

    def upload_sdrs(self, user_id="", bucket_name="", sdrs=[]):
        bucket = self.client.bucket(bucket_name)
        bucket.set_allow_multiples(1)

        # TODO Comprobar que si el id existe dentro de este bucket, que meta los datos
        jsons = []
        for s in sdrs:
            j = self.sdr_gen.generate_sdr(user_id, s)
            jsons.append(j)

        # print "\n\n", user_id, jsons
        o = bucket.new(user_id, data=jsons[0])
        o.store()

    def query_buckets(self, user_id=""):
        for b in self.client.get_buckets():
            query = self.client.add(b)
            query.map(
                """
                function(v) {
                    var data = JSON.parse(v.values[0].data);
/*
                    var res = [];
                    if (v.key === '""" + user_id + """') {
                        res = [[v.key, data]];
                    }
                    return res;
*/
                    return [[v.key, data]];
                }
                """
            )
            query.reduce("""function(values) { return values; }""")

            # TODO Filtrar los datos segun el user_id
            for result in query.run():
                print str(result) + "\n"

    def removeAllDatabase(self):
        """
        conn = httplib.HTTPConnection('localhost:8098')

        for bucket in self.client.get_buckets():
            b = self.client.bucket(bucket)
            keys = b.get_keys()

            for key in keys:
                conn.request('DELETE', '/riak/' + bucket + '/' + key)
                conn.getresponse()

        conn.close()
        """

        base_url = 'localhost:8080'
        headers = {'content-type': 'application/json'}

        for bucket in self.client.get_buckets():
            b = self.client.bucket(bucket)
            keys = b.get_keys()

            for key in keys:
                url = base_url + '/riak/' + bucket + '/' + key
                requests.delete(url, data='{}', headers=headers)


class TestRiak():

    def __init__(self):
        self.conn = RiakConnector()
        self.users = ['user1', 'user2', 'user3']

    def gen_sdrs(self, bucket_name=None):
        now = lambda: str(datetime.datetime.now())

        if not bucket_name:
            d = now()
            bucket_name = d[0:4] + d[5:7] + d[8:10] + "_" + d[11:13]

        for u in self.users:

            sdrs = []
            for j in range(0, 10):
                sdr = {'serviceId': j, 'requestId': u, 'countryId': "ES", 'requestOrder': None}
                events = []

                for k in range(0, 3):
                    events.append({'id': k, 'timestamp': now(), 'description': random.random(), 'details': 'Success'})

                sdr['events'] = events
                sdrs.append(sdr)

            self.conn.upload_sdrs(user_id=u, bucket_name=bucket_name, sdrs=sdrs)

    def query_random_user_data(self):
        i = random.randrange(0, len(self.users))
        u = self.users[i]
        print "Printing " + u + "'s data:\n"
        self.conn.query_buckets(user_id=u)

    def removeAllDatabase(self):
        self.conn.removeAllDatabase()


if __name__ == "__main__":
    t = TestRiak()
    # t.removeAllDatabase()
    t.gen_sdrs(bucket_name="20130320.13")
    # t.gen_sdrs(bucket_name="20130320_15")
    # t.gen_sdrs(bucket_name="20130320_17")
    t.query_random_user_data()
