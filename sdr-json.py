
from . import riak_connector as conn
from . import sdr_generator as sdr

import helpers
import random


class SDR_Uploader():

    def __init__(self, host="localhost", port="8098"):
        self.conn = conn.RiakConnector(host, port)
        self.gen = sdr.SDR_Generator()
        self.users = ['user1', 'user2', 'user3']

    def upload_sdrs(self, user_id, bucket_name, data=""):

        if not bucket_name:
            d = helpers.now()
            bucket_name = 'time.' + d[0:4] + '.' + d[5:7] + '.' + d[8:10] + "." + d[11:13]

        if not data:
            data = self.gen_random_data()

        sdrs = self.gen_sdrs(user_id, data)
        self.conn.upload_data(user_id, bucket_name, sdrs)

    def gen_sdrs(self, user_id, data):
        sdrs = []
        for d in data:
            sdr = self.gen.generate_sdr(user_id, d)
            sdrs.append(sdr)

        return sdrs

    def gen_random_data(self):
        for u in self.users:

            sdrs = []
            for j in range(0, 10):
                sdr = {'serviceId': j, 'requestId': u, 'countryId': "ES", 'requestOrder': None}
                events = []

                for k in range(0, 3):
                    events.append({'id': k, 'timestamp': helpers.now(), 'description': random.random(), 'details': 'Success'})

                sdr['events'] = events
                sdrs.append(sdr)

        return sdrs

    def query_user_data(self, user_id):
        return self.conn.query_buckets(user_id)


if __name__ == "__main__":

    uploader = SDR_Uploader(host="ec2-46-137-37-86.eu-west-1.compute.amazonaws.com", port="8098")
    uploader.upload_sdrs("user2", "time.2013.04.02.15")
    print uploader.query_user_data("user2")
