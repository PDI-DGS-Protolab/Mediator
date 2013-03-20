import datetime
import riak


class SDR_uploader():

    def __init__(self):
        self.n = 0

    def generate_sdr(self, user, service, events):
        eventsList = []
        for event in events:
            data = {
                'eventId': event['id'],
                'timestamp': event['timestamp'],
                'description': event['description'] or None,
                'additionalData': None,
                'details': event['details'] or None
            }
            eventsList.append(data)

        return {
            'serviceId': service['id'],
            'accountId': user['id'],
            'requestId': service['requestId'] or None,
            'requestOrder': service['requestOrder'] or None,
            'country': service['country'],
            'timestamp': str(datetime.datetime.now()),
            'events': eventsList
        }

    def upload_sdr(self, client, sdr):
        test = client.bucket('test')
        o = test.new(str(self.n), data=sdr)
        o.store()
        self.n += 1

    def view_contents(self, client):
        query = client.search('test', "0")
        for r in query.run():
            course = r.get().get_data()
            print course


if __name__ == "__main__":

    user = {'id': '5'}
    service = {'id': "asd", 'requestId': "asd", 'country': "ES", 'requestOrder': None}
    events = [{'id': '2', 'timestamp': "asd", 'description': "HelloWorld", 'details': "Success"},
              {'id': '3', 'timestamp': "blr", 'description': "HelloWorld", 'details': "Success"}]

    uploader = SDR_uploader()
    sdr = uploader.generate_sdr(user, service, events)

    client = riak.RiakClient(host="ec2-54-228-77-174.eu-west-1.compute.amazonaws.com", port="8098")
    #client = riak.RiakClient(host="localhost", port="8098")
    uploader.upload_sdr(client, sdr)
    test = client.bucket('test')
    print test.get('0').get_data()

"""
    for i in range(1, 60000):
        try:
            client = riak.RiakClient(host="ec2-54-228-116-34.eu-west-1.compute.amazonaws.com", port=4369)
            riak.RiakBucket(client, 'test')

            sdr = sdr_u.generate_sdr(user, service, events)
            sdr_u.upload_sdr(client, sdr)
            # sdr_u.view_contents(client)

            print i

        except:
            pass
"""
