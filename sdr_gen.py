
import helpers


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
            'accountId': user_id,
            'serviceId': sdr['serviceId'],
            'requestId': sdr['requestId'] or None,
            'requestOrder': sdr['requestOrder'] or None,
            'country': sdr['countryId'],
            'timestamp': helpers.now(),
            'events': eventsList
        }
