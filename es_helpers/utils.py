from django.conf import settings


class EsTz(object):
    @staticmethod
    def localize(date_string):
        offset = settings.ES_UTC_OFFSET
        return '{}T+{:0>2d}'.format(date_string, int(offset))
