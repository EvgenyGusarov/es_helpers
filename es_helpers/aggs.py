from django.conf import settings
import json


class Aggs(object):
    def get_aggs_itself(self):
        raise NotImplementedError

    def as_dict(self, aggs_only=False):
        res = {
            self.name: self.get_aggs_itself()
        }
        if self.aggs:
            res[self.name]['aggs'] = self.aggs.as_dict(aggs_only=True)
        if aggs_only:
            return res
        else:
            return {'aggs': res}

    def as_json(self, aggs_only=False):
        return json.dumps(self.as_dict(aggs_only), indent=2)


class FiltersAggs(Aggs):
    def __init__(self, name, filters, aggs=None):
        self.name = name
        self.filters = filters
        self.aggs = aggs

    def get_aggs_itself(self):
        filters = {key: value.as_dict(filter_only=True) for key, value in self.filters.items()}
        res = {
            'filters': {
                'filters': filters
            }
        }
        if self.aggs:
            res['aggs'] = self.aggs.as_dict(aggs_only=True)

        return res


class FilterAggs(Aggs):
    def __init__(self, name, filter, aggs):
        self.name = name
        self.filter = filter
        self.aggs = aggs

    def get_aggs_itself(self):
        return {
            'filter': self.filter.as_dict(filter_only=True),
            'aggs': self.aggs.as_dict(aggs_only=True)
        }


class DateHistogramAggs(Aggs):
    def __init__(self, name, field, interval, aggs=None):
        self.name = name
        self.field = field
        self.interval = interval
        self.aggs = aggs

    def get_aggs_itself(self):
        return {
            'date_histogram': {
                'field': self.field,
                'format': 'yyyy-MM-dd',
                'interval': self.interval,
                'min_doc_count': 0,
                'pre_zone': settings.ES_UTC_OFFSET,
                'post_zone': settings.ES_UTC_OFFSET,
            }
        }


class TermsAggs(Aggs):
    def __init__(self, name, field, size=10, min_doc_count=0, order='asc', aggs=None):
        self.name = name
        self.field = field
        self.size = size
        self.min_doc_count = min_doc_count

        if order in ('asc', 'desc'):
            self.order = {'_term': order}
        else:
            self.order = order

        self.aggs = aggs

    def get_aggs_itself(self):
        res = {
            'terms': {
                'field': self.field,
                'size': self.size
            }
        }
        if self.min_doc_count is not None:
            res['terms']['min_doc_count'] = self.min_doc_count
        if self.order is not None:
            res['terms']['order'] = self.order
        return res


class MaxAggs(Aggs):
    def __init__(self, name, field):
        self.name = name
        self.field = field
        self.aggs = None

    def get_aggs_itself(self):
        return {
            'max': {
                'field': self.field
            }
        }
