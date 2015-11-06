import json
from copy import copy, deepcopy
from datetime import datetime, timedelta

from .utils import EsTz


class EsFilter(object):
    skeleton = {
        'query': {
            'filtered': {
                'filter': {
                }
            }
        }
    }

    def __init__(self, *filters, **kwargs):
        self.filters = filters
        self.items = kwargs.items()
        self.aggs = None
        self.sort = None
        self.size = None

    def extend(self, *filters, **kwargs):
        self.filters += filters
        self.items += kwargs.items()

    def add_prefix(self, prefix):
        self.items = [('{}.{}'.format(prefix, key), value) for key, value in self.items]
        for _filter in self.filters:
            _filter.add_prefix(prefix)

    def aggs_by(self, aggs):
        self.aggs = aggs

    def get_filter_itself(self, filter_dicts):
        raise NotImplementedError

    def as_dict(self, filter_only=False):
        filter_dicts = self.get_filter_dicts()

        if filter_only:
            res = self.get_filter_itself(filter_dicts)
        else:
            res = deepcopy(self.skeleton)
            res['query']['filtered']['filter'] = self.get_filter_itself(filter_dicts)
            if self.aggs:
                res['aggs'] = self.aggs.as_dict(aggs_only=True)
            if self.sort:
                res['sort'] = self.sort
            if self.size:
                res['size'] = self.size
        return res

    def as_json(self, filter_only=False):
        return json.dumps(self.as_dict(filter_only), indent=2)

    def get_filter_dicts(self):
        filter_dicts = [_filter.as_dict(filter_only=True) for _filter in self.filters]

        for key, value in self.items:
            parts = key.split('__')
            if len(parts) > 1:
                key = '__'.join(parts[:-1])
                suffix = parts[-1]
            else:
                suffix = ''

            if suffix == 'in':
                filter_dicts.append({'terms': {key: value}})
            elif suffix == 'exists':
                filter_dicts.append({'exists': {'field': key}})
            elif suffix == 'missing':
                filter_dicts.append({'missing': {'field': key}})
            elif suffix == 'range':
                conditions = {}
                for math_op in ('gte', 'gt', 'lte', 'lt', ):
                    if math_op in value:
                        conditions[math_op] = value[math_op]
                if conditions:
                    filter_dicts.append({'range': {key: conditions}})
            elif suffix == 'daterange':
                conditions = {}
                for condition_key in ('gte', 'gt', 'lte', 'lt', ):
                    if condition_key in value:
                        condition_value = value[condition_key]
                        if isinstance(condition_value, datetime):
                            condition_value = condition_value.strftime('%Y-%m-%d')
                        conditions[condition_key] = EsTz.localize(condition_value)
                if conditions:
                    filter_dicts.append({'range': {key: conditions}})
            elif isinstance(value, (int, long)) or isinstance(value, basestring):
                filter_dicts.append({'term': {key: value}})
            else:
                print 'Es filter: ignored key={}, value={}'.format(key, value), type(key), type(value)

        return filter_dicts


class SimpleFilter(EsFilter):
    def get_filter_itself(self, filter_dicts):
        if filter_dicts:
            return filter_dicts[0]
        else:
            return {}

    def extend(self, *filters, **kwargs):
        raise NotImplementedError


class AndFilter(EsFilter):
    def get_filter_itself(self, filter_dicts):
        return {'and': filter_dicts}


class OrFilter(EsFilter):
    def get_filter_itself(self, filter_dicts):
        return {'or': filter_dicts}


class NotFilter(EsFilter):
    def get_filter_itself(self, filter_dicts):
        return {'not': filter_dicts[0]}


class NestedFilter(EsFilter):
    def __init__(self, path, _filter):
        super(NestedFilter, self).__init__(_filter)

        self.path = path
        self.add_prefix(path)

    def get_filter_itself(self, filter_dicts):
        return {
            'nested': {
                'path': self.path,
                'filter': filter_dicts[0],
            }
        }


class ObjectFilter(SimpleFilter):
    def __init__(self, path, **kwargs):
        super(ObjectFilter, self).__init__(**kwargs)

        self.path = path
        self.add_prefix(path)
