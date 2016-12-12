import copy
import operator
import os
import sys
import time

import shotgun_api3
from carbine import carbine

from sg_wrapper_util import string_to_uuid, get_calling_script

# The Primary Text Keys are the field names to check when not defined.
# For example, calling sg.Project("my_project") will be the same as sg.Project(code = "my_project")
primaryTextKeys = ["code", "login", "name"]

# For most entity types, the pluralise() function will define what the plural version of the entity name is.
# This dictionary defines any custom plural forms that we might want to have.
customPlural = {'Person': "People"}

baseOperator = frozenset([
    'is',
    'is_not',
    'less_than',
    'greater_than',
    'contains',
    'not_contains',
    'starts_with',
    'ends_with',
    'between',
    'not_between',
    'in_last',
    'in_next',
    'in',
    'not_in',
    'type_is',
    'type_is_not',
    'in_calendar_day',
    'in_calendar_week',
    'in_calendar_month',
    'name_contains',
    'name_not_contains',
    'name_starts_with',
    'name_ends_with',
    ])

operatorMap = {
    '!': 'is_not',
    'type': 'type_is',
    '!type': 'type_is_not',
    'startswith': 'starts_with',
    'endswith': 'ends_with',
    '<': 'less_than',
    '>': 'greater_than',
    }

# Shotgun field types where a list is expected
dataTypeList = frozenset([
    'multi_entity',
    'tag_list',
    'addressing'
    ])

# anim only: exclude 'Cut' table to avoid conflicts with the CustomEntity23
if os.getenv('PROD_TYPE', 'anim') == 'anim':
    ignoredTables = [
        'Cut',
    ]
else:
    ignoredTables = []


# translation between baseOperators & peewee syntax - for carbine
# returns a comparator
# TODO: every commented filters
# shotgun filters: https://github.com/shotgunsoftware/python-api/wiki/Reference%3A-Filter-Syntax
# peewee query operators: http://docs.peewee-orm.com/en/latest/peewee/querying.html#query-operators
operatorTranslation = {
    'is': lambda x, y: x == y,
    'is_not': operator.ne,
    'less_than': operator.lt,
    'greater_than': operator.gt,
    'contains': lambda x, y: x.contains(y),
    'not_contains': lambda x, y: ~(x.contains(y)),
    'starts_with': lambda x, y: x.startswith(y),
    'ends_with': lambda x, y: x.endswith(y),
    'between': lambda x, y: x.between(y[0], y[1]),
    'not_between': lambda x, y: ~(x.between(y[0], y[1])),
    # 'in_last',
    # 'in_next',
    'in': operator.lshift,
    'not_in': lambda x, y: ~(x << y),
    # 'type_is',
    # 'type_is_not',
    # 'in_calendar_day',
    # 'in_calendar_week',
    # 'in_calendar_month',
    # 'name_contains',
    # 'name_not_contains',
    # 'name_starts_with',
    # 'name_ends_with',
}


# connection types # TODO doc and more connections
# TODO autogenerate this with carbine using the web page fetched json and "through_join_entity_type"
specialConnections = {
    'PublishedFileDependency': 'published_file',
}



class ShotgunWrapperError(Exception):
    pass

class retryWrapper(shotgun_api3.Shotgun):
    ''' Wraps a shotgun_api3 object and retries any connection attempt when a 503 error si catched
        Subclasses shotgun_api3.Shotgun forces us to use getattribute instead of getattr but
        it allow isinstance to make the wrapper transparent
    '''
    def __init__(self, sg, maxConnectionAttempts, retrySleep, printInfo, exceptionType):
        self._sg = sg
        self.maxConnectionAttempts = maxConnectionAttempts
        self.retrySleep = retrySleep
        self.printInfo = printInfo
        self.exceptionType = exceptionType

    def __getattribute__(self, attr):
        self_sg = object.__getattribute__(self, '_sg')
        if not hasattr(self_sg, attr):
            return object.__getattribute__(self, attr)

        attribute = self._sg.__getattribute__(attr)
        if not callable(attribute):
            return attribute

        def retryHook(*args, **kwargs):
            errorCount = 0
            while True:
                try:
                    res = attribute(*args, **kwargs)
                    break

                except self.exceptionType, err:
                    errorCount += 1
                    if errorCount == self.maxConnectionAttempts:
                        raise

                    if self.printInfo:
                        print '[sg_wrapper] Connection error [%d/%d]: %s' \
                              % (errorCount, self.maxConnectionAttempts, str(err))

                    time.sleep(self.retrySleep)

            # prevent Shotgun instance returning itself to unwrap
            if res == self._sg:
                return self
            return res

        return retryHook


# This is the base Shotgun class. Everything is created from here, and it deals with talking to the
# standard Shotgun API.
class Shotgun(object):

    def __init__(self, sgServer='', sgScriptName='', sgScriptKey='', sg=None,
                 disableApiAuthOverride=False, printInfo=True,
                 carbine='optional', carbineLazyMode=True,  # TODO carbine=None, lazy=False
                 maxConnectionAttempts=5, retrySleep=3,
                 **kwargs):

        # carbine setup
        if carbine not in ['required', 'optional']:
            carbine = None
        self.carbine = carbine

        # if carbine is wanted, try to connect, and fail if it is required
        if carbine and not self.carbineConnectionTest():
            self.carbine = None
            if printInfo:
                print 'Carbine connection failed. Falling back to shotgun connection'

        self.carbineLazyMode = carbineLazyMode
        if not self.carbine:
            self.carbineLazyMode = False

        if sg:
            self._sg = sg
        elif sgServer and sgScriptName and sgScriptKey:
            self._sg = shotgun_api3.Shotgun(sgServer, sgScriptName, sgScriptKey, **kwargs)
        else:
            raise RuntimeError('init requires a shotgun object or server, script name and key')

        # wrap shotgun api around around a retry hook, to avoid crashes due to 503 errors
        # the error to catch is a ProtocolError from the shotgun api, which is either
        # the standard shotgunPythonApi module, or tkCore.tank_vendor.shotgun_api3
        # so we try to get the error type in the imported module, and we only wrap the api if we could
        shotgun_api_module = self._sg.__module__
        if shotgun_api_module in sys.modules:
            exceptionType = sys.modules[shotgun_api_module].ProtocolError
            self._sg = retryWrapper(self._sg, maxConnectionAttempts, retrySleep, printInfo, exceptionType)

        self._entity_types = self.get_entity_list()
        self._entity_fields = {}
        self._entities = {}
        self._entity_searches = []

        self.update_user_info()

        if not disableApiAuthOverride:
            self.update_auth_info(sgScriptName, printInfo=printInfo)

    def carbineConnectionTest(self):
        try:
            carbine.db.connect()
        except carbine.OperationalError:
            if not self.carbine == 'required':
                return False
            raise
        return True

    def pluralise(self, name):
        if name in customPlural:
            return customPlural[name]
        if name[-1] == "y" and name[-3:] != "Day":
            return name[:-1] + "ies"
        if name[-1] in ["s", "h"]:
            return name + "es"

        return name + "s"

    def get_entity_list(self):
        if not self.carbine:
            entitySchema = self._sg.schema_entity_read()
        else:
            entitySchema = carbine.carbineTableDescriptions

        entities = []
        for e in entitySchema:
            if e in ignoredTables:
                continue

            newEntity = {
                'type': e,
                'name': entitySchema[e]['name']['value'].replace(" ", ""),
                'fields': []
            }
            newEntity['type_plural'] = self.pluralise(newEntity['type'])
            newEntity['name_plural'] = self.pluralise(newEntity['name'])
            entities.append(newEntity)

        return entities

    def translate_entity_type(self, entityType):

        ''' Translate entity type to 'real' entity type (ie. CustomEntity02 -> Master)
        '''

        r = [ t for t in self._entity_types if t['type'] == entityType ]

        if not r:
            raise ValueError('Could not find entity of type %s' % entityType)
        else:
            return r[0]['name']

    def get_entity_field_list(self, entityType):
        fields = self.get_entity_fields(entityType)
        return fields.keys()

    def get_entity_fields(self, entityType):
        if entityType not in self._entity_fields:

            if not self.carbine:
                # truncate schema_field_read result - only keep what we use
                d = {}
                for field, fieldDict in self._sg.schema_field_read(entityType).items():
                    d[field] = {
                        k: v['value']
                        for k, v in fieldDict.items()
                        if k in ['editable', 'data_type'] and 'value' in v
                    }

                    display_values = fieldDict                       \
                                        .get('properties', {})       \
                                        .get('display_values', {})   \
                                        .get('value')

                    if display_values:
                        d[field]['display_values'] = display_values

                self._entity_fields[entityType] = d

            else:  # carbine
                model = carbine.get_model(entityType)
                self._entity_fields[entityType] = model.getSchema()

        return self._entity_fields[entityType]

    def get_valid_values(self, entityType, field):
        return self.get_entity_fields(entityType)[field].get('display_values')

    def is_entity(self, entityType):
        for e in self._entity_types:
            if entityType in [e['type'], e['name']]:
                return True
        return False

    def is_entity_plural(self, entityType):
        for e in self._entity_types:
            if entityType in [e['type_plural'], e['name_plural']]:
                return True
        return False

    def get_real_type(self, entityType, defaults_to_paramater=False):
        ''' Translate given type to the real shotgun type (ie Cut => CustomEntity23)
        '''
        for e in self._entity_types:
            if entityType in [e['type'], e['name'], e['type_plural'], e['name_plural']]:
                return e['type']

        if defaults_to_paramater:
            return entityType
        else:
            return None

    def get_entity_description(self, entity):

        if isinstance(entity, Entity):
            return {'type': entity.entity_type(), 'id': entity.entity_id()}

        elif isinstance(entity, dict):
            # if dict represent an entity (ie contains at least id & type), convert type if its an alias to the real name (ex: CustomEntity21 => Editing)
            argType = entity.get('type')
            if argType and 'id' in entity:
                real_type = self.get_real_type(argType)
                if real_type:
                    newarg = copy.deepcopy(entity)
                    newarg['type'] = real_type
                    return newarg

        elif isinstance(entity, list):
            return [self.get_entity_description(e) for e in entity]

        return entity


    def find_entity(self, entityType, key = None, find_one = True, fields = None,
                    order=None, exclude_fields = None, carbine=None, optional_filters=None,
                    **kwargs):
        ''' Find Shotgun entity

        :param optional_filters: filters only applied when the result is not available from the cache
        :type optional_filters: dict

        .. note::
            the optional_filters params allows to bypass some of sg_wrapper's current cache limitations

            usecase from DnD's SGRequester:

                we automatically filter by project if the field exists in the entity
                except if an entry already exists in sg_wrapper's cache
                as if it already is in cache, it was put there by dnd, which always filters with
                the project for its first request, so its always valid

                sg_wrapper's 'optional_filters' argument allows this: it counts as a regular filter
                for everything that could not be fetched from the cache

                this allows better caching as sg_wrapper (atm) does not
                fetch from cache if there is another filter than on 'id'
                and thats the case if we pass the project's filter in the initial request

                this gives better performances as the _find_entiy function is mostly used by State
                which mostly filters by a set of ids and nothing else - except for the project filter added here

                its also useful for some updates done by dnd (ex: the Task statuses)
                as the update using sg_wrapper also updates its cache
                and if the project filter was set regularly, it would not it the cache, and the
                request would pass along to carbine, which takes about a second to update from Shotgun
                if the _find_entity is used right after the update, the returned result would
                use carbine's value, not updated (as it takes about a second), and not the new value
                we just updated the entity with
        '''

        startingTime = time.time()

        filters = {}

        thisEntityType = None
        thisEntityFields = None

        for e in self._entity_types:
            if entityType in [e['type'], e['name'], e['type_plural'], e['name_plural']]:
                thisEntityType = e['type']
                if not e['fields']:
                    e['fields'] = self.get_entity_field_list(thisEntityType)
                thisEntityFields = e['fields']


        # EventLogEntry are not saved to carbine: bypass carbine & forward to sg api
        entityCarbine = carbine  # for the future inner EventLogEntry entity requests
        if thisEntityType == 'EventLogEntry':
            carbine = False  # for the request

        if key:
            if type(key) == int:
                filters['id'] = key
            elif type(key) == str:
                foundPrimaryKey = False
                for fieldName in primaryTextKeys:
                    if fieldName in thisEntityFields:
                        filters[fieldName] = key
                        foundPrimaryKey = True
                        break
                if not foundPrimaryKey:
                    raise ShotgunWrapperError("Entity type '%s' does not have one of the defined primary keys(%s)." % (entityType, ", ".join(primaryTextKeys)))

        for arg in kwargs:
            filters[arg] = self.get_entity_description(kwargs[arg])

        # only fetch from cache if no other filters were specified
        # TODO we could also fetch from cache if an id is given and if we already got all
        # the necessary fields to check if we need to prune a cache value, if any other filter
        # does not correspond to the cached fields
        entities_from_cache = []
        if 'id' in filters and len(filters) == 1:
            if thisEntityType in self._entities:

                if not isinstance(filters['id'], tuple):
                    filters['id'] = ('is', filters['id'])

                op = filters['id'][0]
                value = filters['id'][1]

                if op == 'is':
                    op = 'in'
                    value = [value]
                    filters['id'] = (op, value)

                if op == 'in':
                    missing_value_from_cache = []
                    for val in value:
                        if val in self._entities[thisEntityType]:
                            entity = self._entities[thisEntityType][val]

                            if fields and not(set(fields) <= set(entity.fields())):
                                    # remove entity from cache
                                    # it will be added again after the new query
                                    self.unregister_entity(entity)
                                    missing_value_from_cache.append(val)

                            else:  # found in cache

                                if find_one:
                                    return entity
                                entities_from_cache.append(entity)
                        else:
                            missing_value_from_cache.append(val)

                    if not missing_value_from_cache:
                        return entities_from_cache

                    # not everything has been found: prune found values & search for the rest
                    filters['id'] = (op, missing_value_from_cache)

        if optional_filters:
            for fname, fval in optional_filters.iteritems():
                filters[fname] = self.get_entity_description(fval)

        if not fields:
            fields = self.get_entity_field_list(thisEntityType)

        if exclude_fields:
            for f in exclude_fields:
                if f in fields:
                    fields.remove(f)

        for search in self._entity_searches:
            if search['find_one'] == find_one \
              and search['entity_type'] == thisEntityType \
              and search['filters'] == filters \
              and search['order'] == order \
              and set(fields).issubset(set(search['fields'])):
                return search['result']

        sgOrder = []
        if order:

            i=0
            orderLen = len(order)
            while True:
                try:
                    direction = order[i]
                    field = order[i+1]
                except IndexError:
                    raise RuntimeError('Order error: %s' % str(order))
                else:
                    sgOrder.append({'field_name': field, 'direction': direction})
                    i+=2
                    if i >= orderLen:
                        break

        sgFilters = []
        for f in filters:

            filterValue = filters[f]
            if isinstance(filterValue, tuple):
                op = filterValue[0]
                value = self.get_entity_description(filterValue[1])

                if op not in baseOperator:
                    _op = op
                    op = operatorMap.get(_op, None)

                    if not op:
                        raise ValueError('Unknown operator: %s' % _op)
            else:
                op = 'is'
                value = filterValue

            sgFilters.append([f, op, value])

        result = None

        if find_one:
            sg_result = self.sg_find_one(thisEntityType, sgFilters, fields, sgOrder, carbine=carbine)

            if sg_result:
                result = Entity(self, thisEntityType, sg_result, carbine=entityCarbine)
        else:
            sg_results = self.sg_find(thisEntityType, sgFilters, fields, sgOrder, carbine=carbine)

            result = []
            for sg_result in sg_results:
                result.append(Entity(self, thisEntityType, sg_result, carbine=entityCarbine))

            result.extend(entities_from_cache)

        thisSearch = {}
        thisSearch['find_one'] = find_one
        thisSearch['entity_type'] = thisEntityType
        thisSearch['filters'] = filters
        #thisSearch['sgOrder'] = sgOrder
        thisSearch['order'] = order
        thisSearch['fields'] = fields
        thisSearch['result'] = result
        self._entity_searches.append(thisSearch)

        # print 'find_entity(%s) took %s' % (entityType, time.time() - startingTime)

        return result


    def sg_find_one(self, entityType, filters, fields, order=None, carbine=None):
        if carbine == True or (carbine != False and self.carbine):
            return self.carbine_find(entityType, filters, fields, order, find_one=True)
        else:
            return self._sg.find_one(entityType, filters, fields, order)

    def sg_find(self, entityType, filters, fields, order=None, carbine=None):
        if carbine == True or (carbine != False and self.carbine):
            return self.carbine_find(entityType, filters, fields, order, find_one=False)
        else:
            return self._sg.find(entityType, filters, fields, order)


    def carbine_find(self, entityType, filters, fields, order=None, find_one=False):
        startingTime = time.time()
        # entityType <=> table name (! need to handle translation)
        # ~ select *fields from entityType

        model = carbine.get_model(entityType)

        # TODO handle pseudo join fields (ie entity.Task.created_by.HumanUser.firstname)
        # TODO sometimes shotgun also returns the display name
        queryFields = []

        # by default peewee doesnt return the id, but shotgun does
        if 'id' not in fields:
            fields.append('id')

        # TODO kinda meh way to handle paths
        pathRequested = False
        if 'path' in fields and not model.getFieldType('path'):
            if model.getFieldType('path_cache') and model.getFieldType('path_cache_storage'):
                if 'path_cache' not in fields:
                    fields.append('path_cache')

                if 'path_cache_storage' not in fields:
                    fields.append('path_cache_storage')

                fields.remove('path')
                pathRequested = True


        for field in fields:
            fieldtype = model.getFieldType(field)

            if fieldtype == 'Primitive':
                queryFields.append(getattr(model, field))

            elif fieldtype == 'Entity':
                queryFields.append(getattr(model, field + "__type"))
                queryFields.append(getattr(model, field + "__id"))

            elif fieldtype == 'MultiEntity':
                # join
                # handled later
                pass

        query = model.select(*queryFields)
        # print 'select query: %s' % query

        # TODO handle foreign key
        # TODO handle pseudo join
        fullFilter = None
        for _filter in filters:
            field, relation, values = _filter
            if relation not in operatorTranslation.keys():
                raise RuntimeError('operation %s is not handled (yet!) by sg_wrapper using carbine' % relation)

            fieldtype = model.getFieldType(field)

            filterToAdd = None

            if fieldtype == 'Primitive':
                filterToAdd = operatorTranslation[relation](getattr(model, field), values)

            elif fieldtype == 'Entity':
                # TODO atm only supporting filter on id & type

                if isinstance(values, list):
                    if relation not in ['in', 'not_in']:
                        raise RuntimeError('operation %s not supported on a single entity link using a list of values' % relation)

                    # list of dict, grouped by type, to avoid an overcomplicated query
                    typeValues = {}
                    for value in values:
                        _type = value.get('type')
                        if _type not in typeValues:
                            typeValues[_type] = set()
                        typeValues[_type].add(value.get('id'))

                    for t, ids in typeValues.iteritems():
                        subfilter = None

                        if ids:
                            subfilter = operatorTranslation[relation](
                                getattr(model, field + "__id"), list(ids))

                        if t:
                            if not subfilter:
                                subfilter = getattr(model, field + "__type") == t
                            else:
                                subfilter = subfilter & (getattr(model, field + "__type") == t)

                        if not filterToAdd:
                            filterToAdd = subfilter
                        else:
                            filterToAdd = filterToAdd | subfilter

                elif isinstance(values, dict):
                    if 'id' in values:
                        filterToAdd = operatorTranslation[relation](
                            getattr(model, field + "__id"), values['id'])

                    if 'type' in values:
                        secondFilter = operatorTranslation[relation](
                            getattr(model, field + "__type"), values['type'])

                        if not filterToAdd:
                            filterToAdd = secondFilter
                        else:
                            filterToAdd = filterToAdd & secondFilter

                else:
                    raise RuntimeError('filtering on a link without something else than a dict is not supported (yet)')

            elif fieldtype == 'MultiEntity':
                # TODO atm only support filter on id & type
                raise RuntimeError('MultiEntity filters not supported')

            if not fullFilter:
                fullFilter = filterToAdd
            else:
                fullFilter = fullFilter & filterToAdd

        if fullFilter:
            try:
                query = query.where(fullFilter)
            except:
                print "query: %s" % query
                print "filter: %s %s %s" % (field, relation, values)
                print "operation translation: %s" % operatorTranslation[relation]
                raise

        # print 'filter query: %s' % query


        if order:
            for orderRule in order:
                orderAttr = getattr(model, orderRule['field_name'])
                if orderRule.get('direction') == 'desc':
                    orderAttr = orderAttr.desc()
                query = query.order_by(orderAttr)

        if find_one:
            query = query.limit(1)


        # print "query: %s" % query

        res = []
        # TODO sometimes shotgun returns the display name (dunno why, dunno when) on nested structs
        # in addition to the type & the id
        query = query.dicts()
        dbRes = query.execute()
        for row in dbRes:
            formattedRow = {
                'type': entityType
            }

            for field in fields:
                attr = None
                fieldtype = model.getFieldType(field)

                if fieldtype == 'Primitive':
                    attr = row.get(field)

                    if isinstance(attr, str):
                        attr = attr.encode('utf-8')

                    # TODO thats kinda meh hack to handle the paths
                    if(field == 'path_cache'
                            # and ('path' not in fields or not model.getFieldType('path'))
                            and 'path' not in fields
                            and model.getFieldType('path_cache_storage') == 'Entity'):

                        pcs_id = row.get('path_cache_storage__id')
                        pcs_type = row.get('path_cache_storage__type')
                        if pcs_type and pcs_id:
                            path_cache_storage = self.find_entity(pcs_type, key=pcs_id, find_one=True,
                                                                  fields=['linux_path'], carbine=True)
                            if path_cache_storage:
                                linux_path = path_cache_storage._fields.get('linux_path')
                                if linux_path:
                                    formattedRow['path'] = {
                                        'local_path': os.path.join(linux_path, attr)
                                    }

                    # shotgun does some bs with the thumbnail paths
                    # the EventLogEntry does not give the right path (!), neither for a private nor for a public
                    # so we override the bs he gives us here
                    elif field == 'image':
                        baseUrl = None
                        # private => we got an env variable for the thumbnail url
                        if os.getenv('SHOTGUN_SITE_TYPE', 'cloud') != 'cloud':
                            baseUrl = os.getenv('SHOTGUN_THUMBNAIL_SERV_URL')
                            if baseUrl.endswith('/'):
                                baseUrl = baseUrl[:-1]
                        # public => we dont have an env, so url env + hardcoded relative url for thumbnails
                        else:
                            rootUrl = os.getenv('SHOTGUN_URL')
                            if rootUrl:
                                if rootUrl.endswith('/'):
                                    rootUrl = rootUrl[:-1]
                                baseUrl = '%s/thumbnail' % rootUrl

                        # TODO if the thumbnail url format changes, we need to change it here
                        if baseUrl:
                            attr = '%s/%s/%s' % (baseUrl, entityType, row['id'])
                        else:
                            attr = None

                elif fieldtype == 'Entity':
                    entity_id = row.get(field + "__id")
                    if entity_id:

                        entity_type = row.get(field + "__type")
                        if isinstance(entity_type, str):
                            entity_type = entity_type.encode('ascii', 'ignore'),

                        attr = {
                            'type': entity_type,
                            'id': entity_id,
                        }

                    else:
                        attr = None


                elif fieldtype == 'MultiEntity':
                    linkedModel = carbine.get_model(model.getLinkTable(field))
                    subquery = linkedModel.select().where(linkedModel.origin == row['id'])

                    # print "\tsubquery: %s" % subquery

                    if not self.carbineLazyMode:
                        attr = carbineMultiEntityGetter(subquery, sgw=self)

                    else:
                        attr = LazyObject(carbineMultiEntityGetter, subquery, sgw=self)


                if attr or not formattedRow.get(field):
                    formattedRow[field] = attr

            if pathRequested and not 'path' in formattedRow:
                formattedRow['path'] = None

            res.append(formattedRow)

        # print '|_=> Took %s' % (time.time() - startingTime)

        if find_one:
            if res:
                return res[0]
            else:
                return None

        return res

    def update(self, entity, updateFields):
        ''' Update entity fields

        :param entity: entity to update
        :type entity: :class:`~sg_wrapper.Entity`
        :param updateFields: entity fields to update
        :type updateFields: list or dict

        :return: Nothing
        :rtype: None

        :raises ValueError: if updateFields is neither a list nor a dict

        .. note:: If updateFields is a list of field names, the values will be the values stored
                  in the entity yet to be commited
        '''

        if type(updateFields) is dict:
            entityFields = self.get_entity_fields(entity.entity_type())
            updateData = self._translate_data(entityFields, updateFields)
            self._sg.update(entity._entity_type, entity._entity_id, updateData)

        elif type(updateFields) is list:
            entityFields = self.get_entity_fields(entity.entity_type())

            data = {}
            for f in updateFields:
                data[f] = entity.field(f)

            updateData = self._translate_data(entityFields, data)

            self._sg.update(entity._entity_type, entity._entity_id, updateData)

        else:
            raise ValueError('Field type not supported: %s' % type(updateFields))

    def get_new_shotgun_auth_info(self, scriptName=''):
        ''' Get updated shotgun's auth info for the current script

        :param scriptName: The name of the current script
        :type scriptName: str

        :return: The script name and its API key, or (None,None) if there was a problem creating / retrieving the auth infos.
        :rtype: (str,str)

        .. note:: If no script name is provided, it is guessed by analysing the stack trace (cf get_calling_script)

        .. note:: Use the returned script name in any case instead of the provided one: the shotgun's search is case insensitive while the auth is not
        '''

        if not scriptName:
            scriptName = get_calling_script()
            if not scriptName:
                return (None, None)

        scriptEntity = self.sg_find_one('ApiUser', [['firstname', 'is', scriptName]], ['firstname', 'salted_password'])  # also retrieve firstname because the search is case insensitive but the auth is not

        # if no api was found, search it in the retired api. If it is still not found, generate a key in shotgun
        # TODO replace this by a carbine compliant call
        if not scriptEntity:
            archivedScripts = self._sg.find('ApiUser', [['firstname', 'is', scriptName]], ['firstname', 'salted_password'], [], 'all', 0, True)

            if len(archivedScripts) > 0:
                scriptEntity = archivedScripts[0]
                self._sg.revive('ApiUser', scriptEntity['id'])

            else:
                adminPermission = self.find_entity('PermissionRuleSet', 'api_admin')

                if adminPermission is None:
                    return (None, None)

                scriptEntity = self.create('ApiUser', firstname=scriptName, lastname='1.0', description='autogenerated key', permission_rule_set=adminPermission)
                # TODO handle Fault exception

                scriptEntity.reload()  # needed to retrieve the api key

        scriptName = scriptEntity['firstname']  # retrieve the script name because the 'is' query is case insensitive, but the auth is not
        apiKey = scriptEntity['salted_password']

        return (scriptName, apiKey)

    def update_user_info(self):
        ''' Store the current user in the session_uuid field of this shotgun instance
        '''

        # add current user to the shotgun handle:
        #   the only field available (for now - @FUTURE) is the session uuid
        #   so we convert the current user to a valid uuid
        # use sg_wrapper.util.Shotgun.uuid_to_string(uuid) to retrieve the username
        # from a session uuid

        from getpass import getuser
        self._sg.set_session_uuid( string_to_uuid( getuser() ) )

    def update_auth_info(self, scriptName=None, printInfo=True):
        ''' Update the script name and the api key of this shotgun instance

        :param scriptName: The name of the current script
        :type scriptName: str

        :return: True iff the new script name and api key were properly retrieved
        :rtype: bool

        .. note:: If no script name is provided, it is guessed by analysing the stack trace (cf get_calling_script)
        '''

        name, key = self.get_new_shotgun_auth_info(scriptName)

        if name is not None and key is not None:
            self._sg.config.script_name = name
            self._sg.config.api_key = key
            if printInfo:
                print("Shotgun's script API name is now: %s" % name)
            return True

        return False

    def update_tank_auth(self, tk):
        ''' Bind tank's shotgun handle auth config to this shotgun instance.
            This concern the script name, the api key and the username as an uuid

            :param tk: the tank instance to update
            :type tk: tank.api.Tank

            :return: None
            :rtype: None
        '''
        # update tank handle to bind this shotgun scriptname, apikey & user uuid
        tk.shotgun.config.script_name = self._sg.config.script_name
        tk.shotgun.config.api_key = self._sg.config.api_key
        tk.shotgun.set_session_uuid(self._sg.config.session_uuid)

    def register_entity(self, entity):
        if entity._entity_type not in self._entities:
            self._entities[entity._entity_type] = {}

        if entity._entity_id not in self._entities[entity._entity_type]:
            self._entities[entity._entity_type][entity._entity_id] = entity

    def unregister_entity(self, entity):

        if entity._entity_type in self._entities:
            if entity._entity_id in self._entities[entity._entity_type]:
                del(self._entities[entity._entity_type][entity._entity_id])

    def clear_cache(self):
        self._entities = {}
        self._entity_searches = []

    def __getattr__(self, attrName):

        def find_entity_wrapper(*args, **kwargs):
            return self.find_entity(attrName, find_one = True, *args, **kwargs)

        def find_multi_entity_wrapper(*args, **kwargs):
            return self.find_entity(attrName, find_one = False, *args, **kwargs)

        if self.is_entity(attrName):
            return find_entity_wrapper
        elif self.is_entity_plural(attrName):
            return find_multi_entity_wrapper

        # pickle fix (protocol 2)
        raise AttributeError('Could not get attribute %s' % attrName)

    def commit_all(self):
        for entityType in self._entities:
            for entityId in self._entities[entityType]:
                for entity in self._entities[entityType][entityId]:
                    if entity.modified_fields():
                        entity.commit()

    def create(self, entityType, **kwargs):
        for e in self._entity_types:
            if entityType in [e['type'], e['name'], e['type_plural'], e['name_plural']]:
                thisEntityType = e['type']
                if not e['fields']:
                    e['fields'] = self.get_entity_field_list(thisEntityType)
                thisEntityFields = e['fields']

        entityFields = self.get_entity_fields(thisEntityType)

        data = self._translate_data(entityFields, kwargs)

        sgResult = self._sg.create(thisEntityType, data)

        return Entity(self, sgResult['type'], sgResult)

    def _translate_data(self, entityFields, data):
        ''' Translate sw_wrapper data to shotgun data '''
        translatedData = {}

        for arg in data:

            if arg not in entityFields:
                continue

            # assume a list here
            if entityFields[arg]['data_type'] in dataTypeList:
                translatedData[arg] = []
                for e in data[arg]:
                    if isinstance(e, Entity):
                        translatedData[arg].append({
                        'type': e['type'],
                        'id': e['id']})
                    else:
                        translatedData[arg].append(e)

            else:

                if isinstance(data[arg], Entity):
                    translatedData[arg] = {'type': data[arg].entity_type(),
                            'id': data[arg].entity_id()}
                else:
                    translatedData[arg] = data[arg]

        return translatedData

    def batch(self, requests):
        ''' Batch a list of Shotgun commands

        :param requests: list of commands to execute
        :type requests: list
        :return: list of results (Entity for create/update, bool for delete)
        :rtype: list
        '''

        sgRequests = []

        for request in requests:
            # Make sure entity_type is a real SG type
            for e in self._entity_types:
                if request['entity_type'] in [e['type'], e['name'], e['type_plural'], e['name_plural']]:
                    request['entity_type'] = e['type']

            # Translate sg_wrapper.Entity to SG dict
            if 'data' in request:
                entityFields = self.get_entity_fields(request['entity_type'])
                request['data'] = self._translate_data(entityFields, request['data'])

            sgRequests.append(request)

        sgResults = self._sg.batch(sgRequests)

        results = []
        for sgResult in sgResults:
            if isinstance(sgResult, dict) and 'id' in sgResult and 'type' in sgResult:
                results.append(Entity(self, sgResult['type'], sgResult))
            else:
                results.append(sgResult)

        return results

    ##
    # pickle support

    def _register_for_pickle(self, entity, entityCache):

        entityType = entity['type']
        validFields = self.get_entity_fields(entity['type'])

        if entityType in entityCache and entity['id'] in entityCache[entityType]:
            # skip already registered entity'
            return

        if 'name' in entity:

            if 'name' not in validFields:

                if 'code' in validFields:

                    entity['code'] = entity['name']

                elif 'content' in validFields:

                    entity['content'] = entity['name']

                del(entity['name'])

        # __init__ will call register_entity
        e = Entity(self, entityType, fields=entity)

        # but we dont want to pollute original cache
        del self._entities[entityType][entity['id']]
        entityCache[entityType][entity['id']] = e

    def __getstate__(self):

        odict = self.__dict__.copy() # copy the dict since we change it

        _entities = odict['_entities'].copy() # copy dict as size might change

        # process all cached entities
        # register sub entities (ie tasks for Asset or sg_sequence for Shot...)
        # so after pickle we can access myShot.sg_sequence.code

        for entityType, entitiesDict in _entities.iteritems():

            # fix publish file pickle
            _entitiesDict = entitiesDict.copy()

            for entityId, entity in _entitiesDict.iteritems():

                for field in entity.fields():

                    if field in ['type', 'id']:
                        continue

                    if entity['type'] == 'Attachment' and \
                            (field.startswith('local_path') or field in ['name', 'url', 'content_type', 'link_type']):
                        continue

                    value = entity._fields[field]

                    if not value:
                        continue

                    entityFields = self.get_entity_fields(entityType)

                    # sg_wrapper can inject an 'entity' field so skip it...
                    if field not in entityFields:
                        continue

                    if entityFields[field]['data_type'] == 'entity' and not isinstance(value, Entity):
                        self._register_for_pickle(value, odict['_entities'])

                    elif entityFields[field]['data_type'] in dataTypeList:
                        for item in value:
                            if isinstance(item, dict) and 'id' in item and 'type' in item:

                                # schema_field_read will fail on type AppWelcome
                                if item['type'] not in ['AppWelcome']:

                                    self._register_for_pickle(item, odict['_entities'])

        if '_sg' in odict:
            del odict['_sg']

        return odict

    def __setstate__(self, adict):

        self.__dict__.update(adict)


class Entity(object):
    def __init__(self, shotgun, entity_type, fields, carbine=None):
        self._entity_type = entity_type
        self._shotgun = shotgun
        if carbine and shotgun.carbineLazyMode:
            self._fields = LazyDict(fields)
        else:
            self._fields = fields
        self._fields_changed = {}
        self._sg_filters = []

        self._entity_id = self._fields['id']
        self._shotgun.register_entity(self)
        self._carbine = carbine

    def reload(self, mode='all', fields=None):

        ''' Reload (ie. refresh) entity from Shotgun (no cache)

        :param mode:
            * all: query all entity fields (default)
            * basic: query entity with existing fields
            * replace: query entity with fields provided as argument
            * append: query entity with existing fields + fields provided as argument
        :type mode: str
        :param fields: fields to query (for mode 'replace' or 'append')
        :type fields: dict
        '''

        fieldsToQuery = []

        if mode == 'all':

            self._field_names = self._shotgun.get_entity_field_list(self._entity_type)
            fieldsToQuery = self._field_names
        elif mode == 'basic':
            fieldsToQuery = self._fields.keys()
        elif mode == 'replace':
            fieldsToQuery = fields
        elif mode == 'append':
            fieldsToQuery = self._fields.keys() + fields
        else:
            raise ValueError('Unknown mode: %s' % (mode))

        self._fields = self._shotgun.sg_find_one(self._entity_type, [["id", "is", self._entity_id]], fields = fieldsToQuery, carbine=self._carbine)

    def fields(self):
        # Workaround to fix the attachment access to path fields problem.
        # Attachements are handle differently by SG as some fields
        # are dynamic and not described in the schema making sg_wrapper
        # go wrong.
        if self._entity_type == 'Attachment':
            attrNames = self._fields.keys()
            attrNames.extend(self._fields['this_file'].keys())
            attrNames.remove('this_file')
            return attrNames

        return self._fields.keys()

    def entity_type(self):
        return self._entity_type

    def entity_id(self):
        return self._entity_id

    def field(self, fieldName, fields=None):

        ''' Get entity field

        :param fieldName: field name to get
        :type fieldName: str
        :param fields: list of fields to get (optional, default to all)
        :type fields: list

        .. note::
            for speed purpose, specifying a small list of fields
            could help (if entity is not already in cache)
        '''

        # Workaround to fix the attachment access to path fields problem.
        # Attachements are handle differently by SG as some fields
        # are dynamic and not described in the schema making sg_wrapper
        # go wrong.
        toVisit = [self._fields]
        if self._entity_type == 'Attachment':
            toVisit.append(self._fields['this_file'])

        for currentFields in toVisit:
            if fieldName in currentFields:
                attribute = currentFields[fieldName]
                if type(attribute) == dict and 'id' in attribute and 'type' in attribute:
                    if 'entity' not in attribute:

                        if fields:
                            attribute['entity'] = self._shotgun.find_entity(attribute['type'], id = attribute['id'], fields=fields, carbine=self._carbine)
                        else:
                            attribute['entity'] = self._shotgun.find_entity(attribute['type'], id = attribute['id'], carbine=self._carbine)
                        #attribute['entity'] = Entity(self._shotgun, attribute['type'], {'id': attribute['id']})
                    return attribute['entity']
                elif type(attribute) == list:
                    iterator = self.list_iterator(currentFields[fieldName], fields)
                    attrResult = []
                    for item in iterator:
                        attrResult.append(item)
                    return attrResult
                else:
                    return currentFields[fieldName]

        raise AttributeError("Entity '%s' has no field '%s'" % (self._entity_type, fieldName))

    def list_iterator(self, entities, fields):

        for entity in entities:

            # ie for Asset.tag_list (list of str) or for Asset.tasks (list of sg_wrapper.Entity)
            if isinstance(entity, basestring) or isinstance(entity, Entity):
                yield entity
                # Warning: do not remove it or iterator will break (ie for tag_list)
                continue

            if 'entity' not in entity:
                if fields:
                    entity['entity'] = self._shotgun.find_entity(entity['type'], id = entity['id'], fields=fields, carbine=self._carbine)
                else:
                    entity['entity'] = self._shotgun.find_entity(entity['type'], id = entity['id'], carbine=self._carbine)

            yield entity['entity']

    def modified_fields(self):
        return self._fields_changed.keys()

    def commit(self):
        if not self.modified_fields():
            return False

        self._shotgun.update(self, self._fields_changed.keys())
        self._fields_changed = {}
        return True

    def revert(self, revert_fields = None):
        if revert_fields == None:
            revert_fields = self.modified_fields()
        elif type(revert_fields) == "str":
            revert_fields = [revert_fields]

        for field in self.modified_fields():
            if field in revert_fields:
                self._fields[field] = self._fields_changed[field]
                del self._fields_changed[field]

    def set_field(self, fieldName, value):

        entityFields = self._shotgun.get_entity_fields(self._entity_type)

        if fieldName in entityFields:

            if entityFields[fieldName]['editable'] == True:
                oldValue = self._fields[fieldName]
                self._fields[fieldName] = value
                if fieldName not in self._fields_changed:
                    self._fields_changed[fieldName] = oldValue
            else:
                raise AttributeError("Field '%s' in Entity '%s' is not editable" % (fieldName, self._entity_type))
        else:
            raise AttributeError("Entity '%s' has no field '%s'" % (self._entity_type, fieldName))

    def __getattr__(self, attrName):
        return self.field(attrName)

    def __setattr__(self, attrName, value):
        if attrName[0] == "_":
            self.__dict__[attrName] = value
            return

        self.set_field(attrName, value)

    def __getitem__(self, itemName):
        return self.field(itemName)

    def __setitem__(self, itemName, value):
        self.set_field(itemName, value)

    def upload(self, field, path, displayName=None, tagList=None):
        ''' Uploads local file and links it with current entity

        :param field: field's name in entity. Must be a File/Link field
        :type field: str
        :param path: full path of local file
        :type path: str
        :param displayName: optional displayed name, if None, Shotgun will names it with his local name
        :type displayName: str
        :param tagList: optional tags (comma separated str of tags)
        :type tagList: str
        '''
        self._shotgun._sg.upload(self.entity_type(), self.entity_id(), path, field, displayName, tagList)

    # 'partial' pickle support
    # limitations: could not pickle and unpickle if convert_datetimes_to_utc parameter (see Shotgun api) is not the same
    # after unpickle, call attach method to attach entity to a Shotgun connection

    def attach(self, sg):

        ''' After unpickle, attach entity to a Shotgun connection
        '''

        if hasattr(self, '_shotgun') and '_sg' in self._shotgun.__dict__:
            return

        if isinstance(sg, Shotgun):
            self._shotgun = sg
            self._shotgun.register_entity(self)
        else:
            raise RuntimeError('sg should be of type sg_wrapper.Shotgun not %s' % type(sg))

    def __getstate__(self):
        odict = self.__dict__.copy() # copy the dict since we change it

        if '_shotgun' in odict and '_sg' in odict['_shotgun'].__dict__:

            sg = odict['_shotgun']._sg
            convertUtc = sg.config.convert_datetimes_to_utc
        elif '_pickle_shotgun_convert_datetimes_to_utc' in odict:
            convertUtc = odict['_pickle_shotgun_convert_datetimes_to_utc']
        else:
            raise RuntimeError

        if convertUtc == True:

            # datetimes are in local timezone
            # not pickable so convert to utc then discard timezone

            from datetime import datetime
            # copy _fields dict since we change it
            fieldsDict = odict['_fields'].copy()

            for k in fieldsDict:

                v = fieldsDict[k]

                if isinstance(v, datetime):

                    newDate = fieldsDict[k].astimezone(shotgun_api3.sg_timezone.utc)
                    # discard utc timezone (note that datetime objects are immutable)
                    newDate = newDate.replace(tzinfo=None)

                    fieldsDict[k] = newDate

            odict['_fields'] = fieldsDict

        if '_shotgun' in odict and '_sg' in odict['_shotgun'].__dict__:

            # store shotgun config
            odict['_pickle_shotgun_convert_datetimes_to_utc'] = convertUtc
            # do not remove _shotgun entry anymore as we will pickle it
            # with entity (in order to keep cached entries)
            #del odict['_shotgun'] # remove shotgun entry

        return odict

    def __setstate__(self, adict):

        convertUtc = adict['_pickle_shotgun_convert_datetimes_to_utc']

        if convertUtc == True:

            from datetime import datetime
            fieldsDict = adict['_fields']

            for k in fieldsDict:
                if isinstance(fieldsDict[k], datetime):
                    currentDate = fieldsDict[k]
                    # add utc timezone then convert to local time zone
                    currentDate = currentDate.replace(tzinfo=shotgun_api3.sg_timezone.utc)
                    fieldsDict[k] = currentDate.astimezone(shotgun_api3.sg_timezone.local)

        # do not remove shotgun config - so re pickle will work
        #del adict['_pickle_shotgun_convert_datetimes_to_utc']

        self.__dict__.update(adict)


def carbineMultiEntityGetter(subquery, sgw):
    res = []
    for linkedEntity in subquery:
        if linkedEntity.dest__id:
            row = {}
            innerField = specialConnections.get(linkedEntity.dest__type)

            if innerField:
                outerEntity = sgw.find_entity(linkedEntity.dest__type, id=linkedEntity.dest__id, fields=[innerField], carbine=True)
                if innerField not in outerEntity._fields.keys():
                    raise AttributeError("Entity '%s' has no inner field '%s'"
                                         % (linkedEntity.dest__type, innerField))

                innerEntity = outerEntity[innerField]
                if 'type' not in innerEntity._fields.keys() or 'id' not in innerEntity._fields.keys():
                       raise AttributeError("Entity '%s' has a malformed inner field '%s' (missing either id or type)"
                                            % (linkedEntity.dest__type, innerField, innerEntity))
                row['type'] = innerEntity['type'].encode('ascii', 'ignore')
                row['id'] = innerEntity['id']

            else:
                if isinstance(linkedEntity.dest__type, str):
                    row['type'] = linkedEntity.dest__type.encode('ascii', 'ignore')
                else:
                    row['type'] = linkedEntity.dest__type
                row['id'] = linkedEntity.dest__id

            res.append(row)

    return res

class LazyObject(object):
    def __init__(self, func, *funcArgs, **funcKwargs):
        self.func = func
        self.funcArgs = funcArgs
        self.funcKwargs = funcKwargs

    def get(self):
        return self.func(*self.funcArgs, **self.funcKwargs)

    def __copy__(self):
        return LazyObject(self.func, *self.funcArgs, **self.funcKwargs)

    def __deepcopy__(self, memo):
        # TODO rework deepcopy, as its only a simple one copy, due to problems with peewee queries not deepcopy-able

        # func = copy.deepcopy(self.func, memo)
        # funcArgs = copy.deepcopy(self.funcArgs, memo)
        # funcKwargs = copy.deepcopy(self.funcKwargs, memo)
        # return LazyObject(func, *funcArgs, **funcKwargs)

        return LazyObject(self.func, *self.funcArgs, **self.funcKwargs)


class LazyDict(dict):
    def __getitem__(self, key):
        it = super(LazyDict, self).__getitem__(key)
        if not isinstance(it, LazyObject):
            return it
        res = it.get()
        super(LazyDict, self).__setitem__(key, res)
        return res

    # default get is built in C and does not work with a custom __getitem__, so we need to redefine it
    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __copy__(self):
        return LazyDict(super(LazyDict, self))

    def __deepcopy__(self, memo):
        return LazyDict(copy.deepcopy(super(LazyDict, self), memo))

