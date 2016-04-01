import shotgun_api3

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


class ShotgunWrapperError(Exception):
    pass


# This is the base Shotgun class. Everything is created from here, and it deals with talking to the
# standard Shotgun API.
class Shotgun(object):
    
    def __init__(self, sgServer='', sgScriptName='', 
            sgScriptKey='', sg=None, **kwargs):
        
        if sg:
            self._sg = sg
        elif sgServer and sgScriptName and sgScriptKey:
            self._sg = shotgun_api3.Shotgun(sgServer, sgScriptName, sgScriptKey, **kwargs)
        else:
            raise RuntimeError('init requires a shotgun object or server, script name and key')

        self._entity_types = self.get_entity_list()
        self._entity_fields = {}
        self._entities = {}
        self._entity_searches = []
    
    def pluralise(self, name):
        if name in customPlural:
            return customPlural[name]
        if name[-1] == "y" and name[-3:] != "Day":
            return name[:-1] + "ies"
        if name[-1] in ["s", "h"]:
            return name + "es"
        
        return name + "s"
    
    def get_entity_list(self):
        entitySchema = self._sg.schema_entity_read()
        entities = []
        for e in entitySchema:
            newEntity = {'type': e, 'name': entitySchema[e]['name']['value'].replace(" ", ""), 'fields': []}
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
            self._entity_fields[entityType] = self._sg.schema_field_read(entityType)
        return self._entity_fields[entityType]
    
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
    
    def find_entity(self, entityType, key = None, find_one = True, fields = None,
            order=None, exclude_fields = None, **kwargs):
        filters = {}
        
        thisEntityType = None
        thisEntityFields = None
        
        for e in self._entity_types:
            if entityType in [e['type'], e['name'], e['type_plural'], e['name_plural']]:
                thisEntityType = e['type']
                if not e['fields']:
                    e['fields'] = self.get_entity_field_list(thisEntityType)
                thisEntityFields = e['fields']
        
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
            if isinstance(kwargs[arg], Entity):
                filters[arg] = {'type': kwargs[arg].entity_type(), 'id': kwargs[arg].entity_id()}
            else:
                filters[arg] = kwargs[arg]
        
        if 'id' in filters:
            if thisEntityType in self._entities and filters['id'] in self._entities[thisEntityType]:
                
                entity = self._entities[thisEntityType][filters['id']]

                if fields: 
                    
                    # check all required fields are already 
                    # in cached entity fields
                    if set(fields) <= set(entity.fields()):
                        # from cache ...
                        return entity
                    else:
                        # remove entity from cache 
                        # it will be added again after the new query
                        self.unregister_entity(entity)
                else:
                    # from cache ...
                    return entity

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
                value = filterValue[1]
                
                if isinstance(value, Entity):
                    value = {'type': value.entity_type(), 'id': value.entity_id()}
                
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
            sg_result = self.sg_find_one(thisEntityType, sgFilters, fields, sgOrder)

            if sg_result:
                result = Entity(self, thisEntityType, sg_result)
        else:
            sg_results = self.sg_find(thisEntityType, sgFilters, fields, sgOrder)
            
            result = []
            for sg_result in sg_results:
                result.append(Entity(self, thisEntityType, sg_result))

        thisSearch = {}
        thisSearch['find_one'] = find_one
        thisSearch['entity_type'] = thisEntityType
        thisSearch['filters'] = filters
        #thisSearch['sgOrder'] = sgOrder
        thisSearch['order'] = order
        thisSearch['fields'] = fields
        thisSearch['result'] = result
        self._entity_searches.append(thisSearch)
        
        return result

    def sg_find_one(self, entityType, filters, fields, order=None):
        return self._sg.find_one(entityType, filters, fields, order)

    def sg_find(self, entityType, filters, fields, order=None):
        return self._sg.find(entityType, filters, fields, order)
    
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

        elif type(udpateFields) is list:
            print('Warning: sg_wrapper shotgun.update using a field list is deprecated')

            entityFields = self.get_entity_fields(entity.entity_type())

            data = {}
            for f in updateFields:
                data[f] = entity.field(f)

            updateData = self._translate_data(entityFields, data)

            self._sg.update(entity._entity_type, entity._entity_id, updateData)

        else:
            raise ValueError('Field type not supported: %s' % type(updateFields))


    
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
            if entityFields[arg]['data_type']['value'] in dataTypeList:
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

                    if entityFields[field]['data_type']['value'] == 'entity' and not isinstance(value, Entity): 
                        self._register_for_pickle(value, odict['_entities'])
                        
                    elif entityFields[field]['data_type']['value'] in dataTypeList:
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
    def __init__(self, shotgun, entity_type, fields):
        self._entity_type = entity_type
        self._shotgun = shotgun
        self._fields = fields
        self._fields_changed = {}
        self._sg_filters = []

        self._entity_id = self._fields['id']
        self._shotgun.register_entity(self)
    
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
        
        self._fields = self._shotgun.sg_find_one(self._entity_type, [["id", "is", self._entity_id]], fields = fieldsToQuery)
    
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
        
        if fieldName in self._fields:
            attribute = self._fields[fieldName]
            if type(attribute) == dict and 'id' in attribute and 'type' in attribute:
                if 'entity' not in attribute:

                    if fields:
                        attribute['entity'] = self._shotgun.find_entity(attribute['type'], id = attribute['id'], fields=fields)
                    else:
                        attribute['entity'] = self._shotgun.find_entity(attribute['type'], id = attribute['id'])
                    #attribute['entity'] = Entity(self._shotgun, attribute['type'], {'id': attribute['id']})
                return attribute['entity']
            elif type(attribute) == list:
                iterator = self.list_iterator(self._fields[fieldName], fields)
                attrResult = []
                for item in iterator:
                    attrResult.append(item)
                return attrResult
            else:
                return self._fields[fieldName]
            
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
		    entity['entity'] = self._shotgun.find_entity(entity['type'], id = entity['id'], fields=fields)
                else:
		    entity['entity'] = self._shotgun.find_entity(entity['type'], id = entity['id'])

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
            
            if entityFields[fieldName]['editable']['value'] == True:
                oldValue = self._fields[fieldName]
                self._fields[fieldName] = value
                if fieldName not in self._fields_changed:
                    self._fields_changed[fieldName] = oldValue
            else:
                raise AttributeError("Field '%s' in Entity '%s' is not editable" % (fieldName, self._entity_type))
        else:
            raise AttributeError("Entity '%s' has no field '%s'" % (self._entity_type, fieldName))
        
    def __getattr__(self, attrName):
        # Workaround to fix the attachment access to path fields problem.
        # Attachements are handle differently by SG as some fields
        # are dynamic and not described in the schema making sg_wrapper
        # go wrong.
        if self._entity_type == 'Attachment':
            if attrName in self._fields:
                return self._fields[attrName]
            elif attrName in self._fields['this_file']:
                return self._fields['this_file'][attrName]
            else:
                raise AttributeError("Entity '%s' has no field '%s'" % (
                        self._entity_type, attrName))
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
    
    def upload(self, field, path):
        self._shotgun._sg.upload(self.entity_type(), self.entity_id(), path, field)
    
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
        
