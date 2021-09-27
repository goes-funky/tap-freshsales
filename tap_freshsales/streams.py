# If using the class-based model, this is where all the stream classes and their corresponding functions live.
import json
import singer
import datetime
from tap_freshsales import tap_utils

LOGGER = singer.get_logger()


class Stream:
    stream_id = None
    stream_name = None
    endpoint = None
    include = None
    query = None
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    static_filters = []
    filters = []

    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state

    def _get_filters(self):
        # Not all entities are filtered through views
        if self.static_filters:
            return self.filters

        filters = []
        if self.filters:
            try:
                filters = self.client.get_filters(self.endpoint)
            except Exception as err:
                LOGGER.error(err)

        return filters

    def sync(self, filter_entity=False, url=None):
        """ Some entities are filtered from views """
        if not filter_entity:
            LOGGER.info("Syncing stream '{}'".format(self.stream_name))

        filters = self._get_filters()
        if filters and not filter_entity:
            for f in filters:
                # make sure to not have duplicates from different views/ filters
                if not self.static_filters:
                    view_name = f.get("name", False) or f.get("display_name", False)
                    if self.filters and view_name not in self.filters:
                        continue

                    LOGGER.info(
                        "Syncing stream '{}' of view '{}' with ID {}".format(self.stream_name, f['name'], f['id']))
                    url = self.client.url(self.endpoint, query=self.query + str(f['id']) + self.include)

                else:
                    url = self.client.url(self.endpoint, filter=f, include=self.include)

                yield from self.sync(filter_entity=True, url=url)

        else:
            url = url or self.client.url(self.endpoint)
            records = self.client.gen_request(method='GET', stream=self.stream_id, url=url, name=self.stream_name)
            for record in records:
                if record.get('amount', False):
                    record['amount'] = float(record['amount'])  # cast amount to float
                yield record


class Accounts(Stream):
    """
        The parameter filter is mandatory.
        Only one filter is allowed at a time.
        Getting multiple filtered accounts is not possible.
        Use 'include' to embed additional details in the response.
    """
    stream_id = 'sales_accounts'
    stream_name = 'accounts'
    endpoint = 'api/sales_accounts'
    include = ''
    query = 'view/'
    # these are default views
    filters = ['All Accounts', 'Recycle Bin']


class Appointments(Stream):
    """
        The parameter filter is mandatory.
        Only one filter is allowed at a time.
        Getting multiple filtered appointments is not possible.
        Use 'include' to embed additional details in the response.
    """
    stream_id = stream_name = 'appointments'
    endpoint = 'api/appointments?filter={filter}&include={include}'
    include = 'creater,targetable,appointment_attendees'
    static_filters = True
    filters = ['past', 'upcoming']


class Contacts(Stream):
    """
        The parameter filter is mandatory.
        Only one filter is allowed at a time.
        Getting multiple filtered contacts is not possible.
        Use 'include' to embed additional details in the response.
    """
    stream_id = stream_name = 'contacts'
    name = "contact"
    endpoint = 'api/contacts'
    include = '?include=notes'
    query = 'view/'
    child = False
    parent_id = False
    filters = ['All Contacts', 'Recycle Bin']

    def sync(self):
        filters = self.client.get_filters(self.endpoint)
        # all inclusive filters - skip duplicated contacts
        for contact_filter in filters:
            if contact_filter['name'] not in self.filters:
                continue

            view_id = contact_filter['id']
            view_name = contact_filter['name']
            if not self.parent_id:
                LOGGER.info(
                    "Syncing stream '{}' of view '{}' with ID {}".format(self.stream_name, view_name, view_id))
            endpoint = self.client.url(self.endpoint, query=self.query + str(view_id) + self.include)
            records = self.client.gen_request('GET', self.stream_name, endpoint, name=self.child, parent_repeat=True)

            for record in records:
                yield record


class ContactNotes(Contacts):
    stream_id = 'contact_notes'
    child = 'notes'


class ContactActivities(Contacts):
    stream_id = 'contact_activities'
    child = 'activities'
    endpoint = 'api/filtered_search/contact'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]

    def sync(self):
        start = self.client.get_start(self.stream_id)
        data = {
            "filter_rule": [
                {
                    "attribute": self.replication_keys[0],
                    "operator": "is_after",
                    "value": start
               }
          ]
        }
        LOGGER.info("Syncing stream activities from contacts")
        endpoint = self.client.url(self.endpoint)
        records = self.client.gen_request('POST', 'contacts', endpoint, payload=data)
        state_date = start
        for record in records:
            state_date = record['updated_at']

            LOGGER.info("Syncing stream {} of contact with ID {} from {}".format(
                self.stream_name, record['id'], state_date))
            endpoint = self.client.url('api/contacts', query=str(record['id']) + '/' + self.child)
            children = self.client.gen_request('GET', 'contacts', endpoint, name=self.child)
            if isinstance(children, dict) and children.get("error", False):
                LOGGER.warning("Exception on request: {}".format(children["error"]))
                break

            for child in children:
                yield child

        # update stream state with 1 sec for the next data retrieval
        state_date = tap_utils.strftime(tap_utils.strptime(state_date) + datetime.timedelta(seconds=1))
        tap_utils.update_state(self.client.state, self.stream_id, state_date)
        singer.write_state(self.client.state)


class Leads(Stream):
    """
        The parameter filter is mandatory.
        Only one filter is allowed at a time.
        Getting multiple filtered deals is not possible.
        Use 'include' to embed additional details in the response.
    """
    stream_id = stream_name = 'leads'
    endpoint = 'api/leads'
    include = ''
    query = 'view/'
    filters = ['All Leads']


class Deals(Stream):
    """
        The parameter filter is mandatory.
        Only one filter is allowed at a time.
        Getting multiple filtered deals is not possible.
        Use 'include' to embed additional details in the response.
    """
    stream_id = stream_name = 'deals'
    endpoint = 'api/deals'
    include = ''
    query = 'view/'
    filters = ['Open Deals', 'Lost Deals', 'Won Deals', 'Recycle Bin']


class Owners(Stream):
    stream_id = 'owners'
    stream_name = 'users'
    endpoint = 'api/selector/owners'


class Sales(Stream):
    stream_id = stream_name = 'sales'
    endpoint = 'api/sales_activities'


class Tasks(Stream):
    """
        The parameter filter is mandatory per task request
        Only one filter is allowed at a time. Getting multiple filtered tasks is not possible.
        For example, you can’t get both open and overdue tasks in a single request.
    """
    stream_id = stream_name = 'tasks'
    endpoint = 'api/tasks?filter={filter}'
    filters = ['open', 'completed']

    sales_activity_outcomes = {}
    sales_activity_types = {}

    def set_sales_activities(self):
        sales_activity_types = self.client.gen_request(method='GET',
                                                       url=self.client.url('api/selector/sales_activity_entity_types'),
                                                       stream=None)
        sales_activity_outcomes = self.client.gen_request(method='GET',
                                                          url=self.client.url('api/selector/sales_activity_outcomes'),
                                                          stream=None)
        for sale_act_type in sales_activity_types:
            self.sales_activity_types[sale_act_type['id']] = sale_act_type['name']

        for sale_act_outcome in sales_activity_outcomes:
            self.sales_activity_outcomes[sale_act_outcome['id']] = sale_act_outcome['name']

        return

    def sync(self):
        # update sales activities to match labels
        self.set_sales_activities()

        stream = self.endpoint
        # task has static filters - date related could be filtered after import
        # filters = ['open', 'due today', 'due tomorrow', 'overdue', 'completed']
        for state_filter in self.filters:
            LOGGER.info("Syncing stream {} {}".format(state_filter, stream))
            records = self.client.gen_request('GET', self.stream_name,
                                              self.client.url(stream, filter=state_filter,
                                                              include='owner,users,targetable'))
            for record in records:
                # update boolean status with label
                record['status'] = state_filter
                record['outcome_label'] = self.sales_activity_outcomes.get(record['outcome_id'], None)
                record['task_type_label'] = self.sales_activity_types.get(record['task_type_id'], None)
                yield record


class CustomModule(Stream):
    stream_id = stream_name = 'custom_module'
    endpoint = 'api/filtered_search/{stream}'
    custom_module = 'settings/module_customizations'
    custom_fields = 'settings/{name}/forms'
    replication_method = "INCREMENTAL"
    replication_keys = ['updated_at']

    def get_custom_module_schema(self):
        schema = {}
        endpoint = self.custom_fields.format(name=self.stream_name)
        custom_fields = self.client.gen_request('GET', stream=None, url=self.client.url(endpoint))
        forms = [x for x in custom_fields][0]
        if not forms:
            return schema

        # fields located inside basic_info :: ['basic_information','hidden_fields']
        custom_fields = forms.get('fields', False) and forms.get('fields', False)[0]
        for custom_field in custom_fields.get('fields', False):
            field_name, field_type = custom_field.get('name', False), custom_field.get('type', False)
            # all custom field types are strings actually
            schema[field_name] = {'type': ['null', 'string']}
        return schema

    def get_custom_modules(self):
        """
            return dict: schema per custom module
        """
        endpoint = self.custom_module
        custom_modules = self.client.gen_request(method='GET', stream=self.stream_id,
                                                 url=self.client.url(endpoint), name='module_customizations')
        all_custom_modules = {}
        for custom_module in custom_modules:
            entity = custom_module['entity_name']
            # override stream name
            self.stream_id = self.stream_name = entity
            self.endpoint = self.endpoint.format(stream=entity)
            schema = self.get_custom_module_schema()
            all_custom_modules[entity] = schema
        return all_custom_modules

    def sync(self):
        stream = self.endpoint.format(stream=self.stream_name)
        start = self.client.get_start(self.stream_name)
        repl = self.replication_keys
        data = {
            "filter_rule": [
                {
                    "attribute": repl and repl[0] or False,
                    "operator": "is_after",
                    "value": start
                }
            ]
        }
        # GET custom modules
        try:
            records = self.client.gen_request('POST', stream, self.client.url(stream), payload=data)
        except Exception as e:
            # eg. may not have updated_at field. Request with date false in payload will fail
            LOGGER.error("Exception on custom module sync with message: ", e)
            return

        start_state = start
        for record in records:
            LOGGER.info("{}: Syncing details".format(record['id']))
            if record.get('updated_at', False):
                record_date = tap_utils.strftime(tap_utils.strptime(record['updated_at']))
                if record_date >= start:
                    start_state = record.get('updated_at', False)
                    yield record
            else:
                yield record

        # update stream state with 1 sec for the next data retrieval
        start_state = tap_utils.strftime(tap_utils.strptime(start_state) + datetime.timedelta(seconds=1))
        tap_utils.update_state(self.client.state, self.stream_name, start_state)
        singer.write_state(self.client.state)


class Territories(Stream):
    stream_id = stream_name = 'territories'
    endpoint = 'api/selector/territories'


class DealStages(Stream):
    stream_id = stream_name = 'deal_stages'
    endpoint = 'api/selector/deal_stages'


class DealReasons(Stream):
    stream_id = stream_name = 'deal_reasons'
    endpoint = 'api/selector/deal_reasons'


class DealTypes(Stream):
    stream_id = stream_name = 'deal_types'
    endpoint = 'api/selector/deal_types'


class IndustryTypes(Stream):
    stream_id = stream_name = 'industry_types'
    endpoint = 'api/selector/industry_types'


class BusinessTypes(Stream):
    stream_id = stream_name = 'business_types'
    endpoint = 'api/selector/business_types'


class Campaigns(Stream):
    stream_id = stream_name = 'campaigns'
    endpoint = 'api/selector/campaigns'


class DealPaymentStatuses(Stream):
    stream_id = stream_name = 'deal_payment_statuses'
    endpoint = 'api/selector/deal_payment_statuses'


class DealProducts(Stream):
    stream_id = stream_name = 'deal_products'
    endpoint = 'api/selector/deal_products'


class DealPipelines(Stream):
    stream_id = stream_name = 'deal_pipelines'
    endpoint = 'api/selector/deal_pipelines'


class ConstactStatuses(Stream):
    stream_id = stream_name = 'contact_statuses'
    endpoint = 'api/selector/contact_statuses'


class SalesActivityTypes(Stream):
    stream_id = stream_name = 'sales_activity_types'
    endpoint = 'api/selector/sales_activity_types'


class SalesActivityOutcomes(Stream):
    stream_id = stream_name = 'sales_activity_outcomes'
    endpoint = 'api/selector/sales_activity_outcomes'


class SalesActivityEntityTypes(Stream):
    stream_id = stream_name = 'sales_activity_entity_types'
    endpoint = 'api/selector/sales_activity_entity_types'


class LifecycleStages(Stream):
    stream_id = stream_name = 'lifecycle_stages'
    endpoint = 'api/selector/lifecycle_stages'


class Currencies(Stream):
    stream_id = stream_name = 'currencies'
    endpoint = 'api/selector/currencies'


STREAM_OBJECTS = {
    # Main entities
    'accounts': Accounts,
    'appointments': Appointments,
    'contacts': Contacts,
    'leads': Leads,  # Leads only work with old version of freshsales
    'deals': Deals,
    'owners': Owners,
    'sales_activities': Sales,
    'tasks': Tasks,

    # custom module support
    'custom_module': CustomModule,

    # configuration support
    'territories': Territories,
    'deal_stages': DealStages,
    'deal_reasons': DealReasons,
    'deal_types': DealTypes,
    'industry_types': IndustryTypes,
    'business_types': BusinessTypes,
    'campaigns': Campaigns,
    'deal_payment_statuses': DealPaymentStatuses,
    'deal_products': DealProducts,
    'deal_pipelines': DealPipelines,
    'contact_statuses': ConstactStatuses,
    'sales_activity_types': SalesActivityTypes,
    'sales_activity_outcomes': SalesActivityOutcomes,
    'sales_activity_entity_types': SalesActivityEntityTypes,
    'lifecycle_stages': LifecycleStages,
    'currencies': Currencies,

    # Contact Children
    'contact_activities': ContactActivities,
    'contact_notes': ContactNotes
}
