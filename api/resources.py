__author__ = 'Dmitry Golubkov'

import uuid
import json
import deftcore.settings
from datetime import timedelta
from tastypie import fields
from tastypie.exceptions import NotFound, BadRequest
from tastypie.resources import ModelResource, Resource, Bundle, ALL
from tastypie.authentication import ApiKeyAuthentication, Authentication
from tastypie.authorization import DjangoAuthorization, Authorization
from tastypie.serializers import Serializer
from tastypie.utils import trailing_slash
from django.utils import timezone
from django.core.serializers.json import DjangoJSONEncoder
from django.conf.urls import url
from django.db.models import Q
from api.models import Request
from taskengine.models import Task, TRequestProxy, TStepProxy


# from taskengine.atlas.datamgmt import AMIWrapper


class DefaultSerializer(Serializer):
    json_indent = 4

    def to_json(self, data, options=None):
        options = options or {}
        data = self.to_simple(data, options)
        return json.dumps(data,
                          cls=DjangoJSONEncoder,
                          sort_keys=True,
                          ensure_ascii=False,
                          indent=self.json_indent)


class Instance(object):
    def __init__(self, name=None):
        self.name = name or ''
        self.uuid = str(uuid.uuid4())
        self._init_time = timezone.now()

    @property
    def lifetime(self):
        delta = (timezone.now() - self._init_time)
        return delta.days * 24 + delta.seconds // 3600


instances = [Instance('atlas')]


class InstanceResource(Resource):
    uuid = fields.CharField(attribute='uuid')
    name = fields.CharField(attribute='name')
    lifetime = fields.DateTimeField(attribute='lifetime')

    class Meta:
        max_limit = 10
        resource_name = 'instance'
        allowed_methods = ['get']
        object_class = Instance
        authentication = Authentication()
        authorization = Authorization()
        serializer = DefaultSerializer()

    def detail_uri_kwargs(self, bundle_or_obj):
        kwargs = {}

        if isinstance(bundle_or_obj, Bundle):
            kwargs['pk'] = bundle_or_obj.obj.name
        else:
            kwargs['pk'] = bundle_or_obj.name

        return kwargs

    def get_object_list(self, request):
        return instances

    def obj_get_list(self, bundle, **kwargs):
        return self.get_object_list(bundle.request)

    def obj_get(self, bundle, **kwargs):
        pk = str(kwargs['pk'])
        try:
            return (item for item in instances if item.name == pk).next()
        except StopIteration:
            raise NotFound("DEFT instance '%s' is not registered" % pk)

    def obj_create(self, bundle, **kwargs):
        raise BadRequest()

    def obj_update(self, bundle, **kwargs):
        raise BadRequest()

    def obj_delete_list(self, bundle, **kwargs):
        raise BadRequest()

    def obj_delete(self, bundle, **kwargs):
        raise BadRequest()

    def rollback(self, bundles):
        pass


class RequestResource(ModelResource):
    class Meta:
        limit = 100
        queryset = Request.objects.all().order_by('-id')
        resource_name = 'request'
        allowed_methods = ['get', 'post']
        fields = ['id', 'created', 'timestamp', 'action', 'owner', 'body', 'status']
        if deftcore.settings.USE_RESOURCE_AUTH:
            authentication = ApiKeyAuthentication()
            authorization = DjangoAuthorization()
        else:
            authentication = Authentication()
            authorization = Authorization()
        serializer = DefaultSerializer()
        always_return_data = True
        filtering = {
            'action': ALL,
            'body': ALL,
            'owner': ALL,
            'status': ALL
        }

    def unauthorized_result(self, exception):
        super(RequestResource, self).unauthorized_result(exception)

    def prepend_urls(self):
        return [
            url(r'^(?P<resource_name>%s)/actions%s$' % (self._meta.resource_name, trailing_slash()),
                self.wrap_view('get_action_list'),
                name="list of actions"),
            url(r'^(?P<resource_name>%s)/action/(?P<action_name>\w[\w/-]*)%s$' % \
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('perform_action'),
                name='perform action'),
            url(r'^(?P<resource_name>%s)/tag/(?P<tag_name>\w[\w/-]*)%s$' % \
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('view_tag'),
                name='view configuration tag'),
        ]

    def get_action_list(self, request, **kwargs):
        self.method_check(request, allowed=['get'])
        return self.create_response(request, {'result': [e[0] for e in Request.ACTION_LIST]})

    def perform_action(self, request, **kwargs):
        self.method_check(request, allowed=['get'])

        self.is_authenticated(request)
        self.throttle_check(request)

        action_name = kwargs['action_name']
        if not action_name in [e[0] for e in Request.ACTION_LIST]:
            return self.create_response(request, {'result': "Invalid action name: %s" % action_name})

        params = request.GET.dict()
        if isinstance(self._meta.authentication, ApiKeyAuthentication):
            owner = params.pop('username')
            params.pop('api_key')
        else:
            owner = 'default'

        action_request = Request(action=action_name, owner=owner, body=json.dumps(params))
        action_request.save()

        return self.create_response(request, {'result': "Request %d is registered" % action_request.id})

    def view_tag(self, request, **kwargs):
        self.method_check(request, allowed=['get'])

        self.is_authenticated(request)
        self.throttle_check(request)

        tag_name = kwargs['tag_name']
        if not tag_name:
            return self.create_response(request, {'result': "Empty tag name"})

        # ami_wrapper = AMIWrapper()
        # tag = ami_wrapper.get_ami_tag(tag_name)
        tag = None
        return self.create_response(request, {'name': tag_name, 'body': tag})


class TaskResource(ModelResource):
    jedi_task_params = fields.DictField(attribute='jedi_task_params', null=True)
    jedi_task_status = fields.CharField(attribute='jedi_task_status', null=True)
    task_config = fields.DictField(attribute='task_config', null=True)
    formats = fields.CharField(attribute='formats', null=True)
    destination_token = fields.CharField(attribute='destination_token', null=True)
    slice = fields.DecimalField(attribute='slice', null=False)
    hidden = fields.BooleanField(attribute='hidden', null=False)
    slice_input_events = fields.DecimalField(attribute='slice_input_events', null=False)
    has_pileup = fields.BooleanField(attribute='has_pileup', null=True)
    step_id = fields.IntegerField(attribute='step__id')
    start_time_utc = fields.DateTimeField(attribute='start_time_utc', null=True)
    end_time_utc = fields.DateTimeField(attribute='end_time_utc', null=True)
    submit_time_utc = fields.DateTimeField(attribute='submit_time_utc', null=True)
    ttcr_timestamp_utc = fields.DateTimeField(attribute='ttcr_timestamp_utc', null=True)

    def apply_filters(self, request, applicable_filters):
        search_case = request.GET.get('search_case', None)
        reqid = int(request.GET.get('reqid', 0))

        taskname_pattern = None
        name = request.GET.get('name', None)
        if name and '*' in name:
            taskname_pattern = name
            applicable_filters.pop('name__exact', None)

        req_type = request.GET.get('req_type', None)
        if req_type:
            applicable_filters.update({'request__request_type': req_type.upper()})

        base_object_list = super(TaskResource, self).apply_filters(request, applicable_filters)

        if str(search_case).lower() == 'datri_gp_tasks':
            timestamp_days = None
            try:
                timestamp_days = int(request.GET.get('timestamp_days', None))
            except Exception as ex:
                pass
            if timestamp_days:
                timestamp_offset = timezone.now().date() - timedelta(days=timestamp_days)
            else:
                timestamp_offset = timezone.now().date() - timedelta(days=20)
            query_set = (
                    Q(timestamp__gte=timestamp_offset) &
                    Q(provenance__iexact='GP') &
                    Q(step__task_config__icontains='token') & Q(step__task_config__icontains='dst:') &
                    Q(status__in=['running', 'done', 'finished'])
            )
            base_object_list = base_object_list.filter(query_set).distinct()
        elif reqid > 0:
            query_set = (
                Q(request__id=reqid)
            )
            base_object_list = base_object_list.filter(query_set).distinct()
        elif taskname_pattern:
            base_object_list = base_object_list.extra(where=['taskname like %s'],
                                                      params=[taskname_pattern.replace('*', '%')]).distinct()
        return base_object_list

    class Meta:
        limit = 10
        max_limit = 2000
        queryset = Task.objects.filter(~Q(prodSourceLabel='user'))
        resource_name = 'task'
        allowed_methods = ['get']
        fields = ['id',
                  'parent_id',
                  'chain_id',
                  'name',
                  'priority',
                  'project',
                  'reference',
                  'start_time',
                  'end_time',
                  'status',
                  'submit_time',
                  'timestamp',
                  'total_done_jobs',
                  'total_events',
                  'total_req_jobs',
                  'provenance',
                  'phys_group',
                  'total_req_events',
                  'simulation_type',
                  'ttcr_timestamp',
                  'nfiles_to_be_used',
                  'nfiles_used',
                  'nfiles_finished',
                  'nfiles_failed',
                  'nfiles_on_hold']
        if deftcore.settings.USE_RESOURCE_AUTH:
            authentication = ApiKeyAuthentication()
            authorization = DjangoAuthorization()
        else:
            authentication = Authentication()
            authorization = Authorization()
        serializer = DefaultSerializer()
        always_return_data = True
        filtering = {
            'id': ALL,
            'chain_id': ('exact',),
            'parent_id': ('exact',),
            'name': ('exact', 'icontains',),
            'step_id': ALL,
            'simulation_type': ALL
        }


class TRequestResource(ModelResource):
    evgen_steps = fields.ListField(attribute='evgen_steps', null=True)
    is_error = fields.BooleanField(attribute='is_error', null=False)
    creation_time = fields.DateTimeField(attribute='creation_time', null=True)
    approval_time = fields.DateTimeField(attribute='approval_time', null=True)

    class Meta:
        limit = 10
        queryset = TRequestProxy.objects.all()
        resource_name = 't_request'
        allowed_methods = ['get']
        fields = ['id',
                  'manager',
                  'description',
                  'ref_link',
                  'status',
                  'provenance',
                  'request_type',
                  'campaign',
                  'subcampaign',
                  'phys_group',
                  'energy_gev',
                  'project',
                  'reference']
        if deftcore.settings.USE_RESOURCE_AUTH:
            authentication = ApiKeyAuthentication()
            authorization = DjangoAuthorization()
        else:
            authentication = Authentication()
            authorization = Authorization()
        serializer = DefaultSerializer()
        always_return_data = True
        filtering = {
            'id': ('exact', 'gt', 'gte', 'lt', 'lte',),
            'status': ('iexact',),
            'request_type': ('iexact',),
            'phys_group': ('iexact',)
        }


class TStepResource(ModelResource):
    ctag = fields.CharField(attribute='ctag')
    slice = fields.IntegerField(attribute='slice_n')
    request_id = fields.IntegerField(attribute='request_id')

    class Meta:
        limit = 10
        queryset = TStepProxy.objects.all()
        resource_name = 't_step'
        allowed_methods = ['get']
        if deftcore.settings.USE_RESOURCE_AUTH:
            authentication = ApiKeyAuthentication()
            authorization = DjangoAuthorization()
        else:
            authentication = Authentication()
            authorization = Authorization()
        serializer = DefaultSerializer()
        always_return_data = True
        filtering = {
            'id': ALL
        }
