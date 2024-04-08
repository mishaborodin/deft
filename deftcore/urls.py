__author__ = 'Dmitry Golubkov'

# from django.conf.urls import include, url
from tastypie.api import Api
from api import views
from api.resources import InstanceResource, RequestResource, TaskResource, TRequestResource, TStepResource
from django.contrib import admin
from django.http import HttpResponseRedirect
from django.urls import reverse

admin.autodiscover()

# default_api = Api(api_name='v1')
# default_api.register(InstanceResource())
# default_api.register(RequestResource())
# default_api.register(TaskResource())
# default_api.register(TRequestResource())
# default_api.register(TStepResource())
#
# urlpatterns = [
#     # url(r'^$', lambda r: HttpResponseRedirect(
#     #     reverse('api_dispatch_list', kwargs={'resource_name': 'instance', 'api_name': 'v1'}))),
#     # url(r'^test/', views.test_view),
#     # url(r'^admin/', admin.site.urls),
#     # url(r'^api/', include(default_api.urls)),
# ]
