__author__ = 'Dmitry Golubkov'

from django.http import HttpResponse


def test_view(request):
    secured = request.is_secure()
    return HttpResponse('api, secured={0}'.format(str(secured)))
