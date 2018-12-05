# -*- coding: utf-8 -*-
from __future__ import unicode_literals

__author__ = 'Dmitry Golubkov'

from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from .models import User

admin.site.register(User, UserAdmin)
