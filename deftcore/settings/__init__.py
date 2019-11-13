"""
Django settings for deftcore project.
"""

from .private import *

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = ['aipanda015.cern.ch',
                 'aipanda076.cern.ch',
                 'aipanda034.cern.ch',
                 'deft-api.cern.ch',
                 'localhost']

# Application definition
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'taskengine.apps.TaskEngineConfig',
    'tastypie',
    'api.apps.ApiConfig',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'deftcore.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR, '../templates')]
        ,
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
            'builtins': ['taskengine.protocol']
        },
    },
]

WSGI_APPLICATION = 'deftcore.wsgi.application'

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True

# Static files (CSS, JavaScript, Images)
STATIC_URL = '/static/'
STATIC_ROOT = 'static'

DATABASE_ROUTERS = ['deftcore.routers.DefaultRouter']

LOGGING_BASE_DIR = os.path.join(BASE_DIR, '../../logs')

if DEBUG:
    DEFAULT_LOGGING_LEVEL = 'DEBUG'
    DEFAULT_LOGGING_FILENAME = 'deftcore-dev.log'
else:
    DEFAULT_LOGGING_LEVEL = 'INFO'
    DEFAULT_LOGGING_FILENAME = 'deftcore.log'

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': '[%(asctime)s] [%(levelname)s] [%(module)s] [%(funcName)s:%(lineno)d] - %(message)s'
        }
    },
    'handlers': {
        'default': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'default',
            'filename': os.path.join(LOGGING_BASE_DIR, DEFAULT_LOGGING_FILENAME),
            'maxBytes': 16 * 1024 * 1024,
            'backupCount': 50
        },
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'default'
        },
        'default_worker': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'default',
            'filename': os.path.join(LOGGING_BASE_DIR, 'deftcore-worker.log'),
            'maxBytes': 16 * 1024 * 1024
        }
    },
    'loggers': {
        'deftcore.log': {
            'handlers': ['default', 'console'],
            'level': DEFAULT_LOGGING_LEVEL
        },
        'deftcore.worker': {
            'handlers': ['default_worker', 'console'],
            'level': 'DEBUG'
        }
    }
}

JIRA_CONFIG = {
    'auth_url': 'https://its.cern.ch/jira/loginCern.jsp',
    'issue_url': 'https://its.cern.ch/jira/rest/api/2/issue/',
    'verify_ssl_certificates': False,
    'issue_template': {
        'fields': {
            'project': {
                'key': 'ATLPSTASKS'
            },
            'issuetype': {
                'name': 'Information Request'
            },
            'summary': "%s",
            'description': "%s"
        }
    },
    'sub_issue_template': {
        'fields': {
            'project': {
                'key': 'ATLPSTASKS'
            },
            'issuetype': {
                'name': 'Sub-task'
            },
            'summary': "%s",
            'description': "%s",
            'parent': {
                'key': "%s"
            }
        }
    },
    'issue_comment_template': {
        'body': "%s"
    },
    'issue_close_template': {
        'update': {
            'comment': [
                {'add': {'body': "%s"}}
            ]
        },
        'fields': {
            'resolution': {
                'name': 'None'
            }
        },
        'transition': {
            'id': '2'
        },
    }
}

VOMS_CERT_FILE_PATH = os.path.join(BASE_DIR, '../../usercert.pem')
VOMS_KEY_FILE_PATH = os.path.join(BASE_DIR, '../../userkey.pem')
X509_PROXY_PATH = os.path.join(BASE_DIR, '../../proxy')

JEDI_CORE_UTILS_PATH = os.path.join(BASE_DIR, '../../panda-server/pandaserver/srvcore/CoreUtils.py')
JEDI_CLIENT_PATH = os.path.join(BASE_DIR, '../../panda-server/pandaserver/userinterface/Client.py')

if DEBUG:
    USE_RESOURCE_AUTH = False
else:
    USE_RESOURCE_AUTH = True

TASTYPIE_DEFAULT_FORMATS = ['json']

SESSION_ENGINE = 'taskengine.sessions'

RUCIO_ACCOUNT_NAME = 'prodsys'

AMI_API_V2_BASE_URL = 'https://ami.in2p3.fr/AMI2/api/'
AMI_API_V2_BASE_URL_REPLICA = 'https://atlas-ami.cern.ch/AMI2/api/'

AGIS_API_BASE_URL = 'http://atlas-agis-api.cern.ch'

MONITORING_REQUEST_LINK_FORMAT = 'https://prodtask-dev.cern.ch/prodtask/inputlist_with_request/%d/'

REQUEST_GRACE_PERIOD = 1


# 61013 (STOMP) and 61023 (STOMP over SSL, with X.509 authentication)
class MessagingConfig:
    HOSTNAME = 'atlas-mb.cern.ch'
    PORT = 61023
    QUEUE = '/topic/rucio.events'

    def __init__(self):
        pass
