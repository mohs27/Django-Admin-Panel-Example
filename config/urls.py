
from django.contrib import admin
from django.urls import include, path

from config import settings

urlpatterns = [
    path('admin/', admin.site.urls),
]

if settings.DEBUG:
    urlpatterns += [
        path('debug/', include('debug_toolbar.urls')),
    ]
