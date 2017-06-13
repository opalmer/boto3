# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import sys
from collections import namedtuple


_ServiceContext = namedtuple(
    'ServiceContext',
    ['service_name', 'service_model', 'service_waiter_model',
     'resource_json_definitions']
)


class ServiceContext(_ServiceContext):
    """Provides important service-wide, read-only information about a service

    :type service_name: str
    :param service_name: The name of the service

    :type service_model: :py:class:`botocore.model.ServiceModel`
    :param service_model: The model of the service.

    :type service_waiter_model: :py:class:`botocore.waiter.WaiterModel` or
        a waiter model-like object such as
        :py:class:`boto3.utils.LazyLoadedWaiterModel`
    :param service_waiter_model: The waiter model of the service.

    :type resource_json_definitions: dict
    :param resource_json_definitions: The loaded json models of all resource
        shapes for a service. It is equivalient of loading a
        ``resource-1.json`` and retrieving the value at the key "resources".
    """
    pass


def import_module(name):
    """Import module given a name.

    Does not support relative imports.

    """
    __import__(name)
    return sys.modules[name]


def lazy_call(full_name, **kwargs):
    parent_kwargs = kwargs

    def _handler(**kwargs):
        module, function_name = full_name.rsplit('.', 1)
        module = import_module(module)
        kwargs.update(parent_kwargs)
        return getattr(module, function_name)(**kwargs)

    return _handler


def inject_attribute(class_attributes, name, value):
    if name in class_attributes:
        raise RuntimeError(
            'Cannot inject class attribute "%s", attribute '
            'already exists in class dict.' % name)
    else:
        class_attributes[name] = value


class LazyLoadedWaiterModel(object):
    """A lazily loaded waiter model

    This does not load the service waiter model until an attempt is made
    to retrieve the waiter model for a specific waiter. This is helpful
    in docstring generation where we do not need to actually need to grab
    the waiter-2.json until it is accessed through a ``get_waiter`` call
    when the docstring is generated/accessed.
    """
    def __init__(self, bc_session, service_name, api_version):
        self._session = bc_session
        self._service_name = service_name
        self._api_version = api_version

    def get_waiter(self, waiter_name):
        return self._session.get_waiter_model(
            self._service_name, self._api_version).get_waiter(waiter_name)

import random
import json
import urllib
import six
import time
import logging
from botocore.config import Config
from boto3.compat import urllib

logger = logging.getLogger("boto3.utils")


def retry_url(url, retry_on_404=True, num_retries=10, timeout=None):
    """
    Retry a url.  This is specifically used for accessing the metadata
    service on an instance.  Since this address should never be proxied
    (for security reasons), we create a ProxyHandler with a NULL
    dictionary to override any proxy settings in the environment.
    """
    config = Config()
    for i in range(0, num_retries):
        try:
            proxy_handler = urllib.request.ProxyHandler({})
            opener = urllib.request.build_opener(proxy_handler)
            req = urllib.request.Request(url)
            r = opener.open(req, timeout=timeout)
            result = r.read()

            if(not isinstance(result, six.string_types) and
                   hasattr(result, 'decode')):
                result = result.decode('utf-8')

            return result
        except urllib.error.HTTPError as e:
            code = e.getcode()
            if code == 404 and not retry_on_404:
                return ''
        except Exception as e:
            logger.exception('Caught exception reading instance data')
        # If not on the last iteration of the loop then sleep.
        if i + 1 != num_retries:
            logger.debug('Sleeping before retrying')
            time.sleep(min(2 ** i, config.get('Boto', 'max_retry_delay', 60)))
    logger.error('Unable to read instance data, giving up')
    return ''


class LazyLoadMetadata(dict):
    def __init__(self, url, num_retries, timeout=None):
        self._url = url
        self._num_retries = num_retries
        self._leaves = {}
        self._dicts = []
        self._timeout = timeout
        data = retry_url(self._url, num_retries=self._num_retries, timeout=self._timeout)
        if data:
            fields = data.split('\n')
            for field in fields:
                if field.endswith('/'):
                    key = field[0:-1]
                    self._dicts.append(key)
                else:
                    p = field.find('=')
                    if p > 0:
                        key = field[p + 1:]
                        resource = field[0:p] + '/openssh-key'
                    else:
                        key = resource = field
                    self._leaves[key] = resource
                self[key] = None

    def _materialize(self):
        for key in self:
            self[key]

    def __getitem__(self, key):
        if key not in self:
            # allow dict to throw the KeyError
            return super(LazyLoadMetadata, self).__getitem__(key)

        # already loaded
        val = super(LazyLoadMetadata, self).__getitem__(key)
        if val is not None:
            return val

        if key in self._leaves:
            resource = self._leaves[key]
            last_exception = None

            for i in range(0, self._num_retries):
                try:
                    val = retry_url(
                        self._url + urllib.parse.quote(resource,
                                                       safe="/:"),
                        num_retries=self._num_retries,
                        timeout=self._timeout)
                    if val and val[0] == '{':
                        val = json.loads(val)
                        break
                    else:
                        p = val.find('\n')
                        if p > 0:
                            val = val.split('\n')
                        break

                except ValueError as e:
                    logger.debug(
                        "encountered '%s' exception: %s" % (
                            e.__class__.__name__, e))
                    logger.debug(
                        'corrupted JSON data found: %s' % val)
                    last_exception = e

                except Exception as e:
                    logger.debug("encountered unretryable" +
                                   " '%s' exception, re-raising" % (
                                       e.__class__.__name__))
                    last_exception = e
                    raise

                logger.error("Caught exception reading meta data" +
                               " for the '%s' try" % (i + 1))

                if i + 1 != self._num_retries:
                    next_sleep = min(
                        random.random() * 2 ** i,
                        boto.config.get('Boto', 'max_retry_delay', 60))
                    time.sleep(next_sleep)
            else:
                logger.error('Unable to read meta data, giving up')
                logger.error(
                    "encountered '%s' exception: %s" % (
                        last_exception.__class__.__name__, last_exception))
                raise last_exception

            self[key] = val
        elif key in self._dicts:
            self[key] = LazyLoadMetadata(self._url + key + '/',
                                         self._num_retries)

        return super(LazyLoadMetadata, self).__getitem__(key)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def values(self):
        self._materialize()
        return super(LazyLoadMetadata, self).values()

    def items(self):
        self._materialize()
        return super(LazyLoadMetadata, self).items()

    def __str__(self):
        self._materialize()
        return super(LazyLoadMetadata, self).__str__()

    def __repr__(self):
        self._materialize()
        return super(LazyLoadMetadata, self).__repr__()


def _get_instance_metadata(url, num_retries, timeout=None):
    return LazyLoadMetadata(url, num_retries, timeout)