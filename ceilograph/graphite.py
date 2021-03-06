# -*- encoding: utf-8 -*-
#
# Copyright 2014 Sashi Dahal, Robert van Leeuwen
#
# Author: Sashi Dahal <shashi.dahal@spilgames.com>
# Author: Robert van Leeuwen <robert.vanleeuwen@spilgames.com>
# Modified by Mario David <mariojmdavid@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Publish to Graphite
"""

import time
import re
import socket

from oslo_config import cfg
from oslo_config import types
from oslo_utils import netutils
from keystoneclient import session as kssession
from keystoneclient.auth.identity import v3
from keystoneclient.v3 import client as ksclient

import ceilometer
from ceilometer.i18n import _
from ceilometer.openstack.common import log
from ceilometer import publisher

cfg.CONF.import_group('service_credentials', 'ceilometer.service')
cfg.CONF.import_opt('udp_port', 'ceilometer.collector', group='collector')
cfg.CONF.import_group('keystone_authtoken',
                      'keystoneclient.middleware.auth_token')
PortType = types.Integer(1, 65535)
OPTS = [cfg.Opt('default_port',
                default=2003,
                type=PortType,
                help='Default port for communicating with Graphite'),
        cfg.StrOpt('protocol',
                   default='tcp',
                   help='Protocol (tcp or udp) to use for '
                   'communicating with Graphite'),
        cfg.StrOpt('os_user_id',
                   help='Ceilometer user ID'),
        cfg.StrOpt('os_tenant_id',
                   help='Ceilometer project ID'),
        cfg.StrOpt('prefix',
                   default='ceilometer.',
                   help='Graphite prefix key'),
        cfg.BoolOpt('hypervisor_in_prefix',
                    default=True,
                    help='If the hypervisor should be added to the prefix'),
        ]
cfg.CONF.register_opts(OPTS, group="graphite")
LOG = log.getLogger(__name__)


class GraphitePublisher(publisher.PublisherBase):
    def __init__(self, parsed_url):
        self.host, self.port = netutils.parse_host_port(
            parsed_url.netloc,
            default_port=cfg.CONF.graphite.default_port)
        self.hostname = socket.gethostname().split('.')[0]
        self.prefix_account = "testacct2.cloud." + self.hostname
        self.ks = self._get_keystone()
        if cfg.CONF.graphite.hypervisor_in_prefix:
            self.prefix = (cfg.CONF.graphite.prefix + self.hostname + ".")
        else:
            self.prefix = cfg.CONF.graphite.prefix

    def graphitePush(self, metric):
        LOG.debug("Sending graphite metric: " + metric)
        if cfg.CONF.graphite.protocol.lower() == 'tcp':
            graphiteSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        elif cfg.CONF.graphite.protocol.lower() == 'udp':
            graphiteSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        else:
            raise ValueError('%s: invalid prot' % (cfg.CONF.graphite.protocol))
        graphiteSock.connect((self.host, self.port))
        graphiteSock.sendall(metric)
        graphiteSock.close()

    def publish_samples(self, context, samples):
        acct_list = []
        for sample in samples:
            stats_time = str(time.time())
            msg = sample.as_dict()
            prefix = self.prefix

            resource_id = msg['resource_id']
            project_id = msg['project_id']
            user_id = msg['user_id']
            data_type = msg['type']
            value = str(msg['volume'])
            metric_name = msg['name']
            metad = msg['resource_metadata']
            user_name = self._get_user_name(user_id).replace('.', '_')
            project_name = self._get_project_name(project_id)
            lmtr = ['instance', 'memory', 'disk', 'cpu']
            graph_tail = metric_name + ' ' + value + ' ' + stats_time
            graph_hd1 = self.prefix_account + '.' + project_name + '.' \
                            + user_name + '.' + resource_id + '.'
            acct_list.append(graph_hd1+graph_tail)

            LOG.debug('---> PROJECT Name: %s' % project_name)
            LOG.debug('---> USER Name: %s' % user_name)
            LOG.debug('---> METRIC Name: %s  Value: %s' % (metric_name, value))
            LOG.debug('---> TimeST: %s' % stats_time)
            if any(i in metric_name for i in lmtr):
                icpu = str(metad['vcpus'])
                imem = str(metad['memory_mb'])
                idsk = str(metad['disk_gb'])
                graph_tail1 = 'vcpus' + ' ' + icpu + ' ' + stats_time
                graph_tail2 = 'memory_mb' + ' ' + imem + ' ' + stats_time
                graph_tail3 = 'disk_gb' + ' ' + idsk + ' ' + stats_time
                graph_hd1 = self.prefix_account + '.' + project_name + '.' \
                            + user_name + '.' + resource_id + '.'
                acct_list.append(graph_hd1+graph_tail1)
                acct_list.append(graph_hd1+graph_tail2)
                acct_list.append(graph_hd1+graph_tail3)
                LOG.debug('---> subMET Name: %s  Value: %s' % ('vcpus', icpu))
                LOG.debug('---> subMET Name: %s  Value: %s' % ('mem', imem))
                LOG.debug('---> subMET Name: %s  Value: %s' % ('disk', idsk))

        #LOG.debug('OOOOO> ALLList: %s' % acct_list)
        graph_string = '\n'.join(acct_list) + '\n'
        LOG.debug('OOOOO> GRAPHSTRING: %s' % graph_string)
        self.graphitePush(graph_string)

    def publish_events(self, context, events):
        '''Send an event message for publishing

        :param context: Execution context from the service or RPC call
        :param events: events from pipeline after transformation
        '''
        raise ceilometer.NotImplementedError

    def _get_project_name(self, project_id):
        '''Get project name from the project ID
        :param project_id: project ID
        '''
        proj_name = self.ks.projects.get(project_id)
        return proj_name.name

    def _get_user_name(self, user_id):
        '''Get user name from the user ID
        :param user_id: user ID
        '''
        user_name = self.ks.users.get(user_id)
        return user_name.name

    def _get_keystone(self):
        '''Get keystone client session
        '''
        user_id = cfg.CONF.graphite.os_user_id
        password = cfg.CONF.service_credentials.os_password
        project_id = cfg.CONF.graphite.os_tenant_id
        auth_uri = cfg.CONF.keystone_authtoken.auth_uri
        auth_version = cfg.CONF.keystone_authtoken.auth_version
        url = auth_uri + '/' + auth_version
        auth = v3.Password(auth_url=url,
                           user_id=user_id,
                           password=password,
                           project_id=project_id)
        sess = kssession.Session(auth=auth)
        return ksclient.Client(session=sess)
