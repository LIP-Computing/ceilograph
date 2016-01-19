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

cfg.CONF.import_opt('udp_port', 'ceilometer.collector', group='collector')
cfg.CONF.import_opt('os_username',
                    'ceilometer.service',
                    group='service_credentials')
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
        self.prefix_account = "accounting.cloud." + self.hostname
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
            raise ValueError('%s: invalid protocol' %
                                (cfg.CONF.graphite.protocol))

        # For testing purposes we will not publish to graphite
        #graphiteSock.connect((self.host, self.port))
        #graphiteSock.sendall(metric)
        #graphiteSock.close()

    def publish_samples(self, context, samples):
        acct_list = []
        for sample in samples:
            stats_time = time.time()
            msg = sample.as_dict()
            prefix = self.prefix

            # for getting a clear idea of the attributes
            resource_id = msg['resource_id']
            project_id = msg['project_id']
            user_id = msg['user_id']
            data_type = msg['type']  # gauge, cumulative, delta
            volume = msg['volume']  # usage
            metric_name = msg['name']  # network,instance,cpu, disk etc ..
            metadata = msg['resource_metadata']
            instance_match = re.match('instance', metric_name)
            network_match = re.match('network', metric_name)
            disk_match = re.match('disk', metric_name)
            mem_match = re.match('memory', metric_name)
            user_name = self._get_user_name(user_id)
            project_name = self._get_project_name(project_id)

            LOG.debug('---> PROJECTName: %s' % project_name)
            LOG.debug('---> USERName: %s' % user_name)

            # ram,cpu, and disk is not present on all metrics
            if disk_match:
                ram = metadata['memory_mb']
                vcpus = metadata['vcpus']
                disk_gb = metadata['disk_gb']
                vmid = metadata.get('instance_id')
            if network_match:
                vmid = metadata.get('instance_id')
                vm = vmid
            else:
                vm = resource_id

            if disk_match:
                vmram = "%s%s.%s.memory %s %d\n" % (
                    prefix, project_id, vm, ram, stats_time)
                vmcpu = "%s%s.%s.cpu_count %s %d\n" % (
                    prefix, project_id, vm, vcpus, stats_time)
                vmdisk = "%s%s.%s.disk_space %s %d\n" % (
                    prefix, project_id, vm, disk_gb, stats_time)

                LOG.debug(_("[+] Graphite %s" % vmram))
                LOG.debug(_("[+] Graphite %s" % vmcpu))
                LOG.debug(_("[+] Graphite %s" % vmdisk))

            # build the metrics to send to graphite
            # We are only interested in the gauge data
            # Skipping Instance related metrics
            if data_type == 'gauge' and instance_match is None:
                stats_time = time.time()
                vmstat = "%s%s.%s.%s %s %d\n" % (
                    prefix, project_id, vm, metric_name, volume, stats_time)
                self.graphitePush(vmstat)
                if disk_match:
                    self.graphitePush(vmram)
                    self.graphitePush(vmcpu)
                    self.graphitePush(vmdisk)
                print ""
            else:
                LOG.debug(_("[-]"))

            # Publish accounting to graphite
            LOG.debug('---> MetricName: %s' % metric_name)
            LOG.debug('---> MetricName MATCHmem: %s' % mem_match)
            if metric_name == 'cpu_util' or mem_match:
                acct = self.prefix_account + '.' + project_id + '.' \
                    + user_id + '.' + metric_name + ' ' + str(volume) \
                    + ' ' + str(stats_time) + '\n'
                acct_list.append(acct)
                LOG.debug('---> ACCT: %s' % acct)

            try:
                LOG.debug(_("OK"))
            except Exception as e:
                LOG.warn(_("Unable to send to Graphite"))
                LOG.exception(e)
        LOG.debug('---> LISTACCT: %s' % acct_list)

    def publish_events(self, context, events):
        """Send an event message for publishing

        :param context: Execution context from the service or RPC call
        :param events: events from pipeline after transformation
        """
        raise ceilometer.NotImplementedError

    def _get_project_name(self, project_id):
        """Get project name from the project ID
        :param project_id: project ID
        """
        proj_name = self.ks.projects.get(project_id)
        return proj_name

    def _get_user_name(self, user_id):
        """Get user name from the user ID
        :param user_id: user ID
        """
        user_name = self.ks.users.get(user_id)
        return user_name

    def _get_keystone(self):
        username = cfg.CONF.service_credentials.os_username
        password = cfg.CONF.keystone_authtoken.password
        project_name = cfg.CONF.keystone_authtoken.project_name
        auth_url = cfg.CONF.keystone_authtoken.auth_url
        auth_version = cfg.CONF.keystone_authtoken.auth_version
        url = auth_url + '/' + auth_version
        auth = v3.Password(auth_url=url,
                           username=username,
                           password=password,
                           project_name=project_name)
        sess = kssession.Session(auth=auth)
        return ksclient.Client(session=sess)
