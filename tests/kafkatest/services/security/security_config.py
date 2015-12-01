# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import subprocess
from ducktape.template import TemplateRenderer
from kafkatest.utils.remote_account import scp
from kafkatest.services.security.minikdc import MiniKdc

class Keytool(object):

    @staticmethod
    def generate_keystore_truststore(ssl_dir='.'):
        """
        Generate JKS keystore and truststore and return
        Kafka SSL properties with these stores.
        """
        ks_path = os.path.join(ssl_dir, 'test.keystore.jks')
        ks_password = 'test-ks-passwd'
        key_password = 'test-key-passwd'
        ts_path = os.path.join(ssl_dir, 'test.truststore.jks')
        ts_password = 'test-ts-passwd'
        if os.path.exists(ks_path):
            os.remove(ks_path)
        if os.path.exists(ts_path):
            os.remove(ts_path)
        
        Keytool.runcmd("keytool -genkeypair -alias test -keyalg RSA -keysize 2048 -keystore %s -storetype JKS -keypass %s -storepass %s -dname CN=systemtest" % (ks_path, key_password, ks_password))
        Keytool.runcmd("keytool -export -alias test -keystore %s -storepass %s -storetype JKS -rfc -file test.crt" % (ks_path, ks_password))
        Keytool.runcmd("keytool -import -alias test -file test.crt -keystore %s -storepass %s -storetype JKS -noprompt" % (ts_path, ts_password))
        os.remove('test.crt')

        return {
            'ssl.keystore.location' : ks_path,
            'ssl.keystore.password' : ks_password,
            'ssl.key.password' : key_password,
            'ssl.truststore.location' : ts_path,
            'ssl.truststore.password' : ts_password
        }

    @staticmethod
    def runcmd(cmd):
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        proc.communicate()
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd)


class SecurityConfig(TemplateRenderer):

    PLAINTEXT = 'PLAINTEXT'
    SSL = 'SSL'
    SASL_PLAINTEXT = 'SASL_PLAINTEXT'
    SASL_SSL = 'SASL_SSL'
    SASL_MECHANISM_GSSAPI = 'GSSAPI'
    SASL_MECHANISM_PLAIN = 'PLAIN'
    CONFIG_DIR = "/mnt/security"
    KEYSTORE_PATH = "/mnt/security/test.keystore.jks"
    TRUSTSTORE_PATH = "/mnt/security/test.truststore.jks"
    JAAS_CONF_PATH = "/mnt/security/jaas.conf"
    KRB5CONF_PATH = "/mnt/security/krb5.conf"
    KEYTAB_PATH = "/mnt/security/keytab"

    ssl_stores = Keytool.generate_keystore_truststore('.')

    def __init__(self, security_protocol, interbroker_security_protocol=None, sasl_mechanism=SASL_MECHANISM_GSSAPI, template_props=""):
        """
        Initialize the security properties for the node and copy
        keystore and truststore to the remote node if the transport protocol 
        is SSL. If security_protocol is None, the protocol specified in the
        template properties file is used. If no protocol is specified in the
        template properties either, PLAINTEXT is used as default.
        """

        if security_protocol is None:
            security_protocol = self.get_property('security.protocol', template_props)
        if security_protocol is None:
            security_protocol = SecurityConfig.PLAINTEXT
        elif security_protocol not in [SecurityConfig.PLAINTEXT, SecurityConfig.SSL, SecurityConfig.SASL_PLAINTEXT, SecurityConfig.SASL_SSL]:
            raise Exception("Invalid security.protocol in template properties: " + security_protocol)

        if interbroker_security_protocol is None:
            interbroker_security_protocol = security_protocol
        self.interbroker_security_protocol = interbroker_security_protocol
        self.has_sasl = self.is_sasl(security_protocol) or self.is_sasl(interbroker_security_protocol)
        self.has_ssl = self.is_ssl(security_protocol) or self.is_ssl(interbroker_security_protocol)
        self.properties = {
            'security.protocol' : security_protocol,
            'ssl.keystore.location' : SecurityConfig.KEYSTORE_PATH,
            'ssl.keystore.password' : SecurityConfig.ssl_stores['ssl.keystore.password'],
            'ssl.key.password' : SecurityConfig.ssl_stores['ssl.key.password'],
            'ssl.truststore.location' : SecurityConfig.TRUSTSTORE_PATH,
            'ssl.truststore.password' : SecurityConfig.ssl_stores['ssl.truststore.password'],
            'sasl.mechanism' : sasl_mechanism,
            'sasl.kerberos.service.name' : 'kafka'
        }


    def client_config(self, template_props=""):
        return SecurityConfig(self.security_protocol, sasl_mechanism=self.sasl_mechanism, template_props=template_props)

    def setup_node(self, node, miniKdc=None):
        if self.has_ssl:
            node.account.ssh("mkdir -p %s" % SecurityConfig.CONFIG_DIR, allow_fail=False)
            node.account.scp_to(SecurityConfig.ssl_stores['ssl.keystore.location'], SecurityConfig.KEYSTORE_PATH)
            node.account.scp_to(SecurityConfig.ssl_stores['ssl.truststore.location'], SecurityConfig.TRUSTSTORE_PATH)

        if self.has_sasl:
            node.account.ssh("mkdir -p %s" % SecurityConfig.CONFIG_DIR, allow_fail=False)
            jaas_conf_file = self.sasl_mechanism.lower() + "_jaas.conf"
            java_version = node.account.ssh_capture("java -version")
            if any('IBM' in line for line in java_version):
                is_ibm_jdk = True
            else:
                is_ibm_jdk = False
            jaas_conf = self.render(jaas_conf_file,  node=node, is_ibm_jdk=is_ibm_jdk)
            node.account.create_file(SecurityConfig.JAAS_CONF_PATH, jaas_conf)
            if self.has_sasl_kerberos:
                if miniKdc is None:
                    raise Exception("miniKdc must be not None when has_sasl_kerberos is true")
                scp(miniKdc.nodes[0], MiniKdc.KEYTAB_FILE, node, SecurityConfig.KEYTAB_PATH)
                #KDC is set to bind openly (via 0.0.0.0). Change krb5.conf to hold the specific KDC address
                scp(miniKdc.nodes[0], MiniKdc.KRB5CONF_FILE, node, SecurityConfig.KRB5CONF_PATH, '0.0.0.0', miniKdc.nodes[0].account.hostname)

    def clean_node(self, node):
        if self.security_protocol != SecurityConfig.PLAINTEXT:
            node.account.ssh("rm -rf %s" % SecurityConfig.CONFIG_DIR, allow_fail=False)

    def get_property(self, prop_name, template_props=""):
        """
        Get property value from the string representation of
        a properties file.
        """
        value = None
        for line in template_props.split("\n"):
            items = line.split("=")
            if len(items) == 2 and items[0].strip() == prop_name:
                value = str(items[1].strip())
        return value

    def is_ssl(self, security_protocol):
        return security_protocol == SecurityConfig.SSL or security_protocol == SecurityConfig.SASL_SSL

    def is_sasl(self, security_protocol):
        return security_protocol == SecurityConfig.SASL_PLAINTEXT or security_protocol == SecurityConfig.SASL_SSL

    @property
    def security_protocol(self):
        return self.properties['security.protocol']

    @property
    def sasl_mechanism(self):
        return self.properties['sasl.mechanism']

    @property
    def has_sasl_kerberos(self):
        return self.has_sasl and self.sasl_mechanism == SecurityConfig.SASL_MECHANISM_GSSAPI

    @property
    def kafka_opts(self):
        if self.has_sasl:
            return "\"-Djava.security.auth.login.config=%s -Djava.security.krb5.conf=%s\"" % (SecurityConfig.JAAS_CONF_PATH, SecurityConfig.KRB5CONF_PATH)
        else:
            return ""

    def __str__(self):
        """
        Return properties as string with line separators.
        This is used to append security config properties to
        a properties file.
        """

        prop_str = ""
        if self.security_protocol != SecurityConfig.PLAINTEXT:
            for key, value in self.properties.items():
                prop_str += ("\n" + key + "=" + value)
            prop_str += "\n"
        return prop_str

