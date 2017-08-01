# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import socket
from airflow.hooks.base_hook import BaseHook
from smb.SMBConnection import SMBConnection


class SambaHook(BaseHook):
    """
    Allows native python interaction with a samba server.
    """

    def __init__(self, conn_id):
        self.conn = self.get_connection(conn_id)

    def get_conn(self):
        """Initaites connection to a samba server

        Returns: SMBConnection
        """
        extras = self.conn.extra_dejson.items()

        samba = SMBConnection(
            self.conn.login,
            self.conn.password,
            extras.get("client", socket.gethostname()),  # arbitrary
            extras.get("serverName", self.conn.host),  # name of server
            domain=extras.get("domain", ""),
            use_ntlm_v2=extras.get("use_ntlm_v2"))

        # WARNING - idle connections will timeout
        samba.connect(self.conn.host)
        return samba
