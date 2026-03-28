#!/usr/bin/python3 -u

from pathlib import Path

import hvac
import hvac.exceptions
import os
import requests_unixsocket
import urllib.parse
import sys
import persistent_queue

runtime_dir = Path('/opt/vault-certbot/runtime')
runtime_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

tasks_dir = runtime_dir / 'tasks'
tasks_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
tasks = persistent_queue.Queue(path=tasks_dir, maxsize=65536)

try:
    import tomllib  # https://docs.python.org/3/library/tomllib.html
except ModuleNotFoundError:
    try:
        import tomli as tomllib  # https://pypi.org/project/tomli/
    except ModuleNotFoundError:
        raise SystemExit(
            "\nTOML [ https://toml.io/ ] parser not found.\n"
            "\nInstall it with:\n"
            "\n    python3 -m pip install tomli\n"
        )

debug = False

configuration = None
configuration_filename = Path("/opt/vault-certbot/vault-certbot.toml")
configuration_defaults = {'vault-proxy-socket': '/run/vault-proxy-infraops.sock', 'vault-mount-point': 'certificate'}

def read_configuration():
    global configuration
    if not configuration_filename.is_file():
        configuration = configuration_defaults
    else:
        configuration = tomllib.loads(configuration_filename.read_text())

read_configuration()



class Vault:
    configuration_required = ('vault-proxy-socket', 'vault-mount-point')

    def __init__(self):
        for key in self.configuration_required:
            if key not in configuration:
                raise RuntimeError(f'required key {key} not found in configuration')
        self.connect_to_vault()

    def connect_to_vault(self):
        try:
            encoded = urllib.parse.quote(configuration['vault-proxy-socket'], safe="")
            url = f"http+unix://{encoded}"
            session = requests_unixsocket.Session()
            session.headers.update({"X-Vault-Request": "true"})
            self.client = hvac.Client(url=url, token="", session=session)
        except hvac.exceptions.VaultError as exc:
            raise RuntimeError("Vault request failed") from exc
        try:
            self.client.lookup_token()  # calls /v1/auth/token/lookup-self
        except hvac.exceptions.VaultError as exc:
            raise RuntimeError("Vault client is not authenticated") from exc

    def put_certificate(self, cert_name, data):
        assert isinstance(data, dict)
        mount_point=configuration['vault-mount-point']
        path = f'live/{cert_name}'
        secret = data
        self.client.secrets.kv.v1.create_or_update_secret(mount_point=mount_point, path=path, secret=secret)

    def get_certificate(self, cert_name):
        mount_point=configuration['vault-mount-point']
        path = f'live/{cert_name}'
        secret = self.client.secrets.kv.v1.read_secret(path=path, mount_point=mount_point)
        fullchain_pem = secret['data']['fullchain_pem']
        privkey_pem   = secret['data']['privkey_pem']
        return dict(fullchain_pem=fullchain_pem, privkey_pem=privkey_pem)

    def get_certificate_servers(self, cert_name):
        mount_point=configuration['vault-mount-point']
        path = f'config/{cert_name}'
        try:
            secret = self.client.secrets.kv.v1.read_secret(path=path, mount_point=mount_point)
            servers = secret['data']['servers']
            assert isinstance(servers, list)
            return servers
        except hvac.exceptions.VaultError as exc:
            return list()

if __name__ == '__main__':
    from pprint import pprint
    vault = Vault()
    pprint(vault.client.secrets.kv.v1.list_secrets(mount_point=configuration['vault-mount-point'], path='config')['data']['keys'])
    pprint(vault.client.secrets.kv.v1.list_secrets(mount_point=configuration['vault-mount-point'], path='live')['data']['keys'])
