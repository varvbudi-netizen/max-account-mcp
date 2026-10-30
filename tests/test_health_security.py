import asyncio
from pathlib import Path
from types import SimpleNamespace

from max_account.app import create_app
from max_account.config import Settings, prepare_directories


# Весь маршрут состояния проверяется через настоящий вход приложения и вымышленный ключ.
def test_health_checks_key_host_and_origin(tmp_path):
    settings = Settings(
        session_dir=tmp_path / 'private/session', storage_dir=tmp_path / 'private/spool',
        download_dir=tmp_path / 'inbox', input_roots=(tmp_path / 'outbox',),
        token_file=tmp_path / 'private/key', state_db=tmp_path / 'private/state.db',
    )
    prepare_directories(settings)
    settings.token_file.write_text('a' * 48, encoding='ascii')
    settings.token_file.chmod(0o600)
    app = create_app(settings)
    app.runtime.account = SimpleNamespace(status=lambda: {'connected': True})

    async def request(host, origin=None, key='a' * 48):
        events = []
        headers = [(b'host', host.encode()), (b'authorization', ('Bearer ' + key).encode())]
        if origin is not None:
            headers.append((b'origin', origin.encode()))
        scope = {'type': 'http', 'asgi': {'version': '3.0'}, 'http_version': '1.1',
                 'method': 'GET', 'scheme': 'http', 'path': '/health', 'raw_path': b'/health',
                 'query_string': b'', 'headers': headers, 'server': ('127.0.0.1', 9381),
                 'client': ('127.0.0.1', 12345)}
        async def receive():
            return {'type': 'http.request', 'body': b'', 'more_body': False}
        async def send(event):
            events.append(event)
        await app(scope, receive, send)
        return events[0]['status']

    async def scenario():
        assert await request('127.0.0.1:9381') == 200
        assert await request('127.0.0.1:9381', 'http://localhost:9381') == 200
        assert await request('127.0.0.1:9381', key='b' * 48) == 401
        assert await request('evil.example') == 421
        assert await request('127.0.0.1:9381', 'http://evil.example') == 403
    asyncio.run(scenario())
