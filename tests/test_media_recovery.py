import asyncio
import hashlib
import os
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from max_account.config import Settings
from max_account.media import MediaManager
from max_account.state import StateStore


# Все файлы и ответы создаются в изолированном каталоге без обращения к MAX.
def make_media(root):
    settings = Settings(session_dir=root / 'session', storage_dir=root / 'spool',
                        download_dir=root / 'inbox', input_roots=(root / 'input',),
                        token_file=root / 'key', state_db=root / 'state.db')
    state = StateStore(settings.state_db)
    media = MediaManager(settings, state, SimpleNamespace())
    return media, state


# Два имени одного файла после прерывания не расходуют квоту дважды.
def test_quota_counts_hardlinks_once(tmp_path):
    media, _ = make_media(tmp_path)
    target = media.config.download_dir / 'synthetic'
    part = media.config.download_dir / 'synthetic.part'
    target.write_bytes(b'content')
    os.link(target, part)
    assert media._used() == len(b'content')


def test_download_failure_after_ready_commit_keeps_registered_file(tmp_path, monkeypatch):
    media, state = make_media(tmp_path)
    media.account.resolve_attachment = lambda *args: answer()
    class Content:
        chunks = iter((b'content', b''))
        async def read(self, size):
            return next(self.chunks)
    class Response:
        status = 200
        content_length = 7
        content = Content()
        def release(self):
            pass
    class Session:
        async def close(self):
            pass
    async def answer():
        return {'url': 'https://i.oneme.ru/file', 'filename': 'file'}
    async def request(*args):
        return Session(), Response()
    media._request = request
    original = state.finish_staged_asset
    def commit_then_error(file_id, asset):
        original(file_id, asset)
        raise OSError('ошибка после записи')
    monkeypatch.setattr(state, 'finish_staged_asset', commit_then_error)

    async def scenario():
        transfer = await media.start_download('key', '1', '2', '0')
        await asyncio.gather(*media._tasks)
        status = await media.get_transfer_status(transfer['transfer_id'])
        assert status['status'] == 'failed'
        files = list(media.config.download_dir.iterdir())
        assert len(files) == 1 and files[0].read_bytes() == b'content'
        assert state.get_asset(file_id=files[0].name)['status'] == 'ready'
    asyncio.run(scenario())


# Восстановление использует настоящие защищённые дескрипторы Linux, не имитацию Windows.
@pytest.mark.skipif(os.name != 'posix', reason='Нужны файловые дескрипторы и права POSIX')
@pytest.mark.parametrize('point', ['before_link', 'after_link', 'after_unlink', 'replaced', 'missing'])
def test_recover_recorded_file_at_each_interruption(tmp_path, point):
    media, state = make_media(tmp_path)
    file_id = uuid.uuid4().hex
    part = media.config.download_dir / (file_id + '.part')
    target = media.config.download_dir / file_id
    part.write_bytes(b'protected')
    part.chmod(0o640)
    info = part.stat()
    asset = {'file_id': file_id, 'path': str(target), 'kind': 'document', 'filename': 'x',
             'size': info.st_size, 'sha256': hashlib.sha256(b'protected').hexdigest(),
             'status': 'staging', 'created_at': time.time(), 'device': info.st_dev, 'inode': info.st_ino,
             'mtime_ns': info.st_mtime_ns, 'ctime_ns': info.st_ctime_ns}
    state.save_asset(asset)
    if point in ('after_link', 'after_unlink', 'replaced'):
        os.link(part, target)
    if point in ('after_unlink', 'replaced'):
        part.unlink()
    if point == 'replaced':
        target.unlink()
        target.write_bytes(b'foreign')
    if point == 'missing':
        part.unlink()
    result = asyncio.run(media.recover_staged_assets())
    if point == 'replaced':
        assert target.read_bytes() == b'foreign'
        assert state.get_asset(file_id=file_id) is None
        assert len(state.get_staged_assets()) == 1
        assert result['preserved'] == 1
    elif point == 'missing':
        assert not state.get_staged_assets()
        assert result['missing'] == 1
    else:
        assert target.read_bytes() == b'protected'
        assert not part.exists()
        assert state.get_asset(file_id=file_id)['status'] == 'ready'
        assert not state.get_staged_assets()

# Ошибка записи до фиксации и после неё должна приводить к разным безопасным исходам.
@pytest.mark.parametrize('committed', [False, True])
def test_staging_commit_failure_preserves_only_recorded_part(tmp_path, monkeypatch, committed):
    media, state = make_media(tmp_path)
    class Content:
        def __init__(self):
            self.chunks = iter((b'content', b''))
        async def read(self, size):
            return next(self.chunks)
    class Response:
        status = 200
        content_length = 7
        def __init__(self):
            self.content = Content()
        def release(self):
            pass
    class Session:
        async def close(self):
            pass
    async def answer(*args):
        return {'url': 'https://i.oneme.ru/file', 'filename': 'file'}
    async def request(*args):
        return Session(), Response()
    media.account.resolve_attachment = answer
    media._request = request
    original = state.save_asset
    def fail_save(asset):
        if committed:
            original(asset)
        raise OSError('ошибка записи')
    monkeypatch.setattr(state, 'save_asset', fail_save)

    async def scenario():
        transfer = await media.start_download('key', '1', '2', '0')
        await asyncio.gather(*media._tasks)
        assert (await media.get_transfer_status(transfer['transfer_id']))['status'] == 'failed'
        files = list(media.config.download_dir.iterdir())
        records = state.get_staged_assets()
        if committed:
            assert len(records) == len(files) == 1
            assert files[0].name == records[0]['file_id'] + '.part'
            assert files[0].read_bytes() == b'content'
            assert media._used() == len(b'content')
            assert state.get_asset(file_id=records[0]['file_id']) is None
        else:
            assert records == [] and files == []
    asyncio.run(scenario())


# Отмена непосредственно после создания конечного имени сохраняет запись и оба имени.
def test_cancel_after_link_preserves_recoverable_file(tmp_path, monkeypatch):
    media, state = make_media(tmp_path)
    class Content:
        chunks = iter((b'content', b''))
        async def read(self, size):
            return next(self.chunks)
    class Response:
        status = 200
        content_length = 7
        content = Content()
        def release(self):
            pass
    class Session:
        async def close(self):
            pass
    async def answer(*args):
        return {'url': 'https://i.oneme.ru/file', 'filename': 'file'}
    async def request(*args):
        return Session(), Response()
    media.account.resolve_attachment = answer
    media._request = request
    original = os.link
    def link_then_cancel(*args, **kwargs):
        original(*args, **kwargs)
        raise asyncio.CancelledError()
    monkeypatch.setattr(os, 'link', link_then_cancel)

    async def scenario():
        transfer = await media.start_download('key', '1', '2', '0')
        await asyncio.gather(*media._tasks, return_exceptions=True)
        assert (await media.get_transfer_status(transfer['transfer_id']))['status'] == 'failed'
        records = state.get_staged_assets()
        assert len(records) == 1
        assert state.get_asset(file_id=records[0]['file_id']) is None
        assert (media.config.download_dir / records[0]['file_id']).read_bytes() == b'content'
        assert (media.config.download_dir / (records[0]['file_id'] + '.part')).read_bytes() == b'content'
    asyncio.run(scenario())
