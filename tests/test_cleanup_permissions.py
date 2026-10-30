"""Реальная проверка запрета подмены файла участником общей группы на Linux."""

import os
from pathlib import Path
import tempfile
import unittest


# Другой пользователь той же группы не должен заменить файл службы перед его удалением.
@unittest.skipUnless(os.name == "posix" and hasattr(os, "fork") and os.geteuid() == 0,
                     "Проверка второго пользователя выполняется в Linux-сборке от администратора")
class CleanupPermissionTests(unittest.TestCase):
    def test_peer_group_cannot_replace_service_file_in_sticky_directory(self):
        import pwd
        peer_uid = pwd.getpwnam("nobody").pw_uid
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            root.chmod(0o1770)
            inbox = root / "inbox"
            inbox.mkdir(mode=0o770)
            target = inbox / "owned-by-service"

            def attempt_replace():
                child = os.fork()
                if child == 0:
                    try:
                        os.setgroups([os.getegid()])
                        os.setuid(peer_uid)
                        own = inbox / "owned-by-peer"
                        own.write_bytes(b"peer")
                        os.replace(own, target)
                    except PermissionError:
                        os._exit(1)
                    except BaseException:
                        os._exit(2)
                    os._exit(0)
                _, status = os.waitpid(child, 0)
                return os.waitstatus_to_exitcode(status)

            # Контроль доказывает, что участник группы действительно имеет доступ к этому каталогу.
            inbox.chmod(0o770)
            target.write_bytes(b"service")
            target.chmod(0o640)
            self.assertEqual(attempt_replace(), 0)
            self.assertEqual(target.read_bytes(), b"peer")
            target.unlink()

            # Защитный бит закрывает именно замену чужого имени, сохраняя создание своих файлов.
            target.write_bytes(b"service")
            target.chmod(0o640)
            inbox.chmod(0o1770)
            self.assertEqual(attempt_replace(), 1)
            self.assertEqual(target.read_bytes(), b"service")
