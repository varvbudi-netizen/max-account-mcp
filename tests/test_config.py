"""Проверки реальной границы между локальными пользователями и секретом МСП."""

import os
from pathlib import Path
import tempfile
import unittest

from max_account.cli import initialize
from max_account.config import read_service_token
from max_account.errors import BridgeError
from test_app import settings_at


# На Linux доступный постороннему ключ отклоняется до запуска HTTP-службы.
@unittest.skipIf(os.name == "nt", "Права POSIX проверяются на целевом Linux")
class TokenTests(unittest.TestCase):
    def test_other_user_permissions_symlink_and_existing_init(self):
        with tempfile.TemporaryDirectory() as folder:
            settings = settings_at(Path(folder))
            self.assertEqual(len(read_service_token(settings)), 48)
            settings.token_file.chmod(0o644)
            with self.assertRaises(BridgeError):
                read_service_token(settings)
            with self.assertRaises(BridgeError):
                initialize(settings)
            settings.token_file.chmod(0o600)
            target = settings.token_file.with_name("actual.key")
            settings.token_file.rename(target)
            settings.token_file.symlink_to(target)
            with self.assertRaises(BridgeError):
                read_service_token(settings)
