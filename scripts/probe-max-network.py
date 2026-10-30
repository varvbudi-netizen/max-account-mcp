"""Проверка сетевого пути к MAX без сеанса и передачи данных аккаунта."""

import asyncio
import json
import logging
import ssl
from urllib.parse import urlsplit

import aiohttp
from pymax.config import ExtraConfig
from websockets.asyncio.client import connect
from websockets.exceptions import InvalidHandshake


# Сетевые библиотеки не должны печатать заголовки или подробности ошибок.
for name in ("pymax", "websockets", "aiohttp"):
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False


def error_code(error):
    """Преобразует исключение в безопасную категорию без сетевых адресов."""
    if isinstance(error, (asyncio.TimeoutError, TimeoutError)):
        return "TIMEOUT"
    if isinstance(error, (ssl.SSLError, aiohttp.ClientSSLError)):
        return "TLS_ERROR"
    if isinstance(error, InvalidHandshake):
        return "WEBSOCKET_REJECTED"
    if isinstance(error, (OSError, aiohttp.ClientError)):
        return "NETWORK_ERROR"
    return "PROBE_ERROR"


# Все три проверки не используют прокси и не отправляют авторизацию.
async def probe():
    endpoint = ExtraConfig().url
    parsed = urlsplit(endpoint)
    if parsed.scheme != "wss" or not parsed.hostname or parsed.username or parsed.password:
        return {"host": None, "tls": "CONFIG_ERROR", "https": "CONFIG_ERROR", "websocket": "CONFIG_ERROR"}
    host = parsed.hostname
    port = parsed.port or 443
    result = {"host": host}
    tls = ssl.create_default_context()

    # Проверяем сертификат и установку TLS отдельно от протокола HTTP.
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=tls, server_hostname=host), timeout=10,
        )
        writer.close()
        await writer.wait_closed()
        result["tls"] = "OK"
    except Exception as error:
        result["tls"] = error_code(error)

    # Любой ответ HTTP подтверждает прохождение TLS и обмен без авторизации.
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as session:
            async with session.get(f"https://{host}:{port}/", ssl=tls, allow_redirects=False):
                result["https"] = "OK"
    except Exception as error:
        result["https"] = error_code(error)

    # Выполняем только стандартное открытие WebSocket и сразу закрываем соединение.
    try:
        async with connect(
            endpoint, origin="https://web.max.ru", proxy=None,
            ssl=tls, open_timeout=10, close_timeout=2,
        ):
            result["websocket"] = "OK"
    except Exception as error:
        result["websocket"] = error_code(error)
    return result


if __name__ == "__main__":
    result = asyncio.run(probe())
    print(json.dumps(result, ensure_ascii=False))
    raise SystemExit(0 if all(result[name] == "OK" for name in ("tls", "https", "websocket")) else 1)
