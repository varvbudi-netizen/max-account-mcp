"""Проверка установленной службы через настоящий протокол без отправки сообщений MAX."""

import asyncio
import json
from pathlib import Path

import httpx2
from mcp import Client
from mcp.client.streamable_http import streamable_http_client


# Секрет читается внутри процесса, в аргументы командной строки и вывод не попадает.
async def main():
    token = Path("/etc/max-account/mcp.key").read_text(encoding="ascii").strip()
    endpoint = "http://127.0.0.1:9381"
    async with httpx2.AsyncClient(trust_env=False, timeout=httpx2.Timeout(10, read=30)) as anonymous:
        # systemd запускает процесс раньше, чем приложение успевает открыть порт.
        async with asyncio.timeout(15):
            while True:
                try:
                    response = await anonymous.get(endpoint + "/health")
                    break
                except httpx2.ConnectError:
                    await asyncio.sleep(0.25)
        assert response.status_code == 401
    async with httpx2.AsyncClient(
        headers={"Authorization": "Bearer " + token}, trust_env=False,
        timeout=httpx2.Timeout(10, read=30),
    ) as http:
        for mode in ("auto", "legacy"):
            async with Client(streamable_http_client(endpoint + "/mcp", http_client=http), mode=mode) as client:
                tools = await client.list_tools()
                assert len(tools.tools) == 18
                result = await client.call_tool("account_status")
                status = result.structured_content or json.loads(result.content[0].text)
                assert status["ok"]
                # Проверяем транспорт как до первого входа, так и после сохранения сеанса.
                assert status["state"] in ("AUTH_REQUIRED", "ready")
                assert status["storage_cleanup"]["error"] is None
                print(json.dumps({"mode": mode, "protocol": client.protocol_version,
                                  "tools": len(tools.tools), "account_state": status["state"],
                                  "version": status["version"], "storage_cleanup": status["storage_cleanup"]}))


asyncio.run(main())
