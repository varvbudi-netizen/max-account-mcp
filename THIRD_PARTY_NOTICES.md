# Сторонние компоненты и лицензии

Код моста распространяется по [MIT](LICENSE). Сторонние библиотеки остаются под собственными лицензиями; MIT проекта не заменяет их условия. Библиотеки не включены в репозиторий: установщик получает их как зависимости. Для закреплённого набора сохранены полные лицензионные уведомления, включая вложенные уведомления о стороннем коде.

PyMax (`maxapi-python` 2.5.0) — отдельный проект [MaxApiTeam/PyMax](https://github.com/MaxApiTeam/PyMax/tree/v2.5.0). Его исходное уведомление **Copyright (c) 2025 ink-developer** и полный текст MIT сохранены [без изменения](third_party/maxapi-python/licenses/LICENSE). PyMax не переименован в собственную разработку, его пакет не изменён. Исторические авторы upstream перечислены в его собственном репозитории.

В списке есть также Apache, BSD, PSF и другие условия. При распространении окружения, контейнера или сборки сохраняйте относящиеся к ним полные тексты и уведомления. Данные таблицы взяты из метаданных конкретных закреплённых пакетов; сами тексты приоритетнее краткой метки. Список в машиночитаемом виде: [manifest.json](third_party/manifest.json).

| Пакет | Версия | Лицензия из метаданных | Исходные уведомления |
|---|---|---|---|
| PyJWT | 2.15.1 | MIT | [AUTHORS.rst](third_party/PyJWT/licenses/AUTHORS.rst), [LICENSE](third_party/PyJWT/licenses/LICENSE) |
| aiofiles | 25.1.0 | Apache-2.0 | [LICENSE](third_party/aiofiles/licenses/LICENSE), [NOTICE](third_party/aiofiles/licenses/NOTICE) |
| aiohappyeyeballs | 2.7.1 | PSF-2.0 | [LICENSE](third_party/aiohappyeyeballs/licenses/LICENSE) |
| aiohttp | 3.14.5 | Apache-2.0 AND MIT | [LICENSE.txt](third_party/aiohttp/licenses/LICENSE.txt), [LICENSE](third_party/aiohttp/licenses/vendor/llhttp/LICENSE) |
| aiosignal | 1.4.0 | Apache 2.0 | [LICENSE](third_party/aiosignal/licenses/LICENSE) |
| aiosqlite | 0.22.1 | MIT License | [LICENSE](third_party/aiosqlite/licenses/LICENSE) |
| annotated-types | 0.8.0 | MIT | [LICENSE](third_party/annotated-types/licenses/LICENSE) |
| anyio | 4.15.1 | MIT | [LICENSE](third_party/anyio/licenses/LICENSE) |
| attrs | 26.1.0 | MIT | [LICENSE](third_party/attrs/licenses/LICENSE) |
| cffi | 2.1.1 | MIT-0 | [LICENSE](third_party/cffi/licenses/LICENSE) |
| click | 8.5.0 | BSD-3-Clause | [LICENSE.txt](third_party/click/licenses/LICENSE.txt) |
| colorama | 0.4.6 | BSD License | [LICENSE.txt](third_party/colorama/licenses/LICENSE.txt) |
| cryptography | 50.0.2 | Apache-2.0 OR BSD-3-Clause | [LICENSE](third_party/cryptography/licenses/LICENSE), [LICENSE.APACHE](third_party/cryptography/licenses/LICENSE.APACHE), [LICENSE.BSD](third_party/cryptography/licenses/LICENSE.BSD) |
| frozenlist | 1.8.0 | Apache-2.0 | [LICENSE](third_party/frozenlist/licenses/LICENSE) |
| h11 | 0.16.0 | MIT | [LICENSE.txt](third_party/h11/licenses/LICENSE.txt) |
| httpcore2 | 2.16.0 | BSD-3-Clause | [LICENSE.md](third_party/httpcore2/licenses/LICENSE.md) |
| httpx2 | 2.16.0 | BSD-3-Clause | [LICENSE.md](third_party/httpx2/licenses/LICENSE.md) |
| idna | 3.20 | BSD-3-Clause | [LICENSE.md](third_party/idna/licenses/LICENSE.md) |
| jsonschema-specifications | 2025.9.1 | MIT | [COPYING](third_party/jsonschema-specifications/licenses/COPYING) |
| jsonschema | 4.26.0 | MIT | [COPYING](third_party/jsonschema/licenses/COPYING) |
| maxapi-python | 2.5.0 | MIT | [LICENSE](third_party/maxapi-python/licenses/LICENSE) |
| mcp-types | 2.3.0 | MIT | [LICENSE](third_party/mcp-types/licenses/LICENSE) |
| mcp | 2.3.0 | MIT | [LICENSE](third_party/mcp/licenses/LICENSE) |
| msgpack | 1.2.4 | Apache-2.0 | [COPYING](third_party/msgpack/licenses/COPYING) |
| multidict | 7.2.1 | Apache-2.0 | [LICENSE](third_party/multidict/licenses/LICENSE) |
| opentelemetry-api | 1.45.1 | Apache-2.0 | [LICENSE](third_party/opentelemetry-api/licenses/LICENSE) |
| propcache | 0.5.4 | Apache-2.0 | [LICENSE](third_party/propcache/licenses/LICENSE), [NOTICE](third_party/propcache/licenses/NOTICE) |
| pycparser | 3.12 | BSD-3-Clause | [LICENSE](third_party/pycparser/licenses/LICENSE) |
| pydantic | 2.14.1 | MIT | [LICENSE](third_party/pydantic/licenses/LICENSE) |
| pydantic_core | 2.50.1 | MIT | [LICENSE](third_party/pydantic_core/licenses/LICENSE) |
| python-multipart | 0.0.32 | Apache-2.0 | [LICENSE.txt](third_party/python-multipart/licenses/LICENSE.txt) |
| python-socks | 3.2.0 | Apache-2.0 | [LICENSE.txt](third_party/python-socks/licenses/LICENSE.txt) |
| qrcode | 8.2 | BSD | [LICENSE](third_party/qrcode/LICENSE) |
| referencing | 0.37.0 | MIT | [COPYING](third_party/referencing/licenses/COPYING) |
| rpds-py | 2026.9.1 | MIT | [LICENSE](third_party/rpds-py/licenses/LICENSE) |
| sse-starlette | 3.6.0 | BSD-3-Clause | [AUTHORS](third_party/sse-starlette/licenses/AUTHORS), [LICENSE](third_party/sse-starlette/licenses/LICENSE) |
| starlette | 1.10.0 | BSD-3-Clause | [LICENSE.md](third_party/starlette/licenses/LICENSE.md) |
| truststore | 0.10.5 | MIT | [LICENSE](third_party/truststore/licenses/LICENSE) |
| typing-inspection | 0.4.4 | MIT | [LICENSE](third_party/typing-inspection/licenses/LICENSE) |
| typing_extensions | 4.16.0 | PSF-2.0 | [LICENSE](third_party/typing_extensions/licenses/LICENSE) |
| uvicorn | 0.54.0 | BSD-3-Clause | [LICENSE.md](third_party/uvicorn/licenses/LICENSE.md) |
| websockets | 17.3 | BSD-3-Clause | [LICENSE](third_party/websockets/licenses/LICENSE) |
| yarl | 1.25.1 | Apache-2.0 | [LICENSE](third_party/yarl/licenses/LICENSE), [NOTICE](third_party/yarl/licenses/NOTICE) |
| zstandard | 0.25.0 | BSD-3-Clause | [LICENSE](third_party/zstandard/licenses/LICENSE) |
