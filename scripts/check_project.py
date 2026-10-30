"""Проверяет действующие документы проекта без изменения файлов и сетевых запросов."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FILES = (
    "README.md", "AGENTS.md", ".gitignore", ".github/workflows/hygiene.yml",
    "docs/README.md", "docs/PROJECT.md", "docs/STATUS.md",
    "docs/ARCHITECTURE.md", "docs/DEVELOPMENT.md", "docs/OPERATIONS.md",
    "docs/tasks/README.md", "docs/tasks/TEMPLATE.md", "docs/archive/README.md",
    "scripts/check_project.py",
)
ALLOWED_DOCS_DIRECTORIES = {"tasks", "archive", "assets"}
TEMPLATE_FIELDS = {"Мост MAX для Гермеса", "Дать владельцу возможность поручать Гермесу чтение и поиск переписки MAX, отправку сообщений, скачивание и отправку файлов через личный аккаунт, вручную и по расписанию, с сохранением контроля над доступом.", "2026-10-28"}
FIELD_PATTERN = re.compile(r"\{\{[A-Z][A-Z0-9_]*\}\}")
LINK_PATTERN = re.compile(r"(?<!\\)\[[^\]\n]*\]\(([^)\n]+)\)")
SETUP_PATTERN = re.compile(r"<!--\s*НАСТРОИТЬ:\s*(.*?)\s*-->", re.DOTALL)


def without_code_blocks(text: str) -> str:
    """Не принимает ссылки внутри примеров команд за ссылки самого документа."""
    result: list[str] = []
    fence: str | None = None
    for line in text.splitlines():
        stripped = line.lstrip()
        if fence:
            if stripped.startswith(fence):
                fence = None
            continue
        match = re.match(r"(`{3,}|~{3,})", stripped)
        if match:
            fence = match.group(1)
        else:
            result.append(line)
    return "\n".join(result)


def local_links(root: Path, source: Path, text: str, errors: list[str]) -> set[Path]:
    """Проверяет обычные Markdown-ссылки; адреса сайтов не загружает."""
    targets: set[Path] = set()
    for match in LINK_PATTERN.finditer(without_code_blocks(text)):
        target = match.group(1).strip()
        if target.startswith("<") and ">" in target:
            target = target[1:target.index(">")]
        else:
            target = target.split(' "', 1)[0]
        if target.startswith(("https://", "http://", "mailto:", "tel:", "#")):
            continue
        if urlsplit(target).scheme or target.startswith(("/", "\\")):
            errors.append(f"{source}: ссылка должна быть относительной: {target}")
            continue
        target = unquote(target.split("#", 1)[0].split("?", 1)[0])
        if not target:
            continue
        resolved = (root / source.parent / target).resolve()
        if not resolved.is_relative_to(root):
            errors.append(f"{source}: ссылка выходит за пределы проекта: {target}")
        elif not resolved.exists():
            errors.append(f"{source}: не найдена цель ссылки: {target}")
        else:
            targets.add(resolved)
    return targets


def check_project(root: Path, *, template: bool, ready: bool) -> tuple[list[str], int, int]:
    """Сверяет обязательный комплект, реестр документов и незаполненные сведения."""
    errors: list[str] = []
    for relative in REQUIRED_FILES:
        if not (root / relative).is_file():
            errors.append(f"Нет обязательного файла: {relative}")

    docs = root / "docs"
    current = {Path("README.md"), Path("AGENTS.md"), Path("docs/archive/README.md")}
    if docs.is_dir():
        for child in docs.iterdir():
            if child.is_dir() and child.name not in ALLOWED_DOCS_DIRECTORIES:
                errors.append(f"docs/: незарегистрированный каталог {child.name}")
            elif child.is_file() and child.suffix.lower() != ".md":
                errors.append(f"docs/: посторонний файл {child.name}; иллюстрации храните в assets/")
        current.update(path.relative_to(root) for path in docs.glob("*.md"))
        current.update(path.relative_to(root) for path in (docs / "tasks").glob("*.md"))

    links: dict[Path, set[Path]] = {}
    setup_count = 0
    read_count = 0
    for relative in sorted(current):
        path = root / relative
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8-sig")
        except (OSError, UnicodeError) as exc:
            errors.append(f"Не удалось прочитать {relative}: {exc}")
            continue
        read_count += 1
        links[relative] = local_links(root, relative, text, errors)
        for field in sorted(set(FIELD_PATTERN.findall(text))):
            if not template or field not in TEMPLATE_FIELDS:
                errors.append(f"{relative}: незаменённое поле {field}")
        setup = SETUP_PATTERN.findall(text)
        setup_count += len(setup)
        if ready:
            errors.extend(f"{relative}: требуется настройка — {item}" for item in setup)
        if relative.parent == Path("docs"):
            if "Статус:" not in text or "Обновлено:" not in text:
                errors.append(f"{relative}: нужны статус и дата обновления")

    # Реестр задаёт действующий комплект; новый справочник требует явной ссылки.
    registered = links.get(Path("docs/README.md"), set())
    for path in docs.glob("*.md"):
        if path.name != "README.md" and path.resolve() not in registered:
            errors.append(f"docs/README.md: не зарегистрирован {path.name}")

    active = links.get(Path("docs/STATUS.md"), set())
    for path in (docs / "tasks").glob("*.md"):
        if path.name not in {"README.md", "TEMPLATE.md"} and path.resolve() not in active:
            errors.append(f"docs/STATUS.md: нет ссылки на карточку {path.name}")
    return errors, read_count, setup_count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT, help="корень проверяемого проекта")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--template", action="store_true", help="разрешить известные поля образца")
    mode.add_argument("--ready", action="store_true", help="потребовать заполнения начальных сведений")
    args = parser.parse_args()
    root = args.root.expanduser().resolve()
    if not root.is_dir():
        parser.error(f"Каталог проекта не существует: {root}")
    errors, count, setup_count = check_project(root, template=args.template, ready=args.ready)
    if errors:
        print("Проверка не пройдена:")
        for error in errors:
            print(f"- {error}")
        return 1
    print(f"Структура и ссылки в порядке: проверено документов — {count}.")
    if setup_count:
        print(f"Начальные сведения ещё требуют заполнения: пунктов — {setup_count}.")
    print("Эта проверка не подтверждает работоспособность продукта или смысловую актуальность текста.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
