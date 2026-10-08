from __future__ import annotations

import hashlib
import shutil
import subprocess
import sys
import venv
from pathlib import Path


ROOT = Path(__file__).resolve().parent
VENV = ROOT / ".venv"
REQUIREMENTS = ROOT / "requirements.txt"
MARKER = VENV / ".requirements.sha256"


def venv_python() -> Path:
    if sys.platform == "win32":
        return VENV / "Scripts" / "python.exe"
    return VENV / "bin" / "python"


def requirements_hash() -> str:
    return hashlib.sha256(REQUIREMENTS.read_bytes()).hexdigest()


def ensure_user_files() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        shutil.copy2(ROOT / ".env.example", env_path)
        print("Создан .env — после установки вставьте в него cookies Playerok.")


def ensure_environment() -> Path:
    python = venv_python()
    if not python.exists():
        print("Первый запуск: создаю изолированное Python-окружение…")
        venv.EnvBuilder(with_pip=True).create(VENV)
    expected = requirements_hash()
    current = MARKER.read_text(encoding="ascii").strip() if MARKER.exists() else ""
    if current != expected:
        print("Устанавливаю зависимости (это нужно только при первом запуске/обновлении)…")
        subprocess.check_call(
            [str(python), "-m", "pip", "install", "--disable-pip-version-check", "-r", str(REQUIREMENTS)],
            cwd=ROOT,
        )
        MARKER.write_text(expected, encoding="ascii")
    return python


def main() -> int:
    ensure_user_files()
    python = ensure_environment()
    return subprocess.call([str(python), str(ROOT / "main.py"), *sys.argv[1:]], cwd=ROOT)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        print(f"\nНе удалось установить зависимости (код {exc.returncode}).")
        raise SystemExit(exc.returncode)

