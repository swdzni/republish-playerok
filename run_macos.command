#!/bin/bash
cd "$(dirname "$0")" || exit 1

if command -v python3 >/dev/null 2>&1; then
  python3 launcher.py
else
  echo "Python 3 не найден. Установите Python 3 с https://www.python.org/downloads/macos/"
  exit 1
fi

EXIT_CODE=$?
echo
if [ "$EXIT_CODE" -ne 0 ]; then
  echo "Скрипт завершился с кодом $EXIT_CODE. Проверьте сообщение выше и logs/republisher.log."
fi
read -r -p "Нажмите Enter, чтобы закрыть окно…"
exit "$EXIT_CODE"
