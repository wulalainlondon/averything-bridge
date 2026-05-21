# Bridge Release Checklist

目的：避免 bridge 在發版後出現啟動失敗、port 假活著、或語法回歸。

## 1) 修改後必做（本機 source）

在 `bridge/` 目錄執行：

```bash
venv/bin/python -m py_compile bridge_v2.py
bash quick_verify.sh
```

要求：
- `py_compile` 必須成功
- `quick_verify.sh` 的 health 與 compile guard 必須通過

## 2) 安裝到 runtime（唯一正式路徑）

```bash
cd ~/.claude-bridge-runtime
bash install.sh
```

說明：
- 不要混用 source 與 runtime 兩套手動啟動流程
- 以 `install.sh` 同步程式碼 + 重載 launchd 為準

## 3) 發版前健康檢查（runtime）

```bash
launchctl list | rg 'com\.claude-bridge\.app'
lsof -nP -iTCP:8766 -sTCP:LISTEN
~/.claude-bridge-runtime/venv/bin/python ~/.claude-bridge-runtime/bridge_healthcheck.py --host 127.0.0.1 --port 8766
```

要求：
- launchd service 存在
- `8766` 有 LISTEN
- healthcheck exit code = 0

## 4) pairing 相關程式規則（強制）

- 在 handler 內，禁止對 `_PAIRING` 做 rebind（例如 `_PAIRING = {...}` / `_PAIRING = {}`）。
- 一律使用原地更新：
  - `_PAIRING.clear()`
  - `_PAIRING.update({...})`

原因：避免 Python 將 `_PAIRING` 判定為 local，導致 `used prior to global declaration` 類型錯誤。

## 5) 問題排查順序（固定）

1. `tail -n 80 ~/.claude-bridge-runtime/bridge.err`
2. `lsof -nP -iTCP:8766 -sTCP:LISTEN`
3. `bridge_healthcheck.py` exit code
4. `launchctl list | rg com.claude-bridge.app`

