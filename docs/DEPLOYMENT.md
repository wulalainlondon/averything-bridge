# Go Bridge 電腦部署流程

更新：2026-10-23。適用本倉庫目前的安裝入口；本文件不代表已向任何新電腦部署，亦不代表已公開發布新版本。

## 先選路徑

- **首次安裝**：使用 `install.sh`，目標資料目錄須為全新或空目錄，服務名稱及 TCP port 未被占用。
- **既有電腦更新**：走下方保留資料流程。不要重新執行首次安裝器覆蓋原 launcher、pairing、資料目錄或 service guard。
- **只做預檢**：相同參數加 `--check`；會下載或複製套件到一次性 staging、驗證後移除，不安裝、不啟動或重啟服務、不改配對或系統權限。預檢也會檢查目標是否適合首次安裝；已存在的部署會被拒絕。

macOS Intel 與 Apple Silicon 需使用各自合格的套件。Morrie 實際啟用 Go171 Intel，Wulala 實際啟用 Go170 ARM64；不能由原碼版本推定兩台已裝相同版本。上層工作區的 `docs/MORRIE_GO_CURRENT_RELEASE.json`、`docs/GO_CURRENT_RELEASE.json` 記錄實際收據。本次安裝器修訂沒有產生新的 Go App binary。

## 首次安裝：依序執行

1. **檢查目標**：確認 CPU、macOS、空間、預定登入使用者、資料目錄及 port（預設8766）。同一電腦已有另一個 Bridge 時，先盤點它的服務和資料；另選目錄/label/port不代表可以共用或複製它的身分。
2. **準備那台電腦自己的 CLI**：安裝並登入 Codex 和／或 Claude。安裝器只確認可找到 executable；登入、訂閱/額度及原生 app-server socket 要另行核對。沿用健康的原生服務；安裝器不代重啟 Codex daemon。
3. **準備網路**：預設啟用 LAN UDP discovery/mDNS，不傳 `--tunnel`、不安裝/下載/啟動 cloudflared。外出連線先在電腦與手機完成 Tailscale 設定，再在 App 選定該來源的 Tailscale 位址。安裝器不會安裝或登入 Tailscale，也不會啟用 Funnel。若使用者另行選擇 Cloudflare，需明確設 `EVERYTHING_GO_TUNNEL=1` 且已有 cloudflared。
4. **選定已核套件**：在目標電腦放好對應架構的正式 `.app.zip`，由獨立的 qualified release record取得 SHA256。不要把本次對任意下載檔計算出的hash當成來源證明。不要把「原碼已171」解讀為「GitHub已發布171」。
5. **先預檢，再安裝**：依下方命令設定package與checksum。macOS會核SHA、Developer ID/team、strict codesign、Gatekeeper、stapled ticket、CPU、最低OS與bundle版本/source。需要 `xcrun stapler` 等工具；缺少時停止，不跳過驗證。
6. **檔案權限與背景服務**：安裝至目標目錄，產生安全引用路徑的wrapper；新安裝的session-store預設在本身runtime內。檔案權限預檢通過後，建立使用者LaunchAgent，單次bootstrap，核對PID、port及實際執行檔inode。它是在使用者登入時自動啟動，不承諾未登入即可服務。
7. **配對及驗收**：在手機App新增該電腦、完成配對，核對來源身分、session/runtime/replay ready、歷史及收件匣HTTP。再測未授權請求被拒絕。文字任務須有明確測試內容與原request receipt；沒有真的送出時不能宣稱AI工作端到端成功。語音、音樂、鎖屏、推播與LINE各自另驗。

### 已核本機套件命令

以下是範本，請先將 `<...>` 換成目標電腦的實際值；不要直接貼含占位符的內容執行。使用新的資料目錄與名稱。

```bash
export EVERYTHING_GO_ARCHIVE='/absolute/path/to/reviewed.app.zip'
export EVERYTHING_GO_ARCHIVE_SHA256='<64-character-reviewed-sha256>'
export EVERYTHING_GO_SOURCE_REVISION='<40-character-reviewed-source-commit>'
export EVERYTHING_GO_HOME="$HOME/.everything-go-runtime"
export EVERYTHING_GO_LABEL='com.everything-go.app'
export EVERYTHING_GO_PORT=8766
export EVERYTHING_GO_INSTANCE_NAME='My Mac'
export EVERYTHING_GO_TUNNEL=0

bash install.sh --check
# 僅在預檢通過且已決定部署後執行：
bash install.sh
```

`install.sh` 來自本倉庫已檢查的原碼。不是從不固定的 `latest` 抓另一份腳本；也不能拿Morrie專用的歷史launcher當新機安裝器。

若只使用已發布的GitHub套件，清除 `EVERYTHING_GO_ARCHIVE`、`EVERYTHING_GO_ARCHIVE_SHA256`，改設明確的 `EVERYTHING_GO_TAG=vX.Y.Z`。tag必須已發布且經讀回核對；下載固定tag的架構套件及SHA256SUMS，找不到即停止，不選另一版本。`EVERYTHING_GO_SOURCE_REVISION` 可另固定預期來源。沒有可核對的新版公開下載連結時，使用已核本機套件，不捏造連結。

### 首裝失敗的處理

- SHA／簽章／架構／OS或目標衝突：在安裝前停止，清掉本次staging，原目標不覆寫。
- 檔案權限不足：`.app`與wrapper可能已就位，但尚未建立/啟動新服務。由本人完成系統設定授權後，操作者核對同一套件及原目標，重跑該bundle的 `--permission-check`，確認後用同一個 `write_launchd_plist`／單次bootstrap流程繼續。不要反覆跑首次安裝器；它會拒絕現有目錄。若要重新首裝，先核實尚無身分/資料/已接受工作，再移除本次未啟用的檔案。
- bootstrap或health結果不明：保留app、wrapper、plist與logs，查原label/PID/port及收到的工作，確認原操作結果後才決定下一步。沒有自動restart／舊程式或資料庫回退；若需抑制新TaskAPI admission，採相同來源的compatible hold流程。

## 既有電腦更新：保留資料流程

這是Morrie159→170→171實際更新採用的方式；一次更新不必重做手機配對。

1. **記錄原狀**：實際binary版本/SHA/PID、service label、port、原wrapper/plist全部參數、資料/registry路徑、CLI/socket、service guards；核對原native/LINE等其他程序。不能只讀舊CURRENT的版本文字。
2. **備份與保全**：備份原wrapper/plist、pairing/instance/config與registry；SQLite用backup API取得含WAL的一致快照、quick_check及必要原table/row證據。憑證留在原機私有目錄，不匯出到一般報告，也不複製其他電腦的帳號/身分。
3. **旁置合格候選**：把已簽公證套件放到新的版本目錄，確認exact SHA/source、架構、OS及所有原flags可用。已啟用的LINE config/store存在時，必須跑原stable service guard，不能因candidate缺資源而靜默停用LINE。
4. **等待閒置並重查**：核目前running/queue/native maintenance及語音狀態，在部署鎖內重查原PID、原flags和檔案SHA。已接受但未完成的工作不能用停止或取消方式製造閒置。
5. **只切Go服務**：停原確切label一次，停止確認後做最後資料快照；原wrapper只換必要binary路徑，若有guard也同步exact candidate SHA。保留原資料目錄、登入、native capture、推播及LAN/Tailscale設定，啟動同一label一次，不順帶重啟Codex/Claude或獨立LINE Bridge。
6. **讀回真正效果**：新PID實際指向候選、8766 listener、來源身分/配對hash、原table/row、其他程序保持。手機重新連線，核WS/bootstrap及一般App HTTP讀取、拒絕錯誤憑證；不只看程序存在。
7. **記錄與清理**：更新逐電腦CURRENT和來源/套件/實機證據，分開工程測試、人耳與未驗功能。清理本次可重建輸出和重複上傳，保active、相容恢復資料、精確正式套件、原始失敗及備份。新版本接受過工作後，不能盲目恢復舊DB/舊executor。

## 額外能力

推播需沿原設定或已正式部署的受管push relay註冊與實測；不把操作者的Google service-account key放入通用安裝包。[Push relay文件](PUSH_RELAY.md)記錄實作與部署限制。LINE帳號、原生capture、跨電腦授權、語音與音樂服務均有各自的設定與授權範圍，不由首次安裝器複製Morrie/Wulala的私有設定。

Linux保留固定tag/checksum和user-systemd安裝分支，但本輪只核macOS部署路徑，並未新增Linux正式套件或實機驗收；Windows不在此安裝器支援範圍。
