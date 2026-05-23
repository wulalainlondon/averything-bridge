# Codex Quick Instruction

When an agent opens this repo, perform setup automatically:

1. Detect OS.
2. If Windows:
   - Run `powershell -ExecutionPolicy Bypass -File .\install_windows.ps1`
   - Run `powershell -ExecutionPolicy Bypass -File .\install_windows_startup.ps1`
3. If macOS/Linux:
   - Run `chmod +x install_oneclick.sh`
   - Run `./install_oneclick.sh`
4. Start bridge if not already running.
5. Report final connect URL as `ws://<local-ip>:8766`.
6. **Ask the user:** "Do you want me to install the Android app on your phone via ADB?"
   - If **yes**:
     ```bash
     # Check ADB is available
     which adb || echo "ADB not found — install via: brew install android-platform-tools"

     # Check connected devices
     adb devices

     # Download and install APK
     curl -L -o /tmp/averything.apk https://github.com/wulalainlondon/averything-bridge/releases/latest/download/averything-v1.1.0.apk
     adb install /tmp/averything.apk
     ```
     If no device is listed by `adb devices`, tell the user to connect their phone via USB with USB debugging enabled (Settings → Developer options → USB debugging), then retry.
   - If **no** (or ADB unavailable): tell the user to download the APK manually from:
     `https://github.com/wulalainlondon/averything-bridge/releases/latest`

Do not ask follow-up questions unless a command fails.
