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

7. **Ask the user:** "Do you want push notifications when AI tasks finish? (optional — requires a free Firebase account)"
   - If **yes**, follow these steps:
     ```
     Step 1: Go to https://console.firebase.google.com and create a new project.
     Step 2: In the project, go to Project Settings → Service accounts → Generate new private key.
             Download the JSON file.
     Step 3: Place the file at ~/.config/claude-bridge/serviceAccountKey.json
             (or set SERVICE_ACCOUNT_FILE env var to its path)
     Step 4: Run the bridge install script again so it picks up the key:
     ```
     ```bash
     # macOS/Linux
     bash install.sh

     # Windows
     powershell -ExecutionPolicy Bypass -File .\install_windows_startup.ps1
     ```
     Confirm FCM is active by checking the bridge log for "Firebase FCM initialized".
   - If **no**: skip — bridge works fine without it. Push notifications will be disabled.

Do not ask follow-up questions unless a command fails.
