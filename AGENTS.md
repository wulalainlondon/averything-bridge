# AI Agent Quick Instruction

When an agent opens this repo, perform setup automatically. Do not stop after cloning.

1. Detect OS.
2. If Windows:
   - Run `powershell -ExecutionPolicy Bypass -File .\install_windows.ps1`
   - Run `powershell -ExecutionPolicy Bypass -File .\install_windows_startup.ps1`
3. If macOS/Linux:
   - Run `chmod +x install_oneclick.sh`
   - Run `./install_oneclick.sh`
4. Start bridge if not already running.
5. Detect the local LAN IP.
6. Report final connect URL as `ws://<local-ip>:8766`.
7. Tell the user that the Averything mobile app should discover the bridge automatically when the phone and computer are on the same Wi-Fi.
8. **Ask the user:** "Do you want me to install the Android app on your phone via ADB?"
   - If **yes**:
     ```bash
     # Check ADB is available
     which adb || echo "ADB not found — install via: brew install android-platform-tools"

     # Check connected devices
     adb devices

     # Download and install APK
     curl -L -o /tmp/averything.apk https://github.com/BridgeAverthing/averything-app/releases/latest/download/averything-v1.2.4.apk
     adb install /tmp/averything.apk
     ```
     If no device is listed by `adb devices`, tell the user to connect their phone via USB with USB debugging enabled (Settings → Developer options → USB debugging), then retry.
   - If **no** (or ADB unavailable): tell the user to download the APK manually from:
     `https://github.com/BridgeAverthing/averything-app/releases/latest/download/averything-v1.2.4.apk`

9. **Ask the user:** "Do you want push notifications when AI tasks finish? (optional, free)"
   - If **yes**, set it up automatically:
     ```bash
     # 1. Check gcloud / firebase CLI are available
     which gcloud && which firebase || npm install -g firebase-tools

     # 2. Create a Firebase project (choose any project-id)
     firebase projects:create <project-id> --display-name "Bridge"

     # 3. Create a service account key
     gcloud iam service-accounts keys create ~/.config/claude-bridge/serviceAccountKey.json \
       --iam-account=firebase-adminsdk-fbsvc@<project-id>.iam.gserviceaccount.com

     # 4. Copy key into bridge runtime so it takes effect immediately
     mkdir -p ~/.config/claude-bridge
     cp ~/.config/claude-bridge/serviceAccountKey.json ~/.claude-bridge-runtime/serviceAccountKey.json

     # 5. Restart bridge
     bash install.sh
     ```
     Confirm success by checking the log: `grep "FCM initialized" ~/.claude-bridge-runtime/bridge_v2.log`
   - If **no**: skip — bridge works fully without it. Push notifications will be disabled.

Do not ask follow-up questions unless a command fails.
