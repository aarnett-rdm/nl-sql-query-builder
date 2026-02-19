# Getting Started with the NL SQL Query Builder

Welcome! This guide will help you get the Query Builder app running on your computer in just a few minutes.

## What You'll Need

- Windows computer with internet access
- Google Drive for Desktop installed (for shared feedback — your admin will confirm)
- About 10 minutes for first-time setup
- That's it! We'll walk you through everything else.

---

## First-Time Setup (Do this once)

### Step 1: Install Git

Git is a tool that helps you download and update the app easily.

1. Go to: https://git-scm.com/download/win
2. Download the installer (it will auto-detect your Windows version)
3. Run the installer with all default settings (just keep clicking "Next")
4. **You can close the installer when done** - you won't need to open Git directly

### Step 2: Install Python

Python is the programming language the app runs on.

1. Go to: https://www.python.org/downloads/
2. Click the big yellow "Download Python" button
3. **IMPORTANT:** When the installer opens, check the box that says "Add Python to PATH"
4. Click "Install Now"
5. Wait for installation to complete

### Step 3: Download the App

1. Open **Command Prompt** (search for "cmd" in Windows Start menu)
2. Navigate to where you want to install the app (example: your Documents folder):
   ```
   cd Documents
   ```
3. Download the app by copying and pasting this command:
   ```
   git clone https://github.com/aarnett-rdm/nl-sql-query-builder.git
   ```
4. Wait for it to download (takes about 30 seconds)

### Step 4: Install Dependencies

Still in Command Prompt:

1. Go into the app folder:
   ```
   cd nl-sql-query-builder
   ```
2. Install required packages (this takes 2-3 minutes):
   ```
   pip install -r physical_schema/requirements.txt
   pip install -r physical_schema/ui/requirements.txt
   ```
3. Wait for everything to install - you'll see lots of text scroll by, this is normal!

### Step 5: Create Your Configuration File

The app needs a small configuration file to know where to save shared feedback.

1. In your `nl-sql-query-builder\physical_schema` folder, find the file called `.env.example`
2. Make a copy of it and rename the copy to `.env` (no `.example` at the end)
3. Open `.env` in Notepad
4. Find the line that says:
   ```
   NL_SQL_FEEDBACK_PATH=
   ```
5. Add the shared feedback folder path after the `=` sign (your admin will give you this path). It will look something like:
   ```
   NL_SQL_FEEDBACK_PATH=G:/Shared drives/Tickets/Sports Team/Query Tool Feedback/corrections.jsonl
   ```
6. Save and close the file

> **Note:** If your admin hasn't set up a shared folder yet, you can leave this blank — your feedback will save locally on your own computer instead.

### Step 6: Create Desktop Shortcut (Optional but Recommended)

This makes it super easy to start the app later:

1. Find the file `start_app.bat` in your `nl-sql-query-builder` folder
2. Right-click it → "Send to" → "Desktop (create shortcut)"
3. Now you can double-click this shortcut to start the app!

---

## Daily Use (After Setup)

### Starting the App

**Option 1: Using the Desktop Shortcut (Easiest)**
1. Double-click the `start_app.bat` shortcut on your desktop
2. A black window will appear, checking for updates
3. After a few seconds, your web browser will open with the app
4. Start asking questions!

**Option 2: Using Command Prompt**
1. Open Command Prompt
2. Navigate to the app folder:
   ```
   cd Documents\nl-sql-query-builder
   ```
3. Run the launcher:
   ```
   start_app.bat
   ```

### Using the Query Builder

1. The app opens in your web browser (but it runs on your computer, not the internet)
2. Type your question in the chat box at the bottom
3. Click the "Connect to Fabric" button in the sidebar (first time only each session)
4. Sign in with your Microsoft account when prompted
5. Ask questions like:
   - "Show me total revenue for last week"
   - "What were clicks and conversions by campaign last month?"
   - "Compare cost per click for Google Ads vs Microsoft Ads this year"

**After you get results, you can:**
- Click **"✨ Summarize results"** to get an AI-written plain-English summary
- Use **"⬇ CSV"** or **"⬇ Excel"** to download the data
- Click any of the **suggested follow-up questions** to keep digging
- Expand **"View SQL"** to see exactly what query ran

### The Other Pages (sidebar)

The app has several pages — use the sidebar to navigate between them:

| Page | What it's for |
|------|--------------|
| **Query Builder** | Ask questions in plain English, get data back |
| **Multi Date Reporting** | Compare the same metrics across multiple date ranges side by side |
| **Query History** | Browse, re-run, and favorite your past queries; share queries with teammates |
| **Schema Explorer** | Browse available metrics, dimensions, and how tables connect |
| **Feedback Dashboard** | Log issues, see patterns in what the app gets wrong |
| **Visual Reports** | Pre-built visual dashboards |

### When You're Done

1. Just close the browser tab
2. In the black Command Prompt window, press `Ctrl+C` to stop the app
3. Type `exit` and press Enter to close the window

Or just close the Command Prompt window directly - the app will stop automatically.

---

## Troubleshooting

### "git is not recognized as a command"

**Fix:** Git wasn't installed correctly or isn't in your PATH.
1. Restart your computer (this refreshes PATH settings)
2. If still not working, reinstall Git and make sure to check "Add to PATH" during installation

### "python is not recognized as a command"

**Fix:** Python wasn't installed correctly or isn't in your PATH.
1. Restart your computer
2. If still not working, reinstall Python and **make sure to check "Add Python to PATH"**

### "Address already in use" or "Port 8501 is already in use"

**Fix:** The app is already running somewhere.
1. Look for other Command Prompt windows and close them
2. Or restart your computer to clear everything

### App won't connect to Fabric

**Fix:** Authentication issue.
1. Make sure you're on the corporate network (or VPN)
2. Try clicking "Connect to Fabric" again
3. Make sure you sign in with your work Microsoft account (not personal)
4. Check with IT if you still can't connect

### "ModuleNotFoundError" or "No module named..."

**Fix:** Dependencies weren't installed correctly.
1. Open Command Prompt
2. Navigate to the app folder: `cd Documents\nl-sql-query-builder`
3. Re-run the install commands:
   ```
   pip install -r physical_schema/requirements.txt
   pip install -r physical_schema/ui/requirements.txt
   ```

### Feedback isn't showing up for the team

**Fix:** The shared feedback path isn't configured.
1. Open your `.env` file in Notepad (it's in `nl-sql-query-builder\physical_schema\`)
2. Check that `NL_SQL_FEEDBACK_PATH=` has the correct path (ask your admin if unsure)
3. Restart the app

### Getting Updates

**The `start_app.bat` script automatically checks for updates every time you start the app!**

If you see "Already up to date" - you're running the latest version.

If you see files downloading - the app is updating itself. Wait for it to finish, then it will start automatically.

---

## Getting Help

### Report a Bug or Request a Feature

1. Go to: https://github.com/aarnett-rdm/nl-sql-query-builder/issues
2. Click "New Issue"
3. Describe what went wrong (or what feature you'd like)
4. Include screenshots if possible

### Ask for Help

Contact Andrew Arnett via:
- Email: aarnett@reddogmediainc.com
- Teams: @aarnett
- GitHub Issues: https://github.com/aarnett-rdm/nl-sql-query-builder/issues

---

## Tips for Success

✅ **Always let the app update** when you start it - updates include bug fixes and new features

✅ **Be specific in your questions** - "Show revenue and clicks for Google Ads last week" works better than "show me data"

✅ **Use follow-up questions** - after getting results, the app suggests related questions you can click to keep exploring

✅ **Export your results** - use the CSV or Excel buttons below any results table to take data elsewhere

✅ **Review the generated SQL** - it's in a collapsible section below results. This helps you verify the app interpreted your question correctly

✅ **Use the feedback button** - if the app gets something wrong, click the thumbs down and describe the issue. This helps improve the system for everyone

✅ **Check Query History** - if you ran a useful query before, find it in Query History and re-run or share it instead of retyping

✅ **Start simple** - try basic questions first, then work up to complex multi-metric comparisons
