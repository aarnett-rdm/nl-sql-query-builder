# Git Cheat Sheet for Query Builder Users

This is a quick reference for the **3 Git commands** you need to use the Query Builder app. That's it - just 3 commands!

---

## The Only 3 Commands You Need

### 1️⃣ Clone (Download the App) - Do Once

```bash
git clone https://github.com/aarnett-rdm/nl-sql-query-builder.git
```

**What it does:** Downloads the app to your computer for the first time.

**When to use it:** Only once, when you first set up the app.

**Example:**
```bash
cd Documents
git clone https://github.com/aarnett-rdm/nl-sql-query-builder.git
cd nl-sql-query-builder
```

---

### 2️⃣ Pull (Get Updates) - Do Before Each Use

```bash
git pull
```

**What it does:** Downloads the latest updates from the shared repository.

**When to use it:** Every time before you start the app (the `start_app.bat` script does this automatically!)

**Example:**
```bash
cd Documents\nl-sql-query-builder
git pull
```

**What you'll see:**
- `Already up to date` - You're running the latest version ✅
- `Updating abc123..def456` - Downloading updates ⬇️
- File names scrolling by - Updates being applied 📝

---

### 3️⃣ Status (Check for Changes) - Optional

```bash
git status
```

**What it does:** Shows if you have the latest version or if there are updates available.

**When to use it:** When you want to check if you're up to date (rarely needed).

**Example:**
```bash
cd Documents\nl-sql-query-builder
git status
```

**What you'll see:**
- `Your branch is up to date` - You have the latest ✅
- `Your branch is behind by X commits` - Updates available, run `git pull` ⬇️

---

## Common Scenarios

### "I want to start the app"

**Easy way:**
```bash
# Just double-click: start_app.bat
# It automatically does git pull for you!
```

**Manual way:**
```bash
cd Documents\nl-sql-query-builder
git pull
cd physical_schema
streamlit run ui/Query Builder.py
```

---

### "Is there a new version?"

```bash
cd Documents\nl-sql-query-builder
git status
```

If it says "behind by X commits", run:
```bash
git pull
```

---

### "I want to see what changed in the latest update"

```bash
git log --oneline -5
```

Shows the last 5 updates with short descriptions.

**Example output:**
```
def456 Fix: corrected conversion rate calculation
abc123 Feature: added revenue per conversion metric
789xyz Update: improved error messages
```

---

### "Something broke after an update, I want to go back"

⚠️ **This erases any changes you made locally - use with caution!**

```bash
git log --oneline -10
# Find the commit hash (abc123) of the version you want
git reset --hard abc123
```

**Better approach:** Report the bug and wait for a fix, or contact support.

---

## Troubleshooting

### "git is not recognized"

**Fix:** Git isn't installed or not in your PATH.
1. Restart your computer
2. If still broken, reinstall Git: https://git-scm.com/download/win
3. Make sure to check "Add to PATH" during installation

---

### "error: Your local changes would be overwritten by merge"

**What happened:** You accidentally edited a file in the app folder.

**Fix Option 1 (keep your changes):**
```bash
git stash
git pull
git stash pop
```

**Fix Option 2 (discard your changes):**
```bash
git reset --hard
git pull
```

---

### "fatal: not a git repository"

**What happened:** You're not in the right folder.

**Fix:**
```bash
cd Documents\nl-sql-query-builder
# Now try your git command again
```

---

### "Could not resolve host: github.com"

**What happened:** No internet connection or GitHub is blocked.

**Fix:**
1. Check your internet connection
2. Make sure you're on VPN if required
3. If GitHub is blocked, contact IT

---

## Commands You DON'T Need (But Might Hear About)

As a **user** of the app (not a developer), you don't need these commands:

- ❌ `git add` - Only for developers contributing code
- ❌ `git commit` - Only for developers contributing code
- ❌ `git push` - Only for developers contributing code
- ❌ `git branch` - Only for developers managing multiple versions
- ❌ `git merge` - Only for developers combining changes
- ❌ `git checkout` - Only for developers switching between versions

**You're a read-only user** - you only need `clone`, `pull`, and occasionally `status`.

---

## Quick Reference Card

Print this and keep it at your desk:

```
┌─────────────────────────────────────────────────┐
│         GIT COMMANDS FOR QUERY BUILDER          │
├─────────────────────────────────────────────────┤
│                                                 │
│  📥 Download (first time only):                 │
│     git clone <repo-url>                        │
│                                                 │
│  🔄 Update (before each use):                   │
│     git pull                                    │
│                                                 │
│  ✅ Check status (optional):                    │
│     git status                                  │
│                                                 │
│  🚀 Or just double-click:                       │
│     start_app.bat                               │
│     (does everything automatically!)            │
│                                                 │
└─────────────────────────────────────────────────┘
```

---

## Getting Help

### For Git Issues
- Git documentation: https://git-scm.com/doc
- GitHub guides: https://guides.github.com/

### For App Issues
- Contact: Andrew Arnett via email/Teams
- Create an issue: https://github.com/aarnett-rdm/nl-sql-query-builder/issues

---

## Next Steps

Once you're comfortable with these basics:

1. ✅ You can start using the app without thinking about Git
2. ✅ The `start_app.bat` script handles everything automatically
3. ✅ You'll always have the latest version
4. ✅ If you want to learn more Git later, you already know the fundamentals!

**Remember:** You only need `git clone` once and `git pull` to update. That's it! The rest is optional.
