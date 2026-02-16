# GitHub Setup Instructions

This document contains the exact commands to push your local repository to GitHub.

## ✅ What's Already Done

- [x] Local git repository initialized
- [x] Initial commit created (118 files)
- [x] Documentation updated with correct URLs
- [x] `.gitignore` configured to exclude sensitive files

## 🚀 Next Steps: Create Remote Repo and Push

### Option 1: Using GitHub CLI (Recommended - Fastest)

If you have GitHub CLI installed (`gh`):

```bash
cd c:/Users/aarnett/RAG

# Authenticate to GitHub (if not already)
gh auth login

# Create the private repo and push in one command
gh repo create aarnett-rdm/nl-sql-query-builder --private --source=. --remote=origin --push
```

**Done!** The repo is created and pushed. Skip to "Verify It Worked" below.

---

### Option 2: Using GitHub Web Interface (Manual)

If you don't have GitHub CLI:

#### Step 1: Create the Repo on GitHub.com

1. Go to: https://github.com/new
2. Fill in the form:
   - **Owner**: `aarnett-rdm` (select from dropdown)
   - **Repository name**: `nl-sql-query-builder`
   - **Description** (optional): "Natural language to SQL query builder with LLM integration for Fabric DW"
   - **Visibility**: ⚫ **Private**
   - **DO NOT** check "Add a README file" (we already have one)
   - **DO NOT** add .gitignore or license (we already have these)
3. Click **"Create repository"**

#### Step 2: Push Your Local Repo

GitHub will show you commands after creation. Use these:

```bash
cd c:/Users/aarnett/RAG

# Add the remote
git remote add origin https://github.com/aarnett-rdm/nl-sql-query-builder.git

# Push everything
git push -u origin master
```

**Note:** You'll be prompted for credentials. If you have 2FA enabled (you should!), you'll need to use a Personal Access Token instead of your password:
- Create token: https://github.com/settings/tokens
- Select scopes: `repo` (full control of private repos)
- Copy the token and use it as your password when prompted

---

## ✅ Verify It Worked

1. Go to: https://github.com/aarnett-rdm/nl-sql-query-builder
2. You should see:
   - ✅ README.md displayed on the home page
   - ✅ 118 files in the repository
   - ✅ 2 commits in the history
   - ✅ "Private" badge in the top right

---

## 🔄 Daily Workflow (After Initial Push)

### Making Changes and Pushing Updates

```bash
cd c:/Users/aarnett/RAG

# Make your changes to files...

# Stage changed files
git add .

# Commit with a message
git commit -m "Your descriptive message here"

# Push to GitHub
git push
```

### Example Commit Messages

- ✅ `Fix: corrected conversion rate calculation in Multi Date Reporting`
- ✅ `Feature: add ROI metric to metric registry`
- ✅ `Update: improve LLM prompt for date range disambiguation`
- ✅ `Docs: add troubleshooting section for Fabric auth`

---

## 👥 Inviting Team Members (When Ready)

When you want others to access the repo:

1. Go to: https://github.com/aarnett-rdm/nl-sql-query-builder/settings/access
2. Click **"Add people"**
3. Enter their GitHub username or email
4. Choose permission level:
   - **Read**: Can view and clone (for users who just run the app)
   - **Write**: Can push changes (for contributors)
   - **Admin**: Full control (for co-maintainers)

---

## 🏢 Transferring to Organization (Future)

When your department is ready to move this to an official org account:

### Step 1: Prepare for Transfer

1. Make sure all changes are committed and pushed
2. Note any open Issues or Pull Requests (they'll transfer too)
3. Inform team members (repo URL will change)

### Step 2: Transfer Ownership

1. Go to: https://github.com/aarnett-rdm/nl-sql-query-builder/settings
2. Scroll down to **"Danger Zone"**
3. Click **"Transfer ownership"**
4. Enter the organization name (e.g., `red-dog-media` or `rdm-analytics`)
5. Type the repository name to confirm
6. Click **"I understand, transfer this repository"**

### Step 3: Accept Transfer (Org Admin)

1. An org admin will receive a notification
2. They need to accept the transfer
3. The repo moves to: `https://github.com/ORG-NAME/nl-sql-query-builder`

### Step 4: Update Team Member Remotes

Each team member (including you) needs to update their local repo:

```bash
cd nl-sql-query-builder

# Update the remote URL
git remote set-url origin https://github.com/ORG-NAME/nl-sql-query-builder.git

# Verify it worked
git remote -v

# Pull to confirm
git pull
```

**What transfers:**
- ✅ All code and commit history
- ✅ All branches and tags
- ✅ All Issues and Pull Requests
- ✅ All releases and wiki pages
- ✅ All webhooks and settings

**What doesn't transfer:**
- ❌ Stars and watchers (reset to 0)
- ❌ Forks (they stay linked to old location)

---

## 🔒 Security Best Practices

### Protecting the Main Branch

Once the repo is set up:

1. Go to: https://github.com/aarnett-rdm/nl-sql-query-builder/settings/branches
2. Click **"Add rule"** (or **"Add classic branch protection rule"**)
3. Branch name pattern: `master` (or `main`)
4. Enable:
   - ✅ **Require pull request reviews before merging** (when you have multiple contributors)
   - ✅ **Require status checks to pass** (if you set up CI/CD later)
   - ✅ **Require conversation resolution before merging**
   - ✅ **Do not allow force pushes**
   - ✅ **Do not allow deletions**

### Managing Secrets

**NEVER commit these to GitHub:**
- `.env` files (already in `.gitignore` ✅)
- API keys or passwords
- Database connection strings with credentials
- Personal access tokens

If you accidentally commit a secret:
1. **Immediately revoke/rotate the secret** (change passwords, regenerate tokens)
2. Remove from history: `git filter-branch` or `git filter-repo`
3. Force push: `git push --force` (only if no one else has pulled)

---

## 📊 GitHub Features to Explore

### Issues (Bug Tracking)

- Create templates: `.github/ISSUE_TEMPLATE/bug_report.md`
- Label issues: `bug`, `enhancement`, `question`, `documentation`
- Assign to team members
- Link to Pull Requests

### Projects (Kanban Board)

- Track TODOs, In Progress, Done
- Link Issues and PRs to project cards
- Great for sprint planning

### Actions (CI/CD)

- Auto-run tests on every push
- Auto-deploy on merge to main
- Lint checks, security scans
- Example: `.github/workflows/test.yml`

### Releases

- Tag versions: `v1.0.0`, `v1.1.0`
- Add release notes
- Attach binary artifacts
- Track changelog

---

## 🆘 Troubleshooting

### "Authentication failed"

**Fix:** Use a Personal Access Token instead of password:
1. Create: https://github.com/settings/tokens/new
2. Scopes: `repo` (full control)
3. Copy token
4. Use as password when prompted

Or use SSH instead:
```bash
git remote set-url origin git@github.com:aarnett-rdm/nl-sql-query-builder.git
```

### "remote: Permission denied"

**Fix:** Make sure you're logged in as `aarnett-rdm`:
```bash
gh auth status
```

### "fatal: remote origin already exists"

**Fix:** Remove and re-add:
```bash
git remote remove origin
git remote add origin https://github.com/aarnett-rdm/nl-sql-query-builder.git
```

### "rejected: non-fast-forward"

**Fix:** Someone else pushed changes. Pull first:
```bash
git pull --rebase origin master
git push
```

---

## 📞 Need Help?

- **GitHub Docs**: https://docs.github.com
- **Git Docs**: https://git-scm.com/doc
- **Contact**: Andrew Arnett (aarnett@reddogmediainc.com)

---

## ✅ Quick Reference

```bash
# First time only (choose Option 1 or 2 from above)
gh repo create aarnett-rdm/nl-sql-query-builder --private --source=. --remote=origin --push

# Daily workflow
git add .
git commit -m "Your message"
git push

# Get updates from others
git pull

# Check status
git status

# View commit history
git log --oneline -10
```

**Next:** See [GETTING_STARTED.md](GETTING_STARTED.md) for how users should clone and use the repo.
