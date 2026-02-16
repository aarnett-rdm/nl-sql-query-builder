# Feedback Guide - How to Help Improve the Query Builder

The Query Builder learns from your feedback! When the system gets something wrong, you can tell us and we'll fix it automatically.

## 🎯 Why Your Feedback Matters

Every correction you submit:
- ✅ Helps improve the system for everyone
- ✅ Gets analyzed for patterns (e.g., "sales" should mean "revenue")
- ✅ Can lead to automatic improvements
- ✅ Takes less than 30 seconds to submit

**Your feedback directly improves the system!**

---

## 📝 How to Submit Feedback

### Step 1: Ask a Question

Type your question and get results as usual:
```
"Show me revenue for last week"
```

### Step 2: Rate the Result

After the SQL query appears, you'll see two buttons:

- **👍 Correct** - Click if the query looks right
- **👎 Wrong** - Click if something is wrong

### Step 3: If Wrong, Tell Us Why

If you clicked 👎, a form will expand asking:

**1. What was wrong?**
- Wrong metrics (e.g., showed impressions instead of clicks)
- Wrong dimensions/columns (e.g., wrong table used)
- Wrong platform (Google Ads vs Microsoft Ads)
- Wrong date range (last week vs last month)
- Wrong filters (wrong campaign, account, etc.)
- Other

**2. What should it have been?** (optional)
Type a quick note explaining the issue:
- "Should use revenue metric, not impressions"
- "Date should be last month, not last week"
- "Need Google Ads data, not Microsoft"

**3. Submit**
Click "Submit Feedback" and you're done!

---

## 💡 Good Feedback Examples

### Example 1: Wrong Metric
```
❌ System showed: impressions
✅ Should have been: clicks
📝 Your note: "I asked for clicks but got impressions instead"
```

### Example 2: Wrong Date Range
```
❌ System showed: Last 7 days
✅ Should have been: Last month
📝 Your note: "Said 'last month' but got 'last week'"
```

### Example 3: Wrong Platform
```
❌ System showed: Google Ads data
✅ Should have been: Microsoft Ads data
📝 Your note: "Asked for Microsoft Ads specifically"
```

### Example 4: Synonym Not Recognized
```
❌ System didn't recognize: "sales"
✅ Should have mapped to: revenue metric
📝 Your note: "Sales and revenue are the same thing"
```

---

## 🔄 What Happens to Your Feedback

### Immediate
1. **Stored securely** - Your feedback is saved
2. **Thank you message** - You see "✅ Feedback submitted!"
3. **Pattern detection** - System analyzes for common issues

### Within a Week
1. **Admin reviews** - We check the feedback dashboard
2. **Patterns identified** - System finds: "5 users said 'sales' should be 'revenue'"
3. **Fix implemented** - We add the synonym or fix the issue
4. **Update pushed** - Improvement goes live

### Next Time You Use It
1. **Auto-update** - `start_app.bat` pulls the latest fixes
2. **Improved results** - Your issue is fixed!
3. **Everyone benefits** - All users get the improvement

---

## 📊 Types of Feedback We Track

### 1. Metric Mismatches
When the system uses the wrong metric or doesn't recognize a term:
- "Should use revenue, not cost"
- "ROI wasn't recognized"
- "Sales should map to revenue"

### 2. Dimension Issues
When the system picks the wrong table or column:
- "AccountName should use GoogleAdsAccount table"
- "CampaignName picked wrong source"

### 3. Platform Confusion
When Google Ads vs Microsoft Ads gets mixed up:
- "Asked for Google data, got Microsoft"
- "Need both platforms, not just one"

### 4. Date Range Problems
When date filters are wrong:
- "Last month parsed as last week"
- "Q1 2025 didn't work"
- "YTD is missing data"

### 5. Filter Issues
When WHERE clauses are wrong:
- "Campaign filter didn't apply"
- "State = Texas didn't work"
- "Campaign name contains 'Super Bowl' missed results"

### 6. Other
Anything else that's wrong or unexpected

---

## ⚡ Quick Tips

### DO:
- ✅ Submit feedback when something is clearly wrong
- ✅ Be specific in your notes ("should be X, not Y")
- ✅ Click 👍 when queries are correct (helps us know what works!)
- ✅ Submit feedback even for small issues

### DON'T:
- ❌ Worry about being too picky - we want all feedback!
- ❌ Hesitate because "someone else probably reported it" - we track frequency
- ❌ Skip the notes field - details help us fix issues faster
- ❌ Feel bad about reporting multiple issues - that's helpful!

---

## 🎓 Feedback Best Practices

### Be Specific
**Good:** "Asked for revenue, got cost instead"
**Less helpful:** "Wrong data"

### Explain the Intent
**Good:** "Wanted last month (Jan 2026), got last week"
**Less helpful:** "Date wrong"

### Mention Synonyms
**Good:** "Sales and revenue should mean the same thing"
**Less helpful:** "Didn't understand sales"

### One Issue Per Feedback
If multiple things are wrong, pick the main issue and note the others:
**Good:** "Wrong metric (revenue vs cost) - also wrong date range"

---

## 📈 Seeing Your Impact

Want to see how feedback improves the system?

### Option 1: Feedback Dashboard (Admin Only)
- Navigate to "Feedback Dashboard" in the sidebar
- See total feedback, patterns, and top issues
- Track improvements over time

### Option 2: Watch for Updates
- `start_app.bat` shows "Updating..." when fixes are available
- Check release notes in commit messages
- Try queries that previously failed

---

## ❓ FAQ

**Q: Is my feedback anonymous?**
A: Yes! We only store the query, what was wrong, and your notes. No personal info.

**Q: How long does it take to fix issues?**
A: Common patterns (5+ reports) are often fixed within a week. Unique issues may take longer.

**Q: What if I'm not sure what's wrong?**
A: That's okay! Use the "Other" category and describe what you expected vs what you got.

**Q: Can I see my past feedback?**
A: Currently no, but we're tracking all feedback and act on patterns.

**Q: Does clicking 👍 do anything?**
A: Yes! It tells us the query was correct, which helps validate our improvements.

**Q: Should I report every issue?**
A: Yes! Even if you think others reported it, more reports = higher priority fix.

**Q: What if the query is mostly right but has one small issue?**
A: Still report it! Use the notes to explain "Query is 90% right, but should use X instead of Y"

---

## 🙏 Thank You!

Your feedback makes this tool better for everyone. Every thumbs down helps us learn and improve.

**Keep the feedback coming!** 🚀

---

**Questions?** Contact Andrew Arnett:
- Email: aarnett@reddogmediainc.com
- Teams: @aarnett
- GitHub Issues: https://github.com/aarnett-rdm/nl-sql-query-builder/issues
