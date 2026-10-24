TARGET PLANNER - SETUP

What it does: plans a month's sales target from the same month last year, for overall, brand, region and store,
with a day-by-day phasing for the target month. Filters: compare month, channel (Overall / Offline / Online),
brand, region, store, growth %, phasing method.

1. Google Sheet for the planning data
   - Create a new Google Sheet named "Target Planner Data".
   - Copy its ID: the long text between /d/ and /edit in the sheet's address.
   - Share the sheet with rista-sheet@grn-project-487012.iam.gserviceaccount.com as Editor.

2. GitHub
   - Repo -> Settings -> Secrets and variables -> Actions -> New repository secret.
     Name: TARGET_SHEET_ID   Value: the sheet ID from step 1.
   - Upload build_target_base.py to the TOP of the repo, and .github/workflows/target-base.yml into .github/workflows/.
   - Actions -> Target base (Target Planner data) -> Run workflow.
     The log should end with: "Target_Base: N rows written to the sheet".

3. Apps Script (script.google.com)
   - New project. Rename it "Target Planner".
   - Replace the contents of Code.gs with target-planner/Code.gs.
   - Set TARGET_SHEET_ID at the top of Code.gs to the sheet ID from step 1.
   - File -> New -> HTML file. Name it exactly: Index. Replace its contents with target-planner/Index.html.
   - Save (Ctrl+S).

4. Run it
   - Click Deploy -> New deployment -> type: Web app.
     Execute as: Me.   Who has access: Anyone within your organisation (or only specific people).
   - Authorise the access it asks for, then open the web app address.
   - Choose Target month Nov 2026, Compare with Nov 2025, and press Calculate.

Notes
- Like-for-like growth: a store counts only if it sold in all six comparison months (the latest 3 months and the same 3 months
  last year). Stores new in that period use their region's like-for-like growth, or the overall figure if the region has none.
- The sheet holds sales from August 2025 onward. Each calculation reads the whole tab, so it can take several seconds.
- Online = Swiggy, Zomato, Ownly, Others and anything else that is not In Store. Flipkart is counted as Online.
- Phasing for a store, brand or region uses that selection's own last-year pattern, so the store phasings do not add up to the overall phasing.
