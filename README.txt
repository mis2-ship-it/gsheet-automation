DISCOUNT DASHBOARD - SETUP

1. Google Sheet for the dashboard data
   - Create a new Google Sheet named "Discount Dashboard Data".
   - Share it with rista-sheet@grn-project-487012.iam.gserviceaccount.com as Editor.
   - Copy its ID: the text between /d/ and /edit in the sheet's address.

2. GitHub
   - Repo -> Settings -> Secrets and variables -> Actions -> New repository secret.
     Name: DISCOUNT_SHEET_ID    Value: the sheet ID from step 1.
   - Upload write_discount_sheet.py to the TOP of the repo.
   - Upload .github/workflows/discounts.yml (replaces the existing file).
   - Actions -> Discount Code rows (Swiggy and Zomato) -> Run workflow.
     The last step should show: "Disc_Label: N rows written" and "Disc_Item: N rows written".

3. Apps Script (script.google.com)
   - New project. Rename it "Discount Dashboard".
   - Replace the contents of Code.gs with discount-dashboard/Code.gs.
   - Set DISCOUNT_SHEET_ID at the top of Code.gs to the sheet ID from step 1.
   - File -> New -> HTML file. Name it exactly: Index. Replace its contents with discount-dashboard/Index.html.
   - Save (Ctrl+S).

4. Deploy
   - Deploy -> New deployment -> type: Web app.
     Execute as: Me.   Who has access: Anyone within your organisation (or only specific people).
   - Authorise the access it asks for, then open the web app address.

Notes
- Periods: Daily = that day against the day before. Weekly = the 7 days ending on the date, against the 7 days before.
  Monthly = the calendar month against the month before. Month to date = the 1st to the date, against the same days last month.
- The sheet keeps the last 400 days (write_discount_sheet.py --keep-days). Older days are dropped automatically.
- Discount rows are pulled at 07:45 IST each day, so the dashboard shows the previous completed day in the morning.
