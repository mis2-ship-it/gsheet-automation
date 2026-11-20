SALES: MORNING CALL SUB-TAB

WHAT IT SHOWS
  Sales > Morning Call. Region-wise, store-wise tables for the latest day of data:
  - MoM (MTD): month to date (1st to the reference day) against the same days last month.
    Shows the full last month as a reference, and the trend for the month.
    Trend = month to date ÷ days elapsed × balance days + month to date.
  - YoY (same day and YTD): the same day last year against the reference day, and year to date.
  - WoW (same weekday): the reference day against the same weekday a week earlier.
  - FTD vs Pre FTD: the reference day against the day before.
  - Late Night sale (0–5 AM): needs the hourly data, which covers the last 14 business days.
  Stores are listed under their region, with the region totals on the region row. The Sales filters
  above the table apply to every table.
  Download Excel: one sheet per table, in the same layout.

FILES
  site/morning.js       new, loaded after insights.js
  site/index.html       adds one line: <script src="morning.js"></script> after insights.js
  site/insights.js      needed by the Morning Call (the store data and filters)
  site/campaign.js      unchanged

DEPLOY
  Upload site/morning.js and site/index.html next to the page, then hard-refresh.

NOTES
  The reference day is the latest day of data.
  Late Night shows — for days outside the hourly data.
