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
  Each table has a Region wise section (one row per region), then a Region + store section.
  A Region wise summary at the top shows the key figure from each table, one row per region.
  The Sales filters above the tables apply to every table.
  Download Excel: a Summary sheet first, then one sheet per table, with the same two sections.

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

SAME DAY (YoY and MoM)
  Same day = the same weekday in the same week of the month. Weeks run Monday to Sunday, and week 1 is the
  week that holds the 1st. Example: Thu 19 Nov 2026 (week 4) compares with Thu 20 Nov 2025 (last year) and
  Thu 22 Oct 2026 (last month).
  Used for: the Morning Call same-day MoM and YoY, and the MoM and YoY columns of the area manager, top stores
  and daily/weekly tables. Month to date and YTD still compare calendar dates.
  Files changed: site/morning.js and site/insights.js (both are in this zip).

EXCEL COLOURS
  Growth figures in the Excel file are green when up (or flat) and red when down, in the same colours as the dashboard.
