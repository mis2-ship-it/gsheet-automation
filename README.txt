SALES: CAMPAIGN TOTALS, TOP AND BOTTOM STORES, AREA MANAGERS

CAMPAIGN TAB
  Top tiles: total campaign-day sales, average per campaign day, and growth on the last campaign
  (Month A's average against Month B).
  Each campaign day now shows the previous campaign day's sales and the growth against it.
  The table ends with a Total row.

TOP AND BOTTOM STORES, AREA MANAGERS (Daily, Weekly and Monthly tabs)
  Placed straight after the session table.
  Top and bottom stores, with WoW, MoM and YoY (on the Monthly tab, WoW is blank and ranking uses MoM).
  Area manager table: click a manager to open its stores; click a store to see it by brand and source.
  An older "By area manager" table on the page is removed, so only one table shows.

FILES
  site/campaign.js       updated
  site/insights.js       new, loaded last
  site/Index.html        adds one line: <script src="insights.js"></script> after campaign.js
  scripts/build_site.py  unchanged in this round
  data/campaign_days.csv unchanged in this round

DEPLOY
  Upload site/campaign.js, site/insights.js and site/Index.html next to the page, then hard-refresh.

CHECK THE SALES MODULE IS RUNNING (browser console, F12, on the Daily tab)
  typeof FB_INS          -> "object" means insights.js is running. "undefined" means it is not loaded.
  FB_INS.INS             -> loaded: true once the store files are in. error: shows any problem.
  M.stores.length        -> the number of stores in the data (should be more than 0).
  The Network tab should show insights.js with status 200.

REGION AND STORE PERFORMANCE (Daily, Weekly and Monthly tabs, after the area manager table)
  Stores are grouped by region. Each store shows its sales, then for WoW, MoM and YoY the comparison
  sales and the growth. Each region has a total row, and there is an All regions row at the end.
  The filter on top shows All, or only WoW, MoM or YoY.
  On the Monthly tab, WoW shows a dash (there is no week to compare).

REGION TABLE: MONTH TO DATE FOR MoM AND YoY
  WoW: the selected day (or week, or month) against the same period a week earlier.
  MoM and YoY: month to date, the 1st of the month to the selected day, against the same days last month and last year.
  MTD sales is shown as the base for MoM and YoY. On the Weekly tab the reference day is the end of the week;
  on the Monthly tab it is the end of the month, or the last day of data if the month is still running.

AREA MANAGER TABS (Daily, Weekly and Monthly tabs)
  All, Offline (In Store) and Online (every other source). The table shows the chosen channel.
  Top stores, the region table and the store window are not affected by the tabs.

SPEED
  The store files load as soon as the page opens, several at a time, and the block shows progress
  (for example, 24 of 60 stores). Each store is calculated once per redraw.
