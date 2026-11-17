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
