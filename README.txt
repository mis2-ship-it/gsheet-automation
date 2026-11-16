SALES: CAMPAIGN TAB (with your calendar)

WHAT IT SHOWS
  Sales > Campaign. Choose a discount code (or All campaigns), then Campaign Month A and Month B.
  - Each campaign day against the same weekday last week (the day 7 days earlier).
  - Each campaign month against the other days of that month. Every campaign day is left out of
    the other days, whichever discount code it has.
  - Three charts: each campaign day, Month A against Month B, and every campaign month's lift.
  - Insights written from the numbers, a table of the campaign days, and discount % on campaign days.
  Growth is green, a drop is red. The same filters as the other Sales tabs apply.

FILES
  data/campaign_days.csv          your calendar: 80 campaign days, Jan-25 to Nov-26, 3 discount codes
  scripts/build_site.py           reads the calendar (--campaigns) and writes data/campaigns.json
  site/campaign.js                the Campaign tab (loaded after trend.js)
  site/Index.html                 has the one line: <script src="campaign.js"></script>

EACH MONTH
  1. Add the new campaign days to data/campaign_days.csv in the repo, with the same headings:
       Month, Discount Code, Business Date   (dates as dd-mm-yyyy, e.g. 12-11-2026)
     The build accepts "Business Date" or "Date", and "Discount Code" or "Campaign".
  2. In the update workflow, add the flag to the build step:
       --campaigns data/campaign_days.csv
  3. Run Update dashboard. The log warns about any date it could not read, or any Month label
     that does not match its date.

CHECKS
  Dates after the last day of sales data show "No sales yet". Nov-26 campaign days from 15 Nov
  will show that until the sales arrive.
