TARGET PLANNER - UPDATE NOTES

1. Base month (projected to month end)
   - The target starts from the base month (the month before the target month), projected to the end of that month:
     sales so far / days so far x days in the month. A store that opened during the base month is projected from its own first day.
   - So a plan made on the 28th or 29th uses the projected full month. The page shows the actual sales to date and the days used.

2. Outlets
   - Every store that traded in the compare month is listed, even if it is not trading now. Such a store shows
     "Not trading now" and has no target.
   - The top of the page shows outlets last year, operating now (= target outlets), not trading now and new stores.
   - The brand and region tables show outlets last year and outlets now for each brand and region.

3. Automatic growth
   - "Automatic growth from" chooses the growth basis:
       Each store's own MoM        - last year's MoM % for each store (default)
       Region MoM                  - last year's MoM % for the region, the same for all its stores
       Overall MoM                 - last year's overall MoM %, the same for all stores
   - A store's MoM counts only if the store traded in both months and was open before the earlier month.
     A store without a valid MoM uses its region's MoM, then the overall MoM.

4. Growth typed for a store
   - In the store table, type a growth % in "Growth typed" and press Calculate. Leave it blank for automatic.
   - A typed store growth replaces that store's automatic growth, and keeps every other store unchanged.

5. Growth % at the top (unchanged)
   - Blank = automatic (the base projection). Entered = last year's compare month x (1 + Growth %), for every store
     without its own typed growth.

6. Days and average day sales
   - Days of each weekday in the compare month and the target month, and the average day sales per weekday.

7. Store type
   - COCO / FOFO / Unmapped filter. The store list follows the chosen type.

Setup stays the same: TARGET_SHEET_ID at the top of Code.gs; Index as an HTML file;
deploy as a web app (Deploy -> Manage deployments -> edit -> New version).
