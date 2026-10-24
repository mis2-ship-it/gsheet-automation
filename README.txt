TARGET PLANNER - UPDATE NOTES

1. Growth
   - Growth % blank (automatic): target = projected sales of the month before the target month x (1 + last year's MoM %).
     Last year's MoM % = compare month / the month before it, per store. For example, Nov 2025 / Oct 2025.
     Each store uses its own MoM. A store is used only if it traded from before the start of the earlier month.
     A new store uses its region's MoM, or the overall MoM if the region has none.
     For Nov 2026, the base is Oct 2026. Until October ends, Oct 2026 is projected: sales so far / days so far x days in month.
   - Growth % entered: target = last year's compare month x (1 + Growth %). Nothing else changes.

2. Days and average day sales
   - The top of the page shows the days of each weekday (Mon to Sun) in the compare month and the target month.
   - Average day sales: last year's actual average per weekday, and the projected average per weekday for the target month.

3. Phasing
   - Default: each weekday's average last year (Mon, Tue, ... Sun). Other choices: Mon-Thu / Fri-Sun averages, or same date last year.

4. Store type
   - COCO / FOFO / Unmapped filter. The store list follows the chosen type.

5. Setup stays the same as before: TARGET_SHEET_ID at the top of Code.gs; Index as an HTML file;
   deploy as a web app (Deploy -> Manage deployments -> edit -> New version, so the link keeps working).
