/* ============ Daily date picker ============
   A calendar on the Daily tab's date bar. Picking a date selects that day, the same as the arrows do.
   Dates outside the data are moved to the nearest day that has data. Loaded last, after am.js. */
const dayIso = i => toDate(i).toISOString().slice(0, 10);

function addDayPicker() {
  const onDaily = (S.top === "sales" && S.sub === "daily") || (S.top === "rating" && S.rkind === "daily");
  if (!onDaily || !PERIOD || PERIOD.key === null || !PERIOD.keys || !PERIOD.keys.length) return;
  const bar = document.querySelector("#periodwrap .periodbar");
  if (!bar || bar.querySelector(".daypick")) return;
  const input = document.createElement("input");
  input.type = "date";
  input.className = "daypick";
  input.dataset.pickday = "1";
  input.min = dayIso(PERIOD.keys[0]);
  input.max = dayIso(PERIOD.keys[PERIOD.keys.length - 1]);
  input.value = dayIso(PERIOD.key);
  input.setAttribute("aria-label", "Choose a date");
  input.style.cssText = "color-scheme:dark;background:#121C33;color:#E5ECF8;border:1px solid #24314f;" +
    "border-radius:8px;padding:6px 8px;font:inherit;min-height:36px;";
  bar.insertBefore(input, bar.querySelector(".latest"));
}

const renderPeriodBarPick = renderPeriodBar;
renderPeriodBar = function () {
  renderPeriodBarPick();
  addDayPicker();
};

document.addEventListener("change", e => {
  if (!e.target.dataset || !e.target.dataset.pickday || !e.target.value || !PERIOD || !PERIOD.keys) return;
  const first = PERIOD.keys[0], last = PERIOD.keys[PERIOD.keys.length - 1];
  let i = Math.round((Date.parse(e.target.value + "T00:00:00Z") - EPOCH) / DAY);
  i = Math.min(Math.max(i, first), last);
  S.sel = i;
  render();
});
