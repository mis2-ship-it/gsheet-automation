"""add_cancel_tab.py: each script tag goes before </body>, once, for each script. Run from the repo root."""
import os, sys
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import add_cancel_tab as act

PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    PASS, FAIL = (PASS + 1, FAIL) if cond else (PASS, FAIL + 1)
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))


page = '<html><body>\n<script src="am.js"></script>\n<script src="metrics.js"></script>\n</body></html>\n'
new, changed = act.add_tag(page)
check("the Cancellation tag is added once", changed and new.count(act.TAG) == 1)
check("it comes after the other Sales scripts, before </body>",
      new.index(act.TAG) > new.index("metrics.js") and new.index(act.TAG) < new.index("</body>"))
again, changed2 = act.add_tag(new)
check("a second run changes nothing", not changed2 and again == new)
nb, changed3 = act.add_tag('<html><script src="metrics.js"></script>')
check("a page without </body> gets the tag at the end", changed3 and nb.rstrip().endswith(act.TAG))
both, added = act.add_tags(new, ["cancel.js", "product.js"])
check("product.js is added, cancel.js is not added twice", added == ["product.js"] and both.count('src="cancel.js"') == 1 and both.count('src="product.js"') == 1)
check("product.js is before </body>, after the other scripts", both.index('src="product.js"') < both.index("</body>") and both.index('src="product.js"') > both.index("metrics.js"))
print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
