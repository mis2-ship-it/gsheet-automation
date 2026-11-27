"""add_cancel_tab.py: the tag goes before </body>, once. Run from the repo root: python tests/test_add_cancel_tab.py"""
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
check("the tag is added once", changed and new.count(act.TAG) == 1)
check("the tag comes after the other Sales scripts, before </body>",
      new.index(act.TAG) > new.index('metrics.js') and new.index(act.TAG) < new.index('</body>'))
again, changed2 = act.add_tag(new)
check("a second run changes nothing", not changed2 and again == new)
no_body = '<html><script src="metrics.js"></script>'
nb, changed3 = act.add_tag(no_body)
check("a page without </body> gets the tag at the end", changed3 and nb.rstrip().endswith(act.TAG))
print(f"{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
