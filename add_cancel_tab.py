#!/usr/bin/env python3
"""Adds the Cancellation tab to the dashboard page: one script tag for cancel.js, placed just before </body>, after the
other Sales scripts. Safe to run twice (it does nothing when the tag is already there).

Usage (from the repo root):   python add_cancel_tab.py site/index.html
Then check that site/cancel.js is in the site folder, and deploy as usual."""
import sys

TAG = '<script src="cancel.js"></script>'


def add_tag(html):
    """(new html, changed). The tag goes before </body>; without </body>, at the end."""
    if 'src="cancel.js"' in html:
        return html, False
    i = html.rfind("</body>")
    if i == -1:
        return html + "\n" + TAG + "\n", True
    return html[:i] + TAG + "\n" + html[i:], True


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "site/index.html"
    with open(path, encoding="utf-8") as f:
        html = f.read()
    new, changed = add_tag(html)
    if changed:
        with open(path, "w", encoding="utf-8") as f:
            f.write(new)
        print(f"added {TAG} to {path}")
    else:
        print(f"{TAG} is already in {path}")
