#!/usr/bin/env python3
"""Adds the dashboard's page scripts: one script tag each, placed just before </body>, after the other Sales scripts.
Safe to run twice (a script already on the page is left as it is).

Usage (from the repo root):   python add_cancel_tab.py site/index.html cancel.js product.js
With no script names it adds cancel.js (the Cancellation tab)."""
import sys

DEFAULT = ["cancel.js"]
TAG = '<script src="cancel.js"></script>'


def tag_for(name):
    return f'<script src="{name}"></script>'


def add_tags(html, names):
    """(new html, [the names added]). Each tag goes before </body>; without </body>, at the end."""
    added = []
    for name in names:
        if f'src="{name}"' in html:
            continue
        tag = tag_for(name)
        i = html.rfind("</body>")
        html = html + "\n" + tag + "\n" if i == -1 else html[:i] + tag + "\n" + html[i:]
        added.append(name)
    return html, added


def add_tag(html):
    """The Cancellation tag only: (new html, changed)."""
    new, added = add_tags(html, DEFAULT)
    return new, bool(added)


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "site/index.html"
    names = sys.argv[2:] or DEFAULT
    with open(path, encoding="utf-8") as f:
        html = f.read()
    new, added = add_tags(html, names)
    if added:
        with open(path, "w", encoding="utf-8") as f:
            f.write(new)
        print(f"added {', '.join(tag_for(n) for n in added)} to {path}")
    else:
        print(f"already in {path}: {', '.join(names)}")
