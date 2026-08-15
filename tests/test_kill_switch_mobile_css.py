"""The kill switch must stay a button when it moves to the bottom on a phone.

The desktop rule pins the button with `position: fixed; top: 20px`. The mobile
override moved it to the bottom by adding `bottom: 16px` — and left `top` in
place. A fixed box given BOTH edges and no height does not pick one: it
stretches to span them. The button became a 816px red slab down the middle of
a 852px phone screen, with `align-items: center` parking "KILL ALL" halfway
down it, covering the Cascade panel underneath.

Measured on a 393x852 viewport, before and after:

    before   x=131 y=20  w=131 h=816   (95.8% of the viewport)
    after    x=131 y=785 w=131 h=51

The rule this pins is the general one, because the specific bug is a special
case of it and the next person to reposition this button will hit the same
edge: whenever a `.kill-switch` rule sets one vertical edge, it must settle the
other. Asserting only `top: auto` would pass a future rewrite that switched to
`inset` or moved the button back to the top.
"""

import os
import re
import unittest

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_APP_CSS = os.path.join(_HERE, "static", "cryptoforge-app.css")


def _blocks_for(css: str, selector: str) -> list[tuple[str, str]]:
    """Every declaration block whose selector list mentions `selector`.

    Returns (context, body) pairs, where context names the enclosing @media so
    a failure says which breakpoint is wrong rather than just "a rule".
    """
    found: list[tuple[str, str]] = []
    media_stack: list[str] = []
    depth_stack: list[int] = []
    i = 0
    while i < len(css):
        brace = css.find("{", i)
        if brace == -1:
            break
        prelude = css[i:brace].strip().splitlines()
        head = prelude[-1].strip() if prelude else ""

        close = brace + 1
        depth = 1
        while close < len(css) and depth:
            if css[close] == "{":
                depth += 1
            elif css[close] == "}":
                depth -= 1
            close += 1
        body = css[brace + 1 : close - 1]

        if head.startswith("@media"):
            media_stack.append(head)
            depth_stack.append(close)
            i = brace + 1  # descend into the media block
            continue

        while depth_stack and brace > depth_stack[-1]:
            media_stack.pop()
            depth_stack.pop()

        if selector in head:
            found.append((media_stack[-1] if media_stack else "(top level)", body))
        i = close
    return found


class KillSwitchMobileGeometryTests(unittest.TestCase):
    def setUp(self):
        with open(_APP_CSS, encoding="utf-8") as handle:
            self.css = handle.read()
        self.blocks = _blocks_for(self.css, ".kill-switch")

    def test_the_stylesheet_still_has_kill_switch_rules(self):
        """Guards the parser: a silent zero-match would make every other test
        in this file pass for the wrong reason."""
        self.assertTrue(self.blocks, "no .kill-switch rules found — has the selector been renamed?")

    def test_no_rule_sets_one_vertical_edge_and_leaves_the_other_dangling(self):
        """top + bottom on a fixed box with no height means "stretch"."""
        for context, body in self.blocks:
            declarations = {
                name.strip(): value.strip()
                for name, _, value in (d.partition(":") for d in body.split(";"))
                if name.strip()
            }
            sets_top = "top" in declarations or "inset" in declarations
            sets_bottom = "bottom" in declarations or "inset" in declarations
            if sets_bottom and not sets_top:
                self.fail(
                    f"{context}: .kill-switch sets bottom={declarations.get('bottom')!r} "
                    "without settling `top`. The desktop rule pins top:20px, so this "
                    "stretches the button to full viewport height. Add `top: auto`."
                )

    def test_the_mobile_rule_releases_the_top_edge(self):
        """The specific regression, named so a failure is self-explaining."""
        mobile = [(ctx, body) for ctx, body in self.blocks if "max-width" in ctx and "bottom" in body]
        self.assertTrue(mobile, "expected a mobile .kill-switch rule that repositions to the bottom")
        for context, body in mobile:
            self.assertRegex(
                re.sub(r"\s+", " ", body),
                r"top\s*:\s*auto",
                f"{context}: mobile kill switch must release top, or it spans the whole screen",
            )

    def test_the_desktop_rule_still_pins_the_top(self):
        """If this ever stops being true the override above is dead weight, and
        the reason for `top: auto` would be invisible to the next reader."""
        desktop = [body for ctx, body in self.blocks if ctx == "(top level)"]
        self.assertTrue(
            any(re.search(r"top\s*:\s*\d", body) for body in desktop),
            "desktop .kill-switch no longer pins `top` — revisit the mobile override",
        )


if __name__ == "__main__":
    unittest.main()
