#!/usr/bin/env python3
"""Quick JS integrity check for strategy.html."""

import re

with open("strategy.html", "r") as f:
    html = f.read()

# Find all script blocks
scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL)
combined = "\n".join(scripts)

# All function calls via onclick/onchange
onclick_fns = set(re.findall(r'onclick="([a-zA-Z_][a-zA-Z0-9_]*)\(', html))
onchange_fns = set(re.findall(r'onchange="([a-zA-Z_][a-zA-Z0-9_]*)\(', html))
all_ui_fns = onclick_fns | onchange_fns

# Defined functions
defined_fns = set(re.findall(r"(?:function|async function)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(", combined))
const_fns = set(
    re.findall(r"(?:const|let|var)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(?:async\s+)?(?:function|\()", combined)
)
defined_fns |= const_fns

missing = all_ui_fns - defined_fns
if missing:
    print("UI functions referenced but NOT defined in JS:")
    for fn in sorted(missing):
        print(f"  - {fn}")
else:
    print("All UI onclick/onchange functions have definitions in the JS.")

# Codex new functions
new_fns = [
    "_applyBrokerState",
    "refreshBrokerState",
    "disconnectBroker",
    "toggleBrokerConnection",
    "_safeDomId",
    "_setTablePage",
    "_renderTablePager",
    "_resultsUsesTradeView",
    "_renderResultsHead",
    "_flattenRunTrades",
    "_buildRunTradeRows",
    "emergencyStop",
]
for fn in new_fns:
    status = "OK" if fn in defined_fns else "MISSING!"
    print(f"  {status}: {fn}")

# Bracket/brace balance check
opens = combined.count("{") + combined.count("(") + combined.count("[")
closes = combined.count("}") + combined.count(")") + combined.count("]")
print(f"\nBracket balance: opens={opens}, closes={closes}, delta={opens - closes}")
if opens != closes:
    print("  WARNING: Bracket mismatch detected!")
else:
    print("  Brackets balanced.")
