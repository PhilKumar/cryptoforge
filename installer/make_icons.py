"""
installer/make_icons.py — the app icon, from the one we already draw.

`executor/pwa.icon` renders the three ascending bars for the web console. The
installer needs the same mark as a macOS `.icns` and a Windows `.ico`, and
drawing it a second time by hand is how the menu bar and the Dock end up
showing two slightly different logos.

Run from the repo root:

    python -m installer.make_icons build/icons
"""

from __future__ import annotations

import io
import os
import subprocess  # nosec B404 - one fixed argv below, never a shell
import sys

from executor import pwa

# macOS wants every size in the iconset or `iconutil` refuses the folder.
ICNS_SIZES = ((16, 1), (16, 2), (32, 1), (32, 2), (128, 1), (128, 2), (256, 1), (256, 2), (512, 1), (512, 2))
ICO_SIZES = (16, 24, 32, 48, 64, 128, 256)


def write_png_set(out_dir: str) -> str:
    """The `.iconset` folder macOS builds an `.icns` from."""
    iconset = os.path.join(out_dir, "cascade.iconset")
    os.makedirs(iconset, exist_ok=True)
    for base, scale in ICNS_SIZES:
        name = f"icon_{base}x{base}{'@2x' if scale == 2 else ''}.png"
        with open(os.path.join(iconset, name), "wb") as handle:
            handle.write(pwa.icon(base * scale))
    return iconset


def build_icns(out_dir: str) -> str:
    iconset = write_png_set(out_dir)
    icns = os.path.join(out_dir, "cascade.icns")
    # Absolute path and a literal argv: this runs on a build machine whose
    # PATH is whatever the workflow left behind.
    subprocess.run(  # nosec B603 - argv is a literal list, shell=False
        ["/usr/bin/iconutil", "-c", "icns", iconset, "-o", icns], check=True
    )
    return icns


def build_ico(out_dir: str) -> str:
    """Pillow does the container; the pixels are still ours."""
    from PIL import Image

    os.makedirs(out_dir, exist_ok=True)
    ico = os.path.join(out_dir, "cascade.ico")
    largest = Image.open(io.BytesIO(pwa.icon(max(ICO_SIZES)))).convert("RGBA")
    largest.save(ico, format="ICO", sizes=[(s, s) for s in ICO_SIZES])
    return ico


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    out_dir = argv[0] if argv else "build/icons"
    os.makedirs(out_dir, exist_ok=True)
    made = []
    if sys.platform == "darwin":
        made.append(build_icns(out_dir))
    else:
        # The iconset is still worth writing anywhere: it is the input a Mac
        # runner turns into an .icns, and it costs nothing to produce.
        made.append(write_png_set(out_dir))
    try:
        made.append(build_ico(out_dir))
    except ImportError:
        print("Pillow is not installed, so no .ico was written.", file=sys.stderr)
    for path in made:
        print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
