# PyInstaller spec for the Cascade executor — one spec, both platforms.
#
#     pyinstaller installer/cascade.spec --noconfirm
#
# One-FOLDER rather than one-file, deliberately. A one-file build unpacks
# itself into a temp directory on every launch, which costs seconds on a cold
# disk and — more to the point — makes `os.path.dirname(__file__)` a path that
# changes between runs. `executor/ui.py` resolves its fonts that way, and the
# recovery design leans on state living somewhere stable.
#
# `console=False`: there is no terminal in a double-clicked app, so a console
# window would only ever be an empty black rectangle the buyer has to close.
# Everything that would have gone to it goes to the log file that
# `installer/app_entry.py` opens first.

import os
import sys

from PyInstaller.utils.hooks import collect_submodules

REPO = os.path.abspath(os.getcwd())
ICON_DIR = os.path.join(REPO, "build", "icons")

# The woff2 faces the console serves at /assets/fonts. Bundled rather than
# fetched: the whole point of that directory is a page that works with no
# network and no CDN, which is also what makes the app self-contained.
datas = [(os.path.join(REPO, "executor", "assets"), os.path.join("executor", "assets"))]

# keyring reaches its platform backend by name at runtime, so the analysis
# cannot see it. Missing it produces an app that silently has nowhere to put an
# API key — the one failure `executor/secrets.py` refuses to paper over.
hiddenimports = collect_submodules("keyring.backends") + [
    "pystray._darwin" if sys.platform == "darwin" else "pystray._win32",
]

a = Analysis(
    [os.path.join(REPO, "installer", "app_entry.py")],
    pathex=[REPO],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    # Nothing on this list is imported by `executor/`, and each one drags in
    # tens of megabytes. The buyer's download is a trust signal: a 400 MB
    # download to place spot orders invites the question of what else is in it.
    excludes=["tkinter", "numpy", "pandas", "matplotlib", "scipy", "PySide6", "PyQt5", "IPython"],
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="Cascade",
    debug=False,
    strip=False,
    upx=False,  # UPX-packed binaries trip antivirus heuristics; not worth it here
    console=False,
    icon=os.path.join(ICON_DIR, "cascade.ico") if sys.platform == "win32" else None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    name="Cascade",
)

if sys.platform == "darwin":
    app = BUNDLE(
        coll,
        name="Cascade.app",
        icon=os.path.join(ICON_DIR, "cascade.icns"),
        bundle_identifier="in.philforge.cascade",
        info_plist={
            # It lives in the menu bar. Without this it also takes a Dock icon
            # and a menu bar of its own, which is a lot of furniture for a
            # program whose entire interface is one small icon.
            "LSUIElement": True,
            "CFBundleName": "Cascade",
            "CFBundleDisplayName": "Cascade",
            "NSHighResolutionCapable": True,
            # Stops macOS offering to "reopen windows" for an app that has none.
            "NSSupportsAutomaticTermination": False,
            "NSSupportsSuddenTermination": False,
        },
    )
