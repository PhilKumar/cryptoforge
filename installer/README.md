# Building the buyer's app

The buyer double-clicks one thing and never opens a terminal. This folder is
what turns `executor/` into that.

```
python -m installer.make_icons build/icons
pyinstaller installer/cascade.spec --noconfirm --distpath build/dist --workpath build/work
```

On a Mac that produces `build/dist/Cascade.app` (~49 MB). On Windows,
`build/dist/Cascade/Cascade.exe`. Tag `executor-v*` and
`.github/workflows/release-executor.yml` builds both and drafts a release —
PyInstaller cannot cross-compile, so each platform needs its own runner.

## What the app does differently from `python -m executor`

Nothing, except the three things a double-clicked program cannot be told.
`installer/app_entry.py` supplies `--tray` when no arguments are given, sends
output to `~/.cascade-executor/cascade.log` because there is no terminal for it
to land in, and shows a native dialog if it dies before the tray exists — the
alternative being an icon the buyer clicked that silently never appeared.

Arguments still pass through, so support can say "run `Cascade --check` from
Terminal" and it works.

## The folder is called `installer`, not `packaging`

`packaging` is a real library that ships with setuptools, and a directory of
that name shadows it — `python -m packaging.make_icons` resolves to the
installed package and fails with a confusing `No module named`.

## One-folder, not one-file

A one-file build unpacks itself to a fresh temp directory on every launch. That
costs seconds on a cold disk, and it makes `os.path.dirname(__file__)` a path
that changes between runs — which `executor/ui.py` uses to find its fonts.

## Signing: not done, and the item that must be paid for

The workflow's signing steps are written and skip themselves until the
certificates exist. Until then macOS says *"cannot be opened because the
developer cannot be verified"* and Windows shows SmartScreen — thirty seconds
before the app asks for an exchange API key, which is the worst possible moment
to look untrustworthy.

- **macOS**: Apple Developer Program, $99/yr. Needs `MACOS_CERT_P12` (base64),
  `MACOS_CERT_PASSWORD`, `MACOS_SIGN_IDENTITY`, and for notarisation
  `APPLE_ID`, `APPLE_TEAM_ID`, `APPLE_APP_PASSWORD`. Notarised **and stapled**,
  or the first launch needs a network round trip to Apple.
- **Windows**: an OV certificate, ~$200–400/yr, as `WINDOWS_CERT_PFX` (base64)
  and `WINDOWS_CERT_PASSWORD`. OV still accrues SmartScreen reputation over the
  first few hundred installs; EV skips that and costs more.

## Still missing

- **An updater.** There is no version handshake and no update check anywhere in
  `executor/`, and a packaged buyer cannot even replace a folder. Every fix
  shipped to `main` stops at their machine.
- **A login item**, so it survives a reboot.
- **Windows verification.** Everything here has been built and run on macOS
  only. The spec's Windows branches are written but unexercised.
