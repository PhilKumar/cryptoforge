# Embedded faces

CryptoForge's own type, as woff2 inside `@font-face` data URIs. Generated, not
hand-written — regenerate rather than edit.

They are here because the buyer's console must fetch nothing off the machine.
The parent loads these from Google Fonts; a `<link>` to a CDN in a page served
by the executor would tell a font host every time a buyer opened their trading
console, and would break the moment they were offline. Same faces, same names,
no request.

`core.css` is linked in the page head. The other six are the appearance
presets and load only when one is chosen, which is what the parent's boot
script does with its CDN URLs.

| file | families |
|---|---|
| `core` | Sora, Rajdhani, Azeret Mono — the terminal's default trio |
| `institutional` | IBM Plex Sans, IBM Plex Mono |
| `swiss` | Inter, Fira Code |
| `grotesk` | Manrope, Archivo, Fira Code |
| `editorial` | Source Sans 3, Newsreader, Source Code Pro |
| `techno` | Chakra Petch, Share Tech Mono |
| `humanist` | Nunito Sans, Bitter, Martian Mono |

Latin subset only, and only the weights the stylesheet asks for — the full
set would be several megabytes for glyphs no page here renders.

All of these are SIL Open Font License 1.1, which permits embedding.

To regenerate, fetch `https://fonts.googleapis.com/css2?family=…&display=swap`
with a browser User-Agent (that is what makes Google serve woff2 rather than
ttf), keep the blocks under the `/* latin */` comment, download each `url(…)`
and substitute a `data:font/woff2;base64,…` URI in its place.
