# Dōjima — Film Prompt Pack

Turns the five landing plates into moving footage. Written for OpenArt Lite
(image-to-video); the prompts are model-agnostic and work on Kling / Veo /
Seedance / Runway.

Source plates: `media/landing/0{1..5}_*.png` (2816×1536, ~8 MB each)
Beat timings: taken from `media/youtube-pilot/narration-human-ready.txt`,
which is already cut to these same five images.

---

## The one decision: photoreal or painted

The plates are **painterly illustration**, not photographs. Image-to-video
preserves the source look — animating them directly gives you a moving
painting, not a movie.

You asked for photoreal. That needs **two stages**:

| Stage | What it does | Skip it if… |
| --- | --- | --- |
| **1 — Restyle** | image-to-image, plate → photoreal still | you want to keep the illustrated identity |
| **2 — Animate** | image-to-video off the stage-1 still | — |

Both prompts are given per plate below. Run stage 1 on all five first and
look at them side by side before animating anything — photoreal will change
the brand feel of the landing considerably, and it is cheaper to find that
out on five stills than on five clips.

**Style lock:** whichever way you go, the five shots must match each other.
Generate all five in one sitting, same model, same settings.

---

## Shot 1 — The port

**Plate:** `01_establishing.png` · **Beat:** 0:00–0:15 · **Length:** 8s

Wide elevated view of the Dōjima rice market at golden hour. Canals, laden
boats, stone quays, tiled warehouse roofs to the horizon, two arched bridges,
~30 figures in indigo.

This is the "action oriented market / port" shot — the whole frame should be
working.

**Stage 1 — photoreal restyle**
```
Photorealistic cinematic film still of a 1730s Osaka rice port at golden
hour. Aerial three-quarter view over a canal district: wooden cargo boats
stacked with straw rice bales, stone-faced quays, tiled warehouse roofs
receding to a hazy horizon, two arched timber bridges, dozens of merchants
and porters in indigo work robes. Warm low sun, long shadows, haze in the
distance, teal canal water. Shot on 35mm anamorphic, shallow depth at the
edges, natural colour grade. No illustration, no anime, no painterly texture.
```

**Stage 2 — motion**
```
Slow cinematic push-in over the canal, camera drifting forward and settling.
Loaded boats glide along the water leaving wakes. Porters shoulder rice bales
down the quay steps and stack them. Merchants cross the arched bridges.
Canal surface ripples and catches the low sun. Smoke rises from two rooftops.
Distant haze drifts. Sustained natural motion throughout — the port is busy,
never still. Locked style, consistent lighting.
```
**Negative:** `static frame, frozen figures, morphing bodies, warping
architecture, text, watermark, style shift mid-shot`

---

## Shot 2 — The merchant's hand

**Plate:** `02_merchant.png` · **Beat:** 0:15–0:31 · **Length:** 6s

Elderly merchant at dusk, grey topknot, indigo striped kimono, brush in his
right hand, reading a wooden price board of kanji and figures. Paper lantern
with a live flame.

This is the first of the two **hand-stroke** shots. Keep the stroke *gestural*
— see the warning below.

**Stage 1 — photoreal restyle**
```
Photorealistic cinematic portrait of an elderly Japanese rice merchant at
dusk, weathered face, grey hair in a topknot, indigo striped kimono, holding
a writing brush. He stands before a tall wooden price board covered in
brushed characters. A lit paper lantern hangs beside him, warm flame visible
through the paper. Narrow street receding behind, lanterns fading into blue
dusk. Practical firelight only, deep shadows, 85mm lens, shallow depth of
field. No illustration, no anime, no painterly texture.
```

**Stage 2 — motion**
```
Slow dolly-in from his shoulder toward the price board. He raises the brush
and makes a single deliberate downward stroke, then lowers his hand and looks
up at the board. His eyes track across the columns. Lantern flame flickers
and breathes, throwing moving warm light across his face and the boards.
Kimono fabric shifts slightly. Dusk sky darkens almost imperceptibly.
Restrained, weighted movement — an old man who has done this ten thousand
times.
```
**Negative:** `fast motion, cartoonish gestures, face morphing, extra fingers,
brush passing through the board, text rewriting itself, style shift`

---

## Shot 3 — The chalk stroke

**Plate:** `03_ledger_closeup.png` · **Beat:** 0:31–0:47 · **Length:** 6s

Tight on two weathered hands at a dark wooden board. Right hand holds chalk,
having just written **¥480**; older prices in kanji above. Brass candlestick
with a live candle at right.

The money shot for "hand strokes" **and** "candle reality" — both in one
frame. It also carries the landing's line: *watch the last figure on the board.*

**Stage 1 — photoreal restyle**
```
Photorealistic extreme close-up of two weathered old hands on a dark wooden
price board, lit only by a single candle in a brass holder at the right of
frame. The right hand holds a stub of white chalk resting against the board;
chalked price figures are already written above. Shop interior falls away
into warm darkness behind. Skin texture, chalk dust, wood grain, candle
flame. 50mm macro, very shallow depth of field, single-source firelight.
No illustration, no anime, no painterly texture.
```

**Stage 2 — motion**
```
Camera holds nearly still, breathing very slightly. The right hand completes
a chalk stroke on the board and lifts away, leaving fine chalk dust drifting
through the candlelight. The left hand steadies the board's edge. Candle
flame flickers and bends, and the shadows of both hands sway across the wood
with it. Wisp of smoke off the flame. Everything else stays still. Intimate,
patient, no camera move.
```
**Negative:** `hand morphing, extra fingers, chalk floating unattached, the
written figures changing or rearranging, new characters appearing, camera
pan, style shift`

> ⚠️ **Read this before running shot 3.** Video models garble written text.
> If you prompt "writing ¥480", the existing figures on the board *will*
> scramble mid-clip. The prompt above is deliberately written so the number
> is **already there** and the hand only completes and lifts — the motion is
> the hand, the dust and the flame, never the letterforms. Same reason shot 2
> says "a single downward stroke" rather than naming what he writes.
> If a take does scramble the board, shorten to 4s and crop tighter on the
> hand.

---

## Shot 4 — The candle becomes real

**Plate:** `04_transition_candle.png` · **Beat:** 0:47–1:03 · **Length:** 6s

Parchment scroll on a desk with a single trading candlestick — body and wick —
glowing on it. Brush on a rest, books, teacup, plant, lit brass candle right.

This is the concept shot: the *trading* candle and the *wax* candle sharing a
frame, one lit by the other. Land it and the whole metaphor pays.

**Stage 1 — photoreal restyle**
```
Photorealistic cinematic still life on a worn wooden desk lit by one candle.
A sheet of aged parchment lies flat, with a single candlestick chart mark
inked on it — a filled rectangular body with a thin wick above and below,
the ink faintly luminous. A calligraphy brush rests on a metal brush rest,
old bound books, a ceramic teacup, a small potted plant, and a brass
candleholder with a burning candle at the right. Warm single-source
firelight, deep falloff, fine paper fibre and wood grain. 50mm, shallow
depth of field. No illustration, no anime, no painterly texture.
```

**Stage 2 — motion**
```
Very slow push-in toward the parchment. The real candle's flame flickers and
gutters, and its warm light sweeps across the paper. In time with the flame,
the inked candlestick mark on the parchment glows brighter and dimmer, as if
lit from within — the drawn candle and the burning candle breathing together.
Parchment edge lifts very slightly in the draught. Steam curls from the
teacup. Wax runs a single slow bead down the candle. Nothing else moves.
```
**Negative:** `the chart mark changing shape, extra candlesticks appearing,
axis or numbers appearing, fast flicker, strobing, style shift`

---

## Shot 5 — Paper to screen

**Plate:** `05_bridge_modern.png` · **Beat:** 1:03–1:19 · **Length:** 8s

Split frame: aged parchment with a hand-inked black-and-white candle chart on
the left, a modern monitor on the right showing the *same* chart continuing in
neon candles. Shoji window, dusk city outside, brush and inkwell left, plant
and mug right.

The join is the whole point — the chart must read as one continuous series
crossing from paper onto glass.

**Stage 1 — photoreal restyle**
```
Photorealistic cinematic still of a wooden desk at dusk. On the left, a sheet
of aged parchment bearing a hand-inked black-and-white candlestick chart. On
the right, and slightly behind it, a modern white desktop monitor displaying
the same chart continuing in bright modern candlesticks — the two charts
aligned so the series reads as one unbroken sequence crossing from paper onto
the screen. Shoji paper window behind with a blue dusk city beyond, paper
lantern lamp, calligraphy brush and inkwell at left, potted plant and ceramic
mug at right. Mixed warm lamplight and cool screen glow. 35mm, shallow depth
of field. No illustration, no anime, no painterly texture.
```

**Stage 2 — motion**
```
Slow lateral dolly from left to right, travelling from the parchment onto the
monitor, holding the two charts aligned as one continuous series. The inked
candles on the paper hold perfectly still. On the screen, the newest candles
build and print at the right edge, their glow rising. Screen light pulses
gently across the desk and the parchment. Lantern flame flickers warm at
left. City lights beyond the shoji screen twinkle faintly. Steam drifts from
the mug. The move ends settled on the screen.
```
**Negative:** `the paper chart animating, chart redrawing itself, axis labels
or numbers appearing, screen UI changing, monitor warping, style shift`

---

## Cutting it together

Straight sequence, 34s, matching the pilot narration:

| # | Shot | In | Length | Cut on |
| --- | --- | --- | --- | --- |
| 1 | Port | 0:00 | 8s | the push-in settling |
| 2 | Merchant | 0:08 | 6s | his eyes reaching the board |
| 3 | Chalk | 0:14 | 6s | the hand lifting away |
| 4 | Candle | 0:20 | 6s | the mark at its brightest |
| 5 | Bridge | 0:26 | 8s | settled on the screen |

Three things that make it read as a film rather than five clips:

1. **One light source per shot, and it flickers.** Shots 2, 3 and 4 are all
   firelight. Keeping the flicker alive in each is what ties them together —
   it is the same fire, three centuries deep. Shot 5 breaks it deliberately:
   warm flame on the left, cold screen on the right. That break *is* the
   ending.
2. **Motion decelerates across the sequence.** The port is busy, the merchant
   is deliberate, the chalk hand is nearly still. Then shot 5 moves again.
   Loud → quiet → loud.
3. **Cut on movement, not after it.** Every "cut on" above lands mid-gesture.
   Holding to the end of a generated clip is what makes AI footage feel like
   AI footage.

Audio is already in the repo: `media/youtube-pilot/background-music.m4a` and
the recorded narration, both cut to these beats.

---

## Running it

1. Upload all five plates to OpenArt.
2. **Stage 1 on all five**, same model and settings. Compare against the
   originals before going further.
3. Pick the style. Do not mix — five photoreal or five painted.
4. **Stage 2**, 3–4 takes per shot. Shots 2 and 3 will need the most takes;
   hands and text are where these models break.
5. Pull selects into `media/landing/film/` and cut per the table.

Cost control: shots 1 and 5 are the most forgiving (no hands, no faces).
Prove your model and settings on those two before spending takes on 2 and 3.

---

## Stage 2 — as actually run (Gemini · Veo)

Shots 1 and 2 were generated this way on 2026-08-14 and are live in the
landing. Output arrives 1280×720, 24fps, 10s, with a sparkle watermark
bottom-right — the watermark is cropped out afterwards, so ignore it.

**Flow:** Gemini → + → Create video → attach the start image → paste the
prompt → Landscape (16:9) → send.

Negatives are folded into the prompt text; the Gemini composer has no
separate negative field.

### Shot 3 — The chalk stroke
Start image: `film/stage1/03_ledger_closeup_photoreal.png`

```
Animate this exact image. The camera holds nearly still, breathing very
slightly. The right hand completes its chalk stroke on the board and lifts
away, leaving fine chalk dust drifting through the candlelight. The left hand
steadies the board's edge. The candle flame flickers and bends, and the
shadows of both hands sway across the wood with it. A wisp of smoke rises off
the flame. Everything else stays still. Intimate, patient, no camera move.
The chalked figures already written on the board stay exactly as they are and
never change, rearrange, or gain new characters. Keep the photorealistic style
and single-source firelight locked. No text appearing, no hand morphing, no
extra fingers, no style shift.
```

### Shot 4 — The candle becomes real
Start image: `film/stage1/04_transition_candle_photoreal.png`

```
Animate this exact image. Very slow push-in toward the parchment. The real
candle's flame flickers and gutters, and its warm light sweeps across the
paper. In time with the flame, the inked candlestick mark on the parchment
glows brighter and dimmer, as if lit from within — the drawn candle and the
burning candle breathing together. The parchment edge lifts very slightly in
the draught. Steam curls from the teacup. A single slow bead of wax runs down
the candle. Nothing else moves. The inked mark keeps exactly its shape and
position throughout; no extra candlesticks, no axis, no numbers appear. Keep
the photorealistic style and warm single-source firelight locked. No fast
flicker, no strobing, no style shift.
```

### Shot 5 — Paper to screen
Needs its photoreal still first — plate 5 was never restyled (OpenArt ran out
of credits; Gemini declined the restyle twice). Generate the still, then
animate it.

```
Animate this exact image. Slow lateral dolly from left to right, travelling
from the parchment onto the monitor, holding the two charts aligned as one
continuous series. The inked candles on the paper hold perfectly still. On the
screen, the newest candles build and print at the right edge, their glow
rising. Screen light pulses gently across the desk and the parchment. The
lantern flame flickers warm at left. City lights beyond the shoji screen
twinkle faintly. Steam drifts from the mug. The move ends settled on the
screen. The paper chart never animates or redraws itself; no axis labels or
numbers appear; the screen interface does not change. Keep the photorealistic
style locked. No monitor warping, no style shift.
```
