# CryptoForge — film prompt pack

Six shots for Google Veo, written to match the CryptoForge landing page
(`static/landing/`) shot for shot: same palette, same blacks, same restraint.
Phil generates these; Claude does not ([[proj_dojima_film_prompt_pack]] rule).

## One thing to decide before generating

The page's own honesty section says, in print, that there is **no return figure
here** and that every losing day is published. So a film that shows money
piling up would argue against the page it sits on — and a viewer feels that
contradiction even if they cannot name it. These prompts therefore show
prosperity as **time and ease**, never as cash on screen: he is rested, he is
out, he is not looking at his phone. That reads richer than a stack of notes,
and it is the only version that survives next to the honesty band. The shared
negative prompt bans cash, gold and luxury cars for exactly this reason — delete
those words if you want it literal.

## Paste into every prompt

**Palette clause** (append to each shot, verbatim):

> Colour: near-black background, electric cyan and soft violet as the only cool
> light, warm amber as the single warm accent, mint green and coral pink on the
> screens. Deep true blacks with no lifted shadows, no teal-and-orange grade,
> high colour separation.

**Negative prompt** (same for all six):

> text, letters, numbers, subtitles, captions, watermark, logo, UI labels,
> readable screen text, cartoon, anime, illustration, 3D render, CGI,
> video-game look, stock-footage smile, thumbs up, piles of cash, money falling,
> luxury cars, gold, lens flare, light leaks, teal-and-orange grade, lifted
> milky blacks, bright white office, corporate stock footage, slow-motion cliché

---

## Shot 00 — Hero plate · abstract, no person

Sits **behind the headline**, so nothing in it may compete with text: no face,
nothing to read, almost no movement.

> Extreme macro, photoreal, shot on a probe lens: dust motes and micro-scratches
> drifting across the surface of a black glass panel in a dark room, with soft
> out-of-focus bokeh of electric cyan and violet light sources far behind it,
> and one small warm amber point of light low in the frame. Very slow lateral
> drift, almost still. Shallow depth of field, heavy natural bokeh, fine film
> grain, deep true blacks with no haze. No people, no screens, no objects — pure
> light, glass and dust. Ambient, hypnotic, patient.

## Shot 01 — 02:47 · the exhaustion

> Photoreal cinematic footage, 35mm anamorphic, shallow depth of field, handheld
> with natural micro-shake. A South Asian man in his early thirties sits alone
> at a dark desk at nearly three in the morning, lit only by the cold cyan and
> violet glow of two monitors filled with abstract glowing candlestick charts.
> He is spent: slumped forward, one elbow on the desk, the heel of his palm
> pressed into his eye, then dragging slowly down his face. A cold, half-finished
> cup of coffee beside him. Behind him the room falls into near-total black, one
> warm amber lamp barely alive in the far background. Mint-green and coral-pink
> chart light flickers across his face as the screens move. Slow push-in. Rich
> fine grain, deep true blacks. Unglamorous and real. Mood: burnt out, sleepless,
> done with it. The screens show only abstract lines and bars, no readable text.

## Shot 02 — One tap · the handover

> Photoreal cinematic footage, 35mm anamorphic, shallow depth of field. The same
> dark room, the same man. Macro insert: his fingertip presses once on a phone
> screen and lifts away, a single warm amber pulse spreading out from the point
> of contact. Then wider: the frantic mint-green and coral-pink chart light on
> the monitors resolves and calms into one steady amber line holding level across
> a single dark panel. The cyan chaos drains out of the room until only that
> amber line and a soft cyan rim light on his shoulder remain. He exhales, sits
> back in the chair, and closes the laptop lid. Slow deliberate camera drift.
> Deep true blacks, fine grain. Mood: a decision made, tension leaving the body.

## Shot 03 — The night shift · he sleeps, it works

> Photoreal cinematic time-lapse, locked-off camera with a very slow drift,
> shallow depth of field. A dark room at night. In the soft-focus background the
> same man is asleep, a still shape under a blanket on a low sofa, breathing
> slowly. In the sharp foreground the desk is still alive: one dark panel with a
> single warm amber line advancing steadily left to right, and a faint cyan glow
> along the edge of the glass. Through the window behind, city lights pulse and
> the sky cycles slowly from deep blue-black to the first cold grey of dawn.
> Nothing dramatic happens. Fine grain, deep true blacks, no haze. Mood: quiet,
> patient, working. No readable text on any screen.

## Shot 04 — Morning · he does not check it

> Photoreal cinematic footage, 35mm anamorphic, shallow depth of field, handheld
> follow. Bright late-morning sunlight on a Chennai rooftop. The same man walks
> through frame in a light linen shirt, coffee in hand, unhurried and rested. He
> pulls a phone from his pocket, glances at it for barely a second, breathes out
> once — the smallest satisfied release, not a grin — and slides it back into his
> pocket without breaking stride. Warm amber sunlight in the highlights, cool
> cyan and violet in the shadows, deep true blacks retained even in daylight.
> Fine grain. Mood: unbothered, light, the exact opposite of the first shot. No
> readable text on the phone.

## Shot 05 — The day life · the absence of screens

> Photoreal cinematic footage, 35mm anamorphic, shallow depth of field, gentle
> handheld. Golden hour. The same man is with friends at a rooftop table,
> laughing mid-conversation, genuinely present — no phone anywhere in frame, no
> screens at all. Warm amber low sun raking across the table, cool cyan and
> violet in the deep shadows and in the city behind, deep true blacks. The camera
> drifts and finds him, then racks focus past him to the skyline going dark. Fine
> grain. Mood: earned ease, ordinary happiness, time that belongs to him. The
> absence of screens is the point of the shot.

---

## Settings

| | |
|---|---|
| Aspect | **16:9** — every other ratio has to be cropped and loses the sides |
| Length | 5–8 s per shot |
| Resolution | The highest offered; it is downscaled to 720p for the web anyway |
| Takes | Generate **2–3 of each** and keep the best — the first is rarely the one |
| Audio | Irrelevant, it gets stripped |

**Never ask a video model to render text.** It produces convincing-looking
gibberish, and a landing page with fake words on a fake screen reads as fake.
Every screen in these prompts is abstract light on purpose.

## When they are done

Drop them in `~/Downloads` and say the filenames. Then:

1. Watermark measured and cropped (not guessed — the Dōjima crop was wrong once
   at `1184:666` and had to be re-measured to `1120:630`), scaled to 1280×720.
2. Audio stripped; H.264 main, `yuv420p`, CRF 27, `+faststart`.
3. A poster frame per clip, so a blocked or slow video is never a black hole.
4. Wired under the same performance contract as Dōjima: one decode at a time,
   paused off-screen and on a hidden tab, and **no video bytes fetched at all**
   below 880px, on save-data, on 2g, or under `prefers-reduced-motion`.

**Placement:** Shot 00 goes behind the hero headline. Shots 01–05 become a
five-act scroll film, one per section — exhaustion at Method, the handover at
the stripe, the night shift at The desk, morning at Honesty, the day at Access.
Confirmed against the real page once the files exist.
