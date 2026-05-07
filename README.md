# Souvenir

Personal memory-film maker. Paste three things — a Google Photos shared link, a YouTube link, and your text — get an MP4 back.

---

## Setup — pick one

### Option A · GitHub Codespaces *(zero local install)*

1. Go to <https://github.com/gershonconsulting/HomeVideoCreation>.
2. Click the green **Code** button → **Codespaces** tab → **Create codespace on main**.
3. Wait ~60 seconds. The `.devcontainer/` config installs ffmpeg, yt-dlp, fonts, and npm deps automatically.
4. Once the IDE loads, in its terminal: `npm start`
5. A popup appears: *"Your application running on port 3737 is available."* Click **Open in Browser**.
6. Use the app. Generated MP4s live in `/workspaces/HomeVideoCreation/jobs/<id>/output.mp4` — right-click in the file tree → *Download*.

When you're done, close the browser tab — Codespaces idle-stops itself after 30 minutes. Free tier is 60 hours/month, plenty for POC testing.

### Option B · Your own VPS *(production-style hosting)*

For always-on hosting with your own domain (e.g., `souvenir.gershoncrm.com`), deploy to your OpenClaw VPS or any Ubuntu box:

```bash
git clone https://github.com/gershonconsulting/HomeVideoCreation.git
cd HomeVideoCreation
sudo apt install -y nodejs npm ffmpeg python3-pip fonts-liberation
pip3 install --user -U yt-dlp
npm install
FONT_NAME="Liberation Serif" pm2 start server.js --name souvenir
```

Then point an Nginx reverse proxy at `localhost:3737` and add SSL via Let's Encrypt. Standard Hostinger VPS playbook.

### Option C · Docker on any machine

```bash
git clone https://github.com/gershonconsulting/HomeVideoCreation.git
cd HomeVideoCreation
docker compose up
```

Open <http://localhost:3737>. Tear down with `docker compose down`.

### Option D · Native local install

Native install instructions (Windows / Mac / Linux) at the bottom of this README — only useful if you want to develop or you don't have Codespaces / Docker available.

---

## Workflow

---

## Workflow

1. **Paste the YouTube link**, click **Analyze song**. The server pulls metadata via `yt-dlp` (no audio download yet — just info), queries [LRCLIB](https://lrclib.net/) for synced lyrics, and shows you:
   - Artist · title · duration · lyric line count · match confidence
   - **Suggested photo counts** based on lyric structure: one per line, one every two lines, two per line. (Tells you exactly how many photos to curate before the next step.)
   - **"Use synced lyrics as captions"** button → fills the text box with `[mm:ss] line` for every lyric, ready for you to edit on top of.
2. **Paste the Google Photos shared link.** (Set to "Anyone with the link" — that's the default when you Share → Copy link.)
3. **Edit the captions** — keep the synced lyrics, replace some with your own commentary at the same timestamps, or write entirely new text. Mix freely:
   ```
   [00:08] Tu sais, je n'ai jamais été aussi heureux que ce matin-là
   [00:13] Nous marchions sur une plage un peu comme celle-ci

   [02:20] (Cela fait 13 ans que je suis ton plus grand fan)
   ```
   Lines starting with `[mm:ss]` lock to that second. Lines without timestamps spread evenly. If you have *no* timestamps at all, paragraphs separated by blank lines distribute evenly across the song.
4. Click **Render film**. Watch progress, download the MP4.

## How it works under the hood

When you click *Analyze*:
- `yt-dlp -J --skip-download` extracts title/artist/track/duration in ~3 seconds with no audio download.
- The artist+track+duration is sent to LRCLIB's `/api/get` (with duration as a confidence signal). Falls back to title parsing and `/api/search` if needed.
- If both orderings fail (some YouTube titles are "Track - Artist" instead of "Artist - Track"), it tries the swap.

When you click *Render*:
1. **Photos** — fetches the Google Photos shared album page, parses the embedded photo URLs out of the HTML data with regex (`lh3.googleusercontent.com/pw/...`), downloads each at `=w2048`. 6 in parallel.
2. **Audio** — `yt-dlp -x --audio-format mp3 --audio-quality 0` against the YouTube URL.
3. **Probe** — `ffprobe` reads the MP3 duration. That's the film length.
4. **Captions** — your text is parsed: lines with `[mm:ss]` become timestamped SRT cues, lines without get even-distribution. SRT is written to `jobs/<id>/captions.srt`.
5. **Encode** — single ffmpeg pass:
   - photos as a slideshow at `1 / (audioDuration / N)` framerate
   - cover-fit + crop to your chosen resolution, 30 fps
   - SRT burned in via the `subtitles` filter (Georgia 22pt, opaque band on bottom by default)
   - MP3 audio muxed, `-shortest` (last photo holds for the audio tail)
   - H.264 / AAC, MP4 container

## Output location

Each render lives in `jobs/<jobId>/`:

```
jobs/a1b2c3d4/
├── photos/                  # downloaded JPEGs
├── music.mp3                # extracted audio
├── captions.srt             # subtitle file
└── output.mp4               # the final film
```

Re-download any past job at `http://localhost:3737/api/file/<jobId>`. Wipe history with `rm -rf jobs/`.

## Settings

| Setting | What it does |
|---|---|
| Resolution | 1080p (default) or 720p — 720p renders ~30% faster |
| Text position | Bottom band (default) or centered overlay |
| Render speed | Fast (bigger file) / Balanced / Slow (smaller file) |
| Show text | Toggle the captions on/off |

## Caption syntax

```
[00:08] One caption, locked to 8 seconds
[00:13] Next one, runs from 13s until the next timestamp

A continuation line without a timestamp
gets appended to the previous caption.

[01:20] Skip the section between 13s and 1:20 — it'll show no caption.
```

## Limits / known constraints

- **Album size**: Google Photos lazy-loads beyond ~80 photos. The first batch always comes back from the initial HTML; pagination isn't implemented yet.
- **Photo order**: Whatever order Google Photos returns them in. Looks chronological in practice.
- **Album visibility**: Must be a *shared* album with "Anyone with the link" enabled. Private albums require OAuth.
- **Lyrics availability**: LRCLIB has great coverage for popular tracks but not everything. If no synced lyrics are found, the analyze step still tells you the duration and gives photo-count suggestions based on time alone.
- **YouTube ToS**: extracting audio is technically against YouTube's terms. Use only for personal/family content where you have an actual right to the music. Don't post the output publicly.

## Troubleshooting

**"yt-dlp exited with code…"** — `yt-dlp` is missing or YouTube changed their internals. Update it: on Windows, `winget upgrade yt-dlp.yt-dlp`; on Mac, `brew upgrade yt-dlp`; on Linux, `pip3 install -U yt-dlp`.

**"'ffmpeg' is not recognized as an internal or external command"** *(Windows)* — your terminal session predates the install. Close all PowerShell/cmd windows and open a new one. If still missing, FFmpeg isn't in PATH — install `Gyan.FFmpeg` explicitly (see Option A note above) rather than relying on `yt-dlp.FFmpeg`.

**PowerShell: "...cannot be loaded because running scripts is disabled"** — use `PowerShell -ExecutionPolicy Bypass -File .\install-windows.ps1` instead of running the script directly.

**"No photos found"** — link is private (not "Anyone with link"), or the album was emptied/unshared. Verify in an incognito window.

**Lyrics show "Loose match" or "Best guess"** — LRCLIB found something but the duration doesn't line up perfectly. The lyrics might still work, but timing could drift. Inspect with *Show lyrics* before using.

**Render is slow** — change render speed to "Fast" and/or drop to 720p. Modern hardware does balanced 1080p at ~5× realtime.

---

## Native local install (Option D from above)

These are only useful if you can't use Codespaces or Docker.

### Windows

In the cloned folder, open PowerShell and run:

```powershell
PowerShell -ExecutionPolicy Bypass -File .\install-windows.ps1
```

That installs Node.js LTS, FFmpeg, and yt-dlp via `winget`. **Close PowerShell, open a new window**, then `npm install && npm start`.

If the script fails (winget unavailable or stale), do it manually — note the **order matters**:

```powershell
winget install --id OpenJS.NodeJS.LTS -e --silent --accept-package-agreements --accept-source-agreements
winget install --id Gyan.FFmpeg       -e --silent --accept-package-agreements --accept-source-agreements
winget install --id yt-dlp.yt-dlp     -e --silent --accept-package-agreements --accept-source-agreements
```

> Install `Gyan.FFmpeg` *before* `yt-dlp.yt-dlp`. Otherwise winget pulls in `yt-dlp.FFmpeg` as a hidden dependency that it does *not* add to PATH.

### Mac

```bash
brew install node yt-dlp ffmpeg
npm install
npm start
```

### Linux

```bash
sudo apt install -y nodejs npm ffmpeg python3-pip fonts-liberation
pip3 install --user -U yt-dlp
npm install
npm start
```

