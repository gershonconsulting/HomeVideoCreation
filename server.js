// Souvenir — local server
//
// Responsibilities:
//   1. Serve the UI at /
//   2. POST /api/render  → kicks off a render job, streams progress as JSON-lines
//   3. GET  /api/file/:jobId → downloads the finished MP4
//
// Pipeline per job:
//   a. Scrape Google Photos shared album → download all photos
//   b. yt-dlp the YouTube link → extract MP3
//   c. Build photos.txt (concat demuxer) + captions.srt
//   d. Run ffmpeg → photos+audio+burned-in subtitles → output.mp4
//
// All artifacts live under ./jobs/<jobId>/ and are kept until you wipe them.

import express from 'express';
import multer from 'multer';
import fs from 'node:fs/promises';
import { existsSync, createReadStream } from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
// Shared yt-dlp flags to mitigate YouTube's anti-bot detection on cloud IPs:
//  - player_client=android,web — the android client uses a different API path that's
//    less aggressively bot-checked than the web player
//  - user-agent — present as a real Chrome browser, not python-requests
//  - js-runtimes — silence the "no JS runtime" warning by pointing at /usr/bin/node
const YT_DLP_BASE_FLAGS = [
  '--extractor-args', 'youtube:player_client=tv_embedded,android_vr,android_creator,ios,mweb,android,web_safari',
  '--user-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  '--js-runtimes', 'node:/usr/bin/node',
  '--no-warnings',
];

import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 3737;
const JOBS_DIR = path.join(__dirname, 'jobs');
await fs.mkdir(JOBS_DIR, { recursive: true });
// Multer storage for uploaded audio files — saved straight into the new job dir
// (we'll create a tmp dir up-front, the render handler moves files into the job dir)
const UPLOAD_TMP = path.join(JOBS_DIR, '_uploads_tmp');
await fs.mkdir(UPLOAD_TMP, { recursive: true });

// Build/deploy fingerprint surfaced via /api/version so the user can confirm
// which commit is actually running on Render. RENDER_GIT_COMMIT is set by
// Render at deploy time; locally it falls back to "dev".
const PKG = JSON.parse(await fs.readFile(new URL('./package.json', import.meta.url), 'utf8'));
const VERSION = {
  app: PKG.version,
  build: PKG.build || 0,
  commit: process.env.RENDER_GIT_COMMIT || 'dev',
  shortCommit: (process.env.RENDER_GIT_COMMIT || 'dev').slice(0, 7),
  branch: process.env.RENDER_GIT_BRANCH || 'main',
  bootedAt: new Date().toISOString(),
};
const upload = multer({
  dest: UPLOAD_TMP,
  limits: { fileSize: 50 * 1024 * 1024 }, // 50 MB
});


const app = express();
app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ───────────────────────────────────────────────────────────────
// Utility: stream progress events as newline-delimited JSON
// ───────────────────────────────────────────────────────────────
function makeEmitter(res) {
  res.setHeader('Content-Type', 'application/x-ndjson');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('X-Accel-Buffering', 'no');
  return (event) => {
    res.write(JSON.stringify(event) + '\n');
  };
}

// ───────────────────────────────────────────────────────────────
// Step 1: Scrape Google Photos shared album
// ───────────────────────────────────────────────────────────────
async function scrapeGooglePhotos(shareUrl, emit) {
  emit({ phase: 'photos', status: 'fetching' });

  const res = await fetch(shareUrl, {
    headers: {
      'User-Agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      'Accept-Language': 'en-US,en;q=0.9',
    },
  });
  if (!res.ok) throw new Error(`Google Photos returned HTTP ${res.status}`);
  const html = await res.text();

  // The shared-album HTML contains photo URLs as JSON-encoded strings.
  // Pattern: "https://lh3.googleusercontent.com/pw/<id>=<sizesuffix>"
  // We capture all, strip the size suffix to get unique base URLs,
  // then request each at a high res via =w2048.
  const re = /"(https:\/\/lh3\.googleusercontent\.com\/pw\/[A-Za-z0-9_\-]+(?:=[^"]*)?)"/g;
  const allWithSizes = [...html.matchAll(re)].map((m) => m[1]);
  const baseUrls = [...new Set(allWithSizes.map((u) => u.replace(/=[^=]*$/, '')))];

  if (baseUrls.length === 0) {
    throw new Error(
      'No photos found. Make sure the Google Photos link is a "shared album" link (the kind that starts with photos.google.com/share/AF1Qip...) and that the album has public sharing enabled.'
    );
  }

  emit({ phase: 'photos', status: 'found', count: baseUrls.length });
  return baseUrls.map((u) => u + '=w1280');
}

async function downloadPhotos(urls, dir, emit) {
  await fs.mkdir(dir, { recursive: true });
  const localPaths = [];
  let done = 0;
  // Modest concurrency — Google Photos is fine but no need to hammer
  const CONCURRENCY = 6;

  // Worker pool
  const queue = urls.map((url, i) => ({ url, i }));
  async function worker() {
    while (queue.length) {
      const { url, i } = queue.shift();
      const localName = `photo_${String(i).padStart(4, '0')}.jpg`;
      const localPath = path.join(dir, localName);
      const r = await fetch(url);
      if (!r.ok) throw new Error(`Photo ${i} failed: HTTP ${r.status}`);
      const buf = Buffer.from(await r.arrayBuffer());
      await fs.writeFile(localPath, buf);
      localPaths[i] = localPath;
      done += 1;
      emit({ phase: 'photos', status: 'progress', done, total: urls.length });
    }
  }
  await Promise.all(Array.from({ length: CONCURRENCY }, worker));

  emit({ phase: 'photos', status: 'done', count: localPaths.length });
  return localPaths;
}

// ───────────────────────────────────────────────────────────────
// Step 2: Download YouTube audio via yt-dlp → MP3
// ───────────────────────────────────────────────────────────────
function downloadAudio(youtubeUrl, dir, emit) {
  return new Promise((resolve, reject) => {
    emit({ phase: 'audio', status: 'fetching' });

    const outTemplate = path.join(dir, 'music.%(ext)s');
    const proc = spawn('yt-dlp', [
      ...YT_DLP_BASE_FLAGS,
      '-x',
      '--audio-format', 'mp3',
      '--audio-quality', '0',
      '--no-playlist',
      '-o', outTemplate,
      youtubeUrl,
    ]);

    let lastProgress = 0;
    proc.stdout.on('data', (chunk) => {
      const text = chunk.toString();
      // yt-dlp prints lines like: [download]  37.4% of  4.05MiB at 1.20MiB/s ETA 00:02
      const m = text.match(/\[download\]\s+(\d+\.\d+)%/);
      if (m) {
        const p = parseFloat(m[1]);
        if (p - lastProgress >= 1 || p === 100) {
          lastProgress = p;
          emit({ phase: 'audio', status: 'progress', percent: p });
        }
      }
    });
    proc.stderr.on('data', (chunk) => {
      // surface yt-dlp errors to console (not user) for debugging
      process.stderr.write('[yt-dlp] ' + chunk);
    });
    proc.on('error', reject);
    proc.on('close', (code) => {
      if (code !== 0) {
        return reject(
          new Error(
            `yt-dlp exited with code ${code}. Make sure yt-dlp is installed and the URL is valid.`
          )
        );
      }
      const mp3 = path.join(dir, 'music.mp3');
      if (!existsSync(mp3)) {
        return reject(new Error('yt-dlp finished but music.mp3 was not produced.'));
      }
      emit({ phase: 'audio', status: 'done' });
      resolve(mp3);
    });
  });
}

// ───────────────────────────────────────────────────────────────
// Step 3: Probe audio duration with ffprobe
// ───────────────────────────────────────────────────────────────
function probeDuration(audioPath) {
  return new Promise((resolve, reject) => {
    const proc = spawn('ffprobe', [
      '-v', 'error',
      '-show_entries', 'format=duration',
      '-of', 'default=noprint_wrappers=1:nokey=1',
      audioPath,
    ]);
    let out = '';
    proc.stdout.on('data', (c) => (out += c.toString()));
    proc.on('error', reject);
    proc.on('close', (code) => {
      if (code !== 0) return reject(new Error(`ffprobe failed (${code})`));
      const d = parseFloat(out.trim());
      if (!isFinite(d) || d <= 0) return reject(new Error('Could not read audio duration'));
      resolve(d);
    });
  });
}

// ───────────────────────────────────────────────────────────────
// Song analysis: yt-dlp metadata + LRCLIB synced lyrics
// ───────────────────────────────────────────────────────────────
function ytdlpMetadata(youtubeUrl) {
  return new Promise((resolve, reject) => {
    const proc = spawn('yt-dlp', [...YT_DLP_BASE_FLAGS, '-J', '--skip-download', youtubeUrl]);
    let out = '', err = '';
    proc.stdout.on('data', (c) => (out += c.toString()));
    proc.stderr.on('data', (c) => (err += c.toString()));
    proc.on('error', reject);
    proc.on('close', (code) => {
      if (code !== 0) {
        return reject(new Error(`yt-dlp metadata failed: ${err.split('\n').slice(-3).join(' ')}`));
      }
      try {
        const j = JSON.parse(out);
        resolve({
          title: j.title || '',
          artist: j.artist || j.creator || j.uploader || '',
          track: j.track || '',
          album: j.album || '',
          duration: j.duration || 0,
          channel: j.channel || '',
        });
      } catch (e) {
        reject(new Error('yt-dlp returned non-JSON metadata'));
      }
    });
  });
}

function parseTitleArtist(title) {
  // Strip common parenthetical/bracket noise from YouTube titles
  let cleaned = title
    .replace(/\([^)]*(?:audio|video|official|officiel|lyric|paroles|live|remix|hd|hq|\d{4})[^)]*\)/gi, '')
    .replace(/\[[^\]]*(?:audio|video|official|officiel|lyric|paroles|live|remix|hd|hq|\d{4})[^\]]*\]/gi, '')
    .replace(/\(\d{4}\)/g, '')
    .replace(/\s{2,}/g, ' ')
    .trim();
  // "Artist - Title" or "Artist — Title" (em-dash) or "Artist – Title" (en-dash)
  const m = cleaned.match(/^(.+?)\s*[-—–]\s*(.+)$/);
  if (m) return { artist: m[1].trim(), track: m[2].trim() };
  return { artist: '', track: cleaned };
}

function parseLrcLines(lrcText) {
  const out = [];
  for (const raw of lrcText.split(/\r?\n/)) {
    const m = raw.match(/^\[(\d+):(\d+)(?:\.(\d+))?\]\s*(.*)$/);
    if (!m) continue;
    const minutes = parseInt(m[1], 10);
    const seconds = parseInt(m[2], 10);
    const frac = m[3] ? parseFloat('0.' + m[3]) : 0;
    const time = minutes * 60 + seconds + frac;
    const text = (m[4] || '').trim();
    out.push({ time, text });
  }
  return out;
}

async function fetchLyricsFromLrclib(artist, track, duration) {
  const tries = [];
  // 1. Exact get with duration (highest confidence)
  if (artist && track && duration) {
    const url = `https://lrclib.net/api/get?artist_name=${encodeURIComponent(artist)}&track_name=${encodeURIComponent(track)}&duration=${Math.round(duration)}`;
    tries.push({ url, kind: 'exact-with-duration' });
  }
  // 2. Exact get without duration
  if (artist && track) {
    const url = `https://lrclib.net/api/get?artist_name=${encodeURIComponent(artist)}&track_name=${encodeURIComponent(track)}`;
    tries.push({ url, kind: 'exact' });
  }
  // 3. Search and pick best by duration proximity
  const q = [artist, track].filter(Boolean).join(' ');
  if (q) {
    tries.push({ url: `https://lrclib.net/api/search?q=${encodeURIComponent(q)}`, kind: 'search' });
  }

  for (const { url, kind } of tries) {
    try {
      const r = await fetch(url, { headers: { 'User-Agent': 'Souvenir/1.0' } });
      if (!r.ok) continue;
      const data = await r.json();
      const candidates = Array.isArray(data) ? data : [data];
      const best = pickBestLyricMatch(candidates, duration);
      if (best && best.syncedLyrics) {
        return {
          source: 'lrclib',
          via: kind,
          syncedLyrics: best.syncedLyrics,
          plainLyrics: best.plainLyrics || '',
          matchedTrack: best.trackName || best.name,
          matchedArtist: best.artistName,
          matchedDuration: best.duration,
          confidence: confidenceFor(best, duration),
        };
      }
    } catch (e) {
      // try next strategy
    }
  }
  return null;
}

function pickBestLyricMatch(candidates, targetDuration) {
  const withSynced = candidates.filter((c) => c && c.syncedLyrics);
  if (withSynced.length === 0) return candidates[0] || null;
  if (!targetDuration) return withSynced[0];
  // Closest duration wins
  return withSynced
    .map((c) => ({ c, gap: Math.abs((c.duration || 0) - targetDuration) }))
    .sort((a, b) => a.gap - b.gap)[0].c;
}

function confidenceFor(match, targetDuration) {
  if (!match) return 0;
  if (!targetDuration || !match.duration) return 0.7;
  const gap = Math.abs(match.duration - targetDuration);
  if (gap <= 1) return 0.99;
  if (gap <= 5) return 0.9;
  if (gap <= 15) return 0.7;
  return 0.4;
}

// MP3-based analyzer: probe duration via ffprobe, count photos at the
// Google Photos URL, suggest target photo counts, optionally try LRCLIB
// from filename ("Artist - Title.mp3").
async function analyzeFromFile(audioFilePath, photosUrl, originalFilename, emit) {
  const E = emit || (() => {});
  E({ phase: 'analyze', percent: 5, label: 'Reading audio duration' });

  const duration = await probeDuration(audioFilePath).catch(() => 0);
  E({ phase: 'analyze', percent: 30, label: 'Counting photos in album' });

  const photoUrls = await scrapeGooglePhotos(photosUrl, () => {}).catch(() => []);
  const photosFound = photoUrls.length;
  E({ phase: 'analyze', percent: 60, label: `${photosFound} photos found` });

  // Try to extract Artist - Title from filename for bonus LRCLIB sync
  let parsedArtist = '', parsedTitle = '';
  if (originalFilename) {
    const stripped = originalFilename
      .replace(/\.[a-z0-9]+$/i, '')        // drop extension
      .replace(/\([^)]*\)/g, '')          // drop "(ParolesLyrics)" etc
      .replace(/\[[^\]]*\]/g, '')        // drop "[...]"
      .replace(/\s+/g, ' ')                // collapse whitespace
      .trim();
    const m = stripped.match(/^(.+?)\s*[-–—]\s*(.+)$/);
    if (m) { parsedArtist = m[1].trim(); parsedTitle = m[2].trim(); }
  }

  E({ phase: 'analyze', percent: 70, label: parsedArtist && parsedTitle
      ? `Looking up synced lyrics`
      : 'Skipping lyrics (filename did not parse as Artist - Title)' });

  // Optional lyrics lookup — non-fatal
  let lyrics = null, lyricLines = [];
  if (parsedArtist && parsedTitle && duration > 0) {
    try {
      lyrics = await fetchLyricsFromLrclib(parsedArtist, parsedTitle, duration);
      if (!lyrics) lyrics = await fetchLyricsFromLrclib(parsedTitle, parsedArtist, duration);
      if (lyrics && lyrics.syncedLyrics) {
        lyricLines = parseLrcLines(lyrics.syncedLyrics).filter(l => l.text.length > 0);
      }
    } catch {}
  }

  // Build photo-count suggestions:
  //  - cinematic (slow): 8-10s/photo
  //  - balanced (default): ~6s/photo
  //  - montage (fast):    ~3s/photo
  // Also include a "lyric-aligned" option if we have synced lyrics.
  const dur = duration || 240;
  const suggestions = [];
  for (const [sec, label] of [[10, 'cinematic · 1 photo every 10s'],
                              [6,  'balanced · 1 photo every 6s'],
                              [3,  'montage · 1 photo every 3s']]) {
    const count = Math.max(1, Math.round(dur / sec));
    suggestions.push({ count, perPhoto: sec, label });
  }
  if (lyricLines.length > 0) {
    suggestions.unshift({
      count: lyricLines.length,
      perPhoto: +(dur / lyricLines.length).toFixed(1),
      label: `lyric-synced · 1 photo per lyric line (${lyricLines.length} lines)`,
    });
  }

  // Recommended = balanced (6s/photo)
  const recommended = Math.max(1, Math.round(dur / 6));
  const trim = photosFound > recommended ? photosFound - recommended : 0;
  const add  = photosFound > 0 && photosFound < Math.max(1, Math.round(dur / 12)) ? Math.round(dur / 6) - photosFound : 0;

  E({ phase: 'analyze', percent: 100, label: lyrics ? `${lyricLines.length} synced lines matched` : 'Analysis complete' });
  return {
    audio: {
      filename: originalFilename || '',
      duration,
      parsedArtist, parsedTitle,
    },
    photos: {
      found: photosFound,
      recommended,
      trim,
      add,
      perPhotoIfKept: photosFound > 0 ? +(dur / photosFound).toFixed(1) : 0,
    },
    suggestions,
    lyrics: lyrics ? {
      found: true,
      synced: !!lyrics.syncedLyrics,
      source: lyrics.source,
      confidence: lyrics.confidence,
      matchedArtist: lyrics.matchedArtist,
      matchedTrack: lyrics.matchedTrack,
      matchedDuration: lyrics.matchedDuration,
      lineCount: lyricLines.length,
      lines: lyricLines,
      syncedLyricsRaw: lyrics.syncedLyrics,
    } : { found: false },
  };
}

async function analyzeSong(youtubeUrl) {
  const meta = await ytdlpMetadata(youtubeUrl);

  // Build best-guess artist/track
  let artist = meta.artist;
  let track = meta.track;
  if (!artist || !track) {
    const parsed = parseTitleArtist(meta.title);
    artist = artist || parsed.artist;
    track = track || parsed.track;
  }

  // Try LRCLIB with both orderings (some YouTube titles are "Title - Artist")
  let lyrics = await fetchLyricsFromLrclib(artist, track, meta.duration);
  if (!lyrics && artist && track) {
    lyrics = await fetchLyricsFromLrclib(track, artist, meta.duration);
  }

  let lyricLines = [];
  if (lyrics && lyrics.syncedLyrics) {
    lyricLines = parseLrcLines(lyrics.syncedLyrics).filter((l) => l.text.length > 0);
  }

  // Suggested photo counts
  // Strategy: based on lyric structure (or duration if no lyrics)
  const duration = meta.duration || 240;
  const suggestions = [];
  if (lyricLines.length > 0) {
    suggestions.push({
      count: lyricLines.length,
      label: 'one photo per lyric line',
      perPhoto: +(duration / lyricLines.length).toFixed(1),
    });
    suggestions.push({
      count: Math.round(lyricLines.length / 2),
      label: 'one photo every two lines (slower pacing)',
      perPhoto: +(duration / Math.round(lyricLines.length / 2)).toFixed(1),
    });
    suggestions.push({
      count: lyricLines.length * 2,
      label: 'two photos per lyric line (faster pacing)',
      perPhoto: +(duration / (lyricLines.length * 2)).toFixed(1),
    });
  } else {
    // No lyrics — suggest by duration alone (3s, 5s, 8s per photo)
    for (const sec of [3, 5, 8]) {
      const count = Math.round(duration / sec);
      suggestions.push({ count, label: `${sec}s per photo`, perPhoto: sec });
    }
  }

  return {
    audio: {
      title: meta.title,
      artist,
      track,
      duration,
      channel: meta.channel,
    },
    lyrics: lyrics
      ? {
          found: true,
          synced: !!lyrics.syncedLyrics,
          source: lyrics.source,
          confidence: lyrics.confidence,
          matchedArtist: lyrics.matchedArtist,
          matchedTrack: lyrics.matchedTrack,
          matchedDuration: lyrics.matchedDuration,
          lineCount: lyricLines.length,
          lines: lyricLines,
          syncedLyricsRaw: lyrics.syncedLyrics,
        }
      : { found: false },
    suggestions,
  };
}

// ───────────────────────────────────────────────────────────────
// Step 4: Build concat playlist + SRT, then run ffmpeg
// ───────────────────────────────────────────────────────────────
function buildFilename(rawTitle, when) {
  // Sanitize title for filesystem use, append ISO-ish date stamp
  const safe = (rawTitle || 'souvenir')
    .replace(/[^A-Za-z0-9_\-\s]+/g, '')
    .replace(/\s+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 80) || 'souvenir';
  const d = when || new Date();
  const pad = (n) => String(n).padStart(2, '0');
  const stamp = `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())}_${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}`;
  return `${safe}_${stamp}.mp4`;
}

function srtTime(seconds) {
  const t = Math.max(0, seconds);
  const h = Math.floor(t / 3600);
  const m = Math.floor((t % 3600) / 60);
  const s = Math.floor(t % 60);
  const ms = Math.round((t - Math.floor(t)) * 1000);
  return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')},${String(ms).padStart(3, '0')}`;
}

function escapeSubtitlesPath(p) {
  // ffmpeg's subtitles filter has fussy path escaping. Forward slashes work on
  // every OS; the drive-letter colon on Windows must be escaped so it isn't
  // mistaken for a filter-argument separator.
  return p
    .replace(/\\/g, '/')      // Windows backslashes → forward slashes
    .replace(/:/g, '\\:')     // Escape colons (drive letters, etc.)
    .replace(/'/g, "\\'");    // Escape single quotes inside the quoted path
}

// Snap user-written text to the actual song structure when LRCLIB synced
// lyrics are available. This handles cases where the user wrote their own
// commentary in verse/chorus blocks but didn't add [mm:ss] timestamps —
// without this, line N just shows up at duration*N/total which has no
// relation to where the chorus actually plays in the audio.
//
// Strategy:
//  1. Detect chorus instances in the synced lyrics by line repetition.
//  2. Parse user text into "sections" split on [Refrain]/[Chorus]/[Verse]/etc.
//  3. Map user's chorus sections to the song's chorus instance times in
//     order; verse sections fill the gaps between (or before/after).
//  4. Within each section, distribute lines evenly across that time range.
function alignTextToSongStructure(userText, syncedLyricsRaw, audioDuration) {
  if (!syncedLyricsRaw) return null;
  const lyricLines = parseLrcLines(syncedLyricsRaw).filter(l => l.text.length > 0);
  if (lyricLines.length < 8) return null;

  // Find chorus lines: keys (first 30 chars, lowercased) that repeat ≥ 2x
  const counts = {};
  lyricLines.forEach(l => {
    const k = l.text.toLowerCase().trim().slice(0, 30);
    counts[k] = (counts[k] || 0) + 1;
  });
  const chorusKeys = new Set(Object.keys(counts).filter(k => counts[k] >= 2));
  if (chorusKeys.size === 0) return null;

  // Find chorus instances (consecutive runs of chorus-keyed lines)
  const instances = [];
  let inChorus = false, chorusStart = null;
  for (let i = 0; i < lyricLines.length; i++) {
    const k = lyricLines[i].text.toLowerCase().trim().slice(0, 30);
    if (chorusKeys.has(k)) {
      if (!inChorus) { inChorus = true; chorusStart = lyricLines[i].time; }
    } else {
      if (inChorus) {
        instances.push({ start: chorusStart, end: lyricLines[i].time });
        inChorus = false;
      }
    }
  }
  if (inChorus) {
    // Last chorus extends to song end
    instances.push({ start: chorusStart, end: audioDuration });
  }
  if (instances.length === 0) return null;

  // Parse user text into sections separated by markers like [Refrain], [Chorus], [Verse 2], (Parlé...)
  const sections = [];
  let curLines = [];
  let curMarker = null; // null = pre-marker (intro/verse 1)
  for (const raw of userText.split(/\r?\n/)) {
    const line = raw.trim();
    if (!line) continue;
    const hdrSquare = line.match(/^\[(.+?)\]\s*$/);
    const hdrParen  = line.match(/^\(([^)]+)\)\s*$/);
    if (hdrSquare || hdrParen) {
      if (curLines.length > 0) sections.push({ marker: curMarker, lines: curLines });
      curLines = [];
      curMarker = (hdrSquare ? hdrSquare[1] : hdrParen[1]).toLowerCase();
      continue;
    }
    curLines.push(line);
  }
  if (curLines.length > 0) sections.push({ marker: curMarker, lines: curLines });
  if (sections.length === 0) return null;

  // Categorize each section as chorus or verse
  const isChorusMarker = m => m && /refrain|chorus/.test(m);

  // Walk through sections in order, assigning time ranges
  const captions = [];
  let nextChorusIdx = 0;
  let cursor = 0; // time we've used up to

  for (let i = 0; i < sections.length; i++) {
    const sec = sections[i];
    const isChorus = isChorusMarker(sec.marker);
    let start, end;
    if (isChorus) {
      // Snap to next available chorus instance
      const c = instances[nextChorusIdx];
      if (c) {
        start = c.start;
        end = c.end;
        nextChorusIdx++;
      } else {
        // More chorus blocks than the song has — distribute remaining time
        start = cursor;
        end = audioDuration;
      }
    } else {
      // Verse: from cursor to next chorus start (or song end)
      const next = instances[nextChorusIdx];
      start = cursor;
      end = next ? next.start : audioDuration;
    }
    if (end <= start) end = Math.min(audioDuration, start + 5);
    cursor = end;

    const n = sec.lines.length;
    if (n > 0) {
      const per = (end - start) / n;
      for (let j = 0; j < n; j++) {
        captions.push({
          start: start + j * per,
          end: start + (j + 1) * per,
          text: sec.lines[j],
        });
      }
    }
  }

  if (captions.length === 0) return null;
  return captions;
}

// Parse the user's text input into timed caption blocks.
// A line starting with [mm:ss] or [mm:ss.xx] starts a new caption at that time;
// continuation lines (no leading [) get appended to the previous caption.
// If NO timestamps are present anywhere, fall back to even-distribution by
// blank-line-separated paragraphs.
function parseTextToCaptions(text, audioDuration, syncedLyricsRaw) {
  // Strip AI-tool citation noise that users frequently paste from LLM output.
  // E.g. "[cite_start]Tu sais...[cite : 1]" becomes "Tu sais..."
  text = (text || '')
    .replace(/\[cite[^\]]*\]/gi, '')      // [cite : 1], [cite_start], [cite_end]
    .replace(/\[citation[^\]]*\]/gi, '')  // [citation needed]
    .replace(/\u00a0/g, ' ')                 // nbsp
    .trim();

  if (!text) return [];

  // Inline timestamp splitter: works whether the user pasted multi-line OR one
  // long line with [mm:ss] markers embedded mid-sentence.
  //   "Tu sais... [0:13] À prendre... [0:18] Tu rigoles..."
  // becomes captions:
  //   { start: 0,  end: 13, text: "Tu sais..." }      (only if 0:00 implied — see below)
  //   { start: 13, end: 18, text: "À prendre..." }
  //   { start: 18, end: ?,  text: "Tu rigoles..." }
  const timestampRe = /\[(\d+):(\d+)(?:\.(\d+))?\]/g;
  const matches = [...text.matchAll(timestampRe)];

  if (matches.length > 0) {
    // Build captions from each timestamp's position to the next
    const captions = [];
    let prevEnd = 0;
    let beforeFirst = text.slice(0, matches[0].index).trim();
    if (beforeFirst) {
      // Text before the first timestamp = intro caption from 0 to first timestamp
      const firstM = matches[0];
      const firstTime = parseInt(firstM[1], 10) * 60 + parseInt(firstM[2], 10) + (firstM[3] ? parseFloat('0.' + firstM[3]) : 0);
      captions.push({ start: 0, end: firstTime, text: beforeFirst });
    }
    for (let idx = 0; idx < matches.length; idx++) {
      const m = matches[idx];
      const time = parseInt(m[1], 10) * 60 + parseInt(m[2], 10) + (m[3] ? parseFloat('0.' + m[3]) : 0);
      const segStart = m.index + m[0].length;
      const segEnd = idx + 1 < matches.length ? matches[idx + 1].index : text.length;
      const segText = text.slice(segStart, segEnd).trim();
      if (segText) {
        const endTime = idx + 1 < matches.length
          ? parseInt(matches[idx+1][1], 10) * 60 + parseInt(matches[idx+1][2], 10) + (matches[idx+1][3] ? parseFloat('0.' + matches[idx+1][3]) : 0)
          : audioDuration;
        captions.push({ start: time, end: Math.max(time + 0.5, endTime), text: segText });
      }
    }
    return captions;
  }

  // No timestamps anywhere — try song-structure alignment if [Refrain]/[Chorus]
  // markers are present and we have synced lyrics for anchors.
  const lines = text.split(/\r?\n/);
  const hasSectionMarker = lines.some(l => /^\s*[\[\(]\s*(refrain|chorus|verse|couplet|parl)/i.test(l));
  if (hasSectionMarker && syncedLyricsRaw) {
    const aligned = alignTextToSongStructure(text, syncedLyricsRaw, audioDuration);
    if (aligned && aligned.length > 0) {
      console.log('[caption] aligned ' + aligned.length + ' lines to song structure');
      return aligned;
    }
  }

  // Even-distribution fallback. Split on newlines first; if user pasted one
  // run-on string, fall back to sentence boundaries so we don't end up with
  // ONE caption spanning the whole song.
  let segments = lines.map(l => l.trim()).filter(Boolean);
  if (segments.length <= 1 && text.trim()) {
    // Run-on paste — split on sentence-ending punctuation
    segments = text.split(/(?<=[.!?])\s+/).map(s => s.trim()).filter(Boolean);
  }
  if (segments.length === 0) return [];

  const segDur = audioDuration / segments.length;
  return segments.map((segText, i) => ({
    start: i * segDur,
    end: (i + 1) * segDur,
    text: segText,
  }));
}

async function buildVideoArtifacts(jobDir, photoPaths, text, audioDuration, audioPath, syncedLyricsRaw, emit) {
  // captions.srt — built from parsed captions (timed or evenly distributed)
  const captions = parseTextToCaptions(text, audioDuration, syncedLyricsRaw);
  let srt = '';
  for (let i = 0; i < captions.length; i++) {
    const c = captions[i];
    srt += `${i + 1}\n${srtTime(c.start)} --> ${srtTime(c.end)}\n${c.text}\n\n`;
  }
  const srtPath = path.join(jobDir, 'captions.srt');
  await fs.writeFile(srtPath, srt);
  console.log('[srt] wrote ' + captions.length + ' captions, ' + srt.length + ' bytes; first 3 start times: ' + captions.slice(0, 3).map(c => c.start.toFixed(1)).join(', ') + 's; last start: ' + (captions.length ? captions[captions.length-1].start.toFixed(1) : '-') + 's; effectiveText length: ' + (text||'').length);

  // Photo transitions land on a UNION of three anchor sources:
  //   * USER text [mm:ss] anchors   — so photo & text changes coincide
  //   * LRCLIB synced-lyric anchors — denser narrative anchors  
  //   * BEAT downbeats (aubio)      — fill any gap > 3s with downbeats so
  //                                   photos always have a musical landing
  console.log('[photo] buildVideoArtifacts: photos=' + photoPaths.length + ', syncedLyricsRaw=' + (syncedLyricsRaw ? syncedLyricsRaw.length + ' chars' : 'MISSING'));
  emit && emit({ phase: 'beat', status: 'analyzing' });

  // Re-parse user text to extract their [mm:ss] anchor times
  const userCaptions = parseTextToCaptions(text, audioDuration, syncedLyricsRaw);
  const userAnchors = userCaptions.map(c => c.start);

  const lrcAnchors = syncedLyricsRaw
    ? parseLrcLines(syncedLyricsRaw).filter(l => l.text.length > 0).map(l => l.time)
    : [];

  const beatResult = audioPath ? await detectBeats(audioPath) : null;
  const downbeats = beatResult ? beatResult.beats.filter((_, i) => i % 4 === 0) : [];

  // Merge with 0.4s tolerance dedup
  const TOL = 0.4;
  function mergeUnique(target, source) {
    for (const t of source) {
      if (t < 0 || t > audioDuration) continue;
      if (target.some(x => Math.abs(x - t) < TOL)) continue;
      target.push(t);
    }
  }
  let anchors = [0];
  mergeUnique(anchors, userAnchors);
  mergeUnique(anchors, lrcAnchors);
  anchors.sort((a, b) => a - b);

  // Fill any gap >3s with downbeats so photos never sit motionless
  if (downbeats.length) {
    const filled = [anchors[0]];
    for (let i = 1; i < anchors.length; i++) {
      const gap = anchors[i] - anchors[i - 1];
      if (gap > 3) {
        for (const db of downbeats) {
          if (db <= anchors[i - 1] + 1) continue;
          if (db >= anchors[i] - 0.5) break;
          if (filled.length === 0 || db - filled[filled.length - 1] > 1.5) {
            filled.push(db);
          }
        }
      }
      filled.push(anchors[i]);
    }
    anchors = filled;
  }

  const haveUser = userAnchors.length >= 4;
  const haveLrc  = lrcAnchors.length  >= 4;
  const haveBeats = downbeats.length > 0;
  let mode;
  if (haveUser && haveLrc && haveBeats) mode = `union · text(${userAnchors.length})+lrc(${lrcAnchors.length})+beat-fill (${anchors.length} total)`;
  else if (haveUser && haveLrc)  mode = `text+lrc anchors (${anchors.length})`;
  else if (haveLrc)              mode = `lrc anchors (${anchors.length})`;
  else if (haveUser)             mode = `text anchors (${anchors.length})`;
  else if (haveBeats)            mode = `beats only (${anchors.length})`;
  else                           mode = `even (no anchors)`;
  let bpm = beatResult ? beatResult.bpm : null;

  console.log('[photo] mode=' + mode + '  N=' + photoPaths.length + '  anchors=' + anchors.length);

  // Walk forward through the song, picking a transition every ~TARGET_PER
  // seconds, snapping to the nearest anchor in [MIN_PER, MAX_PER]. If no
  // anchor falls in that window, force a synthetic transition at MAX_PER so
  // photos NEVER stay on screen longer than 3s.
  // Photos LOOP — if we need more slots than photos, cycle back through.
  const TARGET_PER = 2.0;
  const MIN_PER    = 1.4;
  const MAX_PER    = 3.0;
  let transitionTimes = [0];
  while (transitionTimes[transitionTimes.length - 1] < audioDuration - 0.3) {
    const last = transitionTimes[transitionTimes.length - 1];
    const ideal = last + TARGET_PER;
    const winLo = last + MIN_PER;
    const winHi = last + MAX_PER;
    // Anchors in window
    const inWin = anchors.filter(a => a >= winLo && a <= winHi);
    let next;
    if (inWin.length > 0) {
      next = inWin.reduce((b, c) => Math.abs(c - ideal) < Math.abs(b - ideal) ? c : b);
    } else {
      next = Math.min(audioDuration, last + MAX_PER);
    }
    transitionTimes.push(next);
  }
  // Force last transition to audioDuration
  transitionTimes[transitionTimes.length - 1] = audioDuration;
  console.log('[photo] transitions=' + (transitionTimes.length - 1) + '  photos=' + photoPaths.length + '  cycle=' + Math.ceil((transitionTimes.length - 1) / photoPaths.length) + 'x');

  emit && emit({
    phase: 'beat',
    status: 'detected',
    bpm,
    beats: anchors.length,
    mode: haveUser && haveLrc ? 'union' : (haveLrc ? 'lyric' : (haveBeats ? 'beat' : 'even')),
  });

  // Shuffle photos so adjacent transitions show visually different images.
  // Google Photos exports chronologically — sequential photos (same beach
  // moment, same family meal) look near-identical so consecutive transitions
  // are perceived as 'stuck'. Fisher-Yates with a deterministic seed keeps
  // shuffles reproducible across re-renders of the same album.
  let seed = photoPaths.reduce((s, p) => (s + p.length) * 31, 0) & 0x7fffffff;
  function rand() { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed / 0x7fffffff; }
  const shuffled = photoPaths.slice();
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
  }
  // After shuffle, also re-shuffle every cycle so the second/third cycle
  // through the album doesn't repeat the same order (visual variety).
  function cyclicPhoto(slotIdx) {
    const cycle = Math.floor(slotIdx / shuffled.length);
    const offset = (slotIdx + cycle * 37) % shuffled.length; // 37 is coprime
    return shuffled[offset];
  }

  // Build ffconcat playlist
  const slots = transitionTimes.length - 1;
  const concatLines = ['ffconcat version 1.0'];
  let lastFile = '';
  for (let i = 0; i < slots; i++) {
    const dur = transitionTimes[i + 1] - transitionTimes[i];
    const f = cyclicPhoto(i);
    concatLines.push(`file '${f}'`);
    concatLines.push(`duration ${dur.toFixed(4)}`);
    lastFile = f;
  }
  concatLines.push(`file '${lastFile}'`);
  console.log('[concat] slots=' + slots + ' unique_photos_used=' + new Set(Array.from({length: slots}, (_, i) => cyclicPhoto(i))).size);
  const concatPath = path.join(jobDir, 'photos.concat');
  await fs.writeFile(concatPath, concatLines.join('\n'));

  return {
    concatPath,
    srtPath,
    captionCount: captions.length,
    mode,
    bpm,
  };
}

// Beat detection — runs aubio's "beat" tracker against the audio file and
// returns an array of beat times in seconds. Falls back to null on failure;
// callers should then use even-spaced photo timing.
function detectBeats(audioPath) {
  return new Promise((resolve) => {
    // `aubio beat` writes one beat-time per line to stdout. -B 1024 -H 256 are
    // the default analysis windows; tweaking these doesn't change beats much.
    const proc = spawn('aubio', ['beat', audioPath]);
    let out = '', err = '';
    proc.stdout.on('data', (c) => (out += c.toString()));
    proc.stderr.on('data', (c) => (err += c.toString()));
    proc.on('error', () => resolve(null));
    proc.on('close', (code) => {
      if (code !== 0) {
        console.warn('[beat] aubio exited', code, err.slice(0, 200));
        return resolve(null);
      }
      const beats = out.split(/\r?\n/).map(parseFloat).filter((n) => !isNaN(n) && n > 0);
      if (beats.length < 8) {
        console.warn('[beat] only', beats.length, 'beats — falling back to even spacing');
        return resolve(null);
      }
      // Compute median BPM for logging
      const intervals = [];
      for (let i = 1; i < beats.length; i++) intervals.push(beats[i] - beats[i - 1]);
      intervals.sort((a, b) => a - b);
      const medianInterval = intervals[Math.floor(intervals.length / 2)];
      const bpm = Math.round(60 / medianInterval);
      console.log(`[beat] detected ${beats.length} beats, ~${bpm} BPM`);
      resolve({ beats, bpm });
    });
  });
}

function runFFmpeg(jobDir, concatPath, srtPath, audioPath, audioDuration, options, emit) {
  return new Promise((resolve, reject) => {
    const outputPath = path.join(jobDir, 'output.mp4');
    const [W, H] = options.resolution.split('x').map(Number);

    // Subtitles ASS style
    // Alignment: 2 = bottom-center, 5 = middle-center
    // Colors are &HBBGGRR& or &HAABBGGRR& with alpha (where AA: 00=opaque, FF=transparent)
    // Font: Georgia is on every Mac/Windows. Linux/Docker may not have it —
    // override via FONT_NAME env var (docker-compose sets it to "Liberation Serif").
    const fontName = process.env.FONT_NAME || 'Georgia';
    // Subtitle styling tuned for 720p — smaller font + tighter box + per-resolution scaling
    // (ffmpeg's subtitles filter renders the ASS at the playback resolution; sizes here
    //  are in pixels of the output video.)
    const fontSize = H >= 1080 ? 28 : H >= 900 ? 24 : 18;
    const outlineW = H >= 1080 ? 3 : 2;
    const marginV  = options.textPosition === 'center' ? 0 : (H >= 1080 ? 60 : 40);
    const subStyle = [
      `Fontname=${fontName}`,
      `Fontsize=${fontSize}`,
      `PrimaryColour=&H00F2F7FA&`,
      `BorderStyle=3`,
      `BackColour=&H99000000&`, // 0x99 = ~60% opaque black band
      `Outline=${outlineW}`,
      `Shadow=0`,
      `Alignment=${options.textPosition === 'center' ? 5 : 2}`,
      `MarginV=${marginV}`,
      `MarginL=${H >= 1080 ? 100 : 60}`,
      `MarginR=${H >= 1080 ? 100 : 60}`,
    ].join(',');

    const subFilter = `subtitles='${escapeSubtitlesPath(srtPath)}':force_style='${subStyle}'`;

    // Video filter chain on the framerate-input stream:
    //   - normalize to target resolution by cover-fit (scale up + crop)
    //   - 30 fps output
    //   - burn subtitles
    const vf = [
      `scale=${W}:${H}:force_original_aspect_ratio=increase`,
      `crop=${W}:${H}`,
      `setsar=1`,
      `fps=30`,
      ...(options.includeText ? [subFilter] : []),
    ].join(',');

    const args = [
      '-y',
      '-loglevel', 'info',
      '-progress', 'pipe:2',  // progress info to stderr
      // Slideshow: concat demuxer with explicit per-photo durations (so we can
      //   beat-sync). 'safe 0' lets us reference absolute paths.
      '-f', 'concat',
      '-safe', '0',
      '-i', concatPath,
      '-i', audioPath,
      '-vf', vf,
      '-map', '0:v',
      '-map', '1:a',
      '-threads', '1',
      '-c:v', 'libx264',
      '-preset', options.preset || 'veryfast',
      '-crf', '22',
      '-pix_fmt', 'yuv420p',
      '-c:a', 'aac',
      '-b:a', '192k',
      '-shortest',
      outputPath,
    ];

    emit({ phase: 'render', status: 'start' });

    const proc = spawn('ffmpeg', args);

    proc.stderr.on('data', (chunk) => {
      const text = chunk.toString();
      // -progress output is key=value lines
      const m = text.match(/out_time_ms=(\d+)/);
      if (m) {
        const seconds = parseInt(m[1], 10) / 1_000_000;
        const fraction = Math.min(1, seconds / audioDuration);
        emit({
          phase: 'render',
          status: 'progress',
          fraction,
          current: seconds,
          total: audioDuration,
        });
      }
    });

    proc.on('error', reject);
    proc.on('close', (code) => {
      if (code !== 0) return reject(new Error(`ffmpeg exited with code ${code}`));
      emit({ phase: 'render', status: 'done', file: outputPath });
      resolve(outputPath);
    });
  });
}

// ───────────────────────────────────────────────────────────────
// Main render endpoint
// ───────────────────────────────────────────────────────────────
app.post('/api/render', upload.single('audioFile'), async (req, res) => {
  const emit = makeEmitter(res);

  const { photosUrl, audioUrl, text, syncedLyrics, title } = req.body || {};
  let options = req.body?.options || {};
  if (typeof options === 'string') {
    try { options = JSON.parse(options); } catch { options = {}; }
  }
  const opts = {
    resolution: options.resolution === '1080p' ? '1920x1080' : '1280x720',
    textPosition: options.textPosition || 'bottom', // 'bottom' | 'center'
    includeText: options.includeText !== false,
    preset: options.preset || 'veryfast',
  };

  try {
    if (!photosUrl) throw new Error('Missing photos URL.');
    if (!audioUrl && !req.file) throw new Error('Provide either an audio file or a YouTube URL.');
    // If user didn't provide custom text but we have synced lyrics from LRCLIB,
    // use those directly as the caption source. This is the default "original
    // lyrics" path — no editing required.
    let effectiveText = text;
    if ((!effectiveText || !effectiveText.trim()) && syncedLyrics) {
      effectiveText = syncedLyrics;
      console.log('[render] using LRCLIB synced lyrics as captions (' + syncedLyrics.length + ' chars)');
    }
    if (opts.includeText && (!effectiveText || !effectiveText.trim())) throw new Error('No captions: provide either custom text or run Analyze first to fetch synced lyrics.');

    const jobId = randomUUID().slice(0, 8);
    const jobDir = path.join(JOBS_DIR, jobId);
    await fs.mkdir(jobDir, { recursive: true });
    emit({ phase: 'init', jobId, dir: jobDir });

    // 1. Photos
    const photoUrls = await scrapeGooglePhotos(photosUrl, emit);
    const photoDir = path.join(jobDir, 'photos');
    const photoPaths = await downloadPhotos(photoUrls, photoDir, emit);

    // 2. Audio — uploaded file wins; otherwise fall back to yt-dlp
    let audioPath;
    if (req.file) {
      // Move the uploaded MP3/WAV/etc into the job dir as music.<ext>
      const ext = path.extname(req.file.originalname || '').toLowerCase() || '.mp3';
      audioPath = path.join(jobDir, 'music' + ext);
      await fs.rename(req.file.path, audioPath);
      emit({ phase: 'audio', status: 'uploaded', filename: req.file.originalname });
      emit({ phase: 'audio', status: 'done' });
    } else {
      audioPath = await downloadAudio(audioUrl, jobDir, emit);
    }
    const audioDuration = await probeDuration(audioPath);
    emit({ phase: 'audio', status: 'duration', seconds: audioDuration });

    // 3. Build SRT from text (timestamped or evenly distributed) + photo timing
    const { concatPath, srtPath, captionCount, mode, bpm } = await buildVideoArtifacts(
      jobDir, photoPaths, effectiveText, audioDuration, audioPath, syncedLyrics, emit
    );
    emit({
      phase: 'plan',
      photos: photoPaths.length,
      captions: captionCount,
      audioDuration,
      mode,
      bpm,
    });

    // 4. Render
    const outputPath = await runFFmpeg(
      jobDir, concatPath, srtPath, audioPath, audioDuration, opts, emit
    );

    const stat = await fs.stat(outputPath);
    const fileName = buildFilename(title, new Date());
    emit({
      phase: 'complete',
      jobId,
      filename: fileName,
      downloadUrl: `/api/file/${jobId}?name=${encodeURIComponent(fileName)}`,
      sizeBytes: stat.size,
      sizeMB: +(stat.size / (1024 * 1024)).toFixed(1),
    });
    res.end();
  } catch (err) {
    console.error('Render error:', err);
    emit({ phase: 'error', message: err.message || String(err) });
    res.end();
  }
});

// ───────────────────────────────────────────────────────────────
// Song analysis endpoint — fast (no audio download)
// ───────────────────────────────────────────────────────────────
app.post('/api/analyze', upload.single('audioFile'), async (req, res) => {
  // Streaming JSONL: progress events + a final {phase:'complete', result:...}.
  res.setHeader('Content-Type', 'application/x-ndjson');
  const emit = (e) => res.write(JSON.stringify(e) + '\n');
  try {
    const { audioUrl, photosUrl } = req.body || {};
    if (req.file) {
      if (!photosUrl) {
        emit({ phase: 'error', message: 'Provide a Google Photos shared URL alongside the audio file.' });
        return res.end();
      }
      const result = await analyzeFromFile(req.file.path, photosUrl, req.file.originalname, emit);
      try { await fs.unlink(req.file.path); } catch {}
      emit({ phase: 'complete', result });
      return res.end();
    }
    if (!audioUrl) {
      emit({ phase: 'error', message: 'Provide an audio file (and photos URL).' });
      return res.end();
    }
    const result = await analyzeSong(audioUrl);
    emit({ phase: 'complete', result });
    res.end();
  } catch (err) {
    console.error('Analyze error:', err);
    emit({ phase: 'error', message: err.message || String(err) });
    res.end();
  }
});

// ───────────────────────────────────────────────────────────────
// Download endpoint
// ───────────────────────────────────────────────────────────────
app.get('/api/file/:jobId', async (req, res) => {
  const jobId = req.params.jobId.replace(/[^a-z0-9-]/gi, '');
  const file = path.join(JOBS_DIR, jobId, 'output.mp4');
  if (!existsSync(file)) {
    return res.status(404).send('Not found');
  }
  const requested = (req.query.name || `souvenir-${jobId}.mp4`).replace(/[^A-Za-z0-9._-]/g, '_');
  res.setHeader('Content-Type', 'video/mp4');
  res.setHeader('Content-Disposition', `attachment; filename="${requested}"`);
  createReadStream(file).pipe(res);
});

// Helper: list past jobs (handy for re-downloading)
app.get('/api/jobs', async (req, res) => {
  const dirs = await fs.readdir(JOBS_DIR);
  const jobs = [];
  for (const d of dirs) {
    const file = path.join(JOBS_DIR, d, 'output.mp4');
    if (existsSync(file)) {
      const s = await fs.stat(file);
      jobs.push({
        jobId: d,
        sizeMB: +(s.size / (1024 * 1024)).toFixed(1),
        createdAt: s.mtime.toISOString(),
      });
    }
  }
  jobs.sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  res.json(jobs);
});

// ───────────────────────────────────────────────────────────────
app.get('/api/version', (_req, res) => res.json(VERSION));

// Diagnostic: run the anchor-walk algorithm with provided inputs and return
// the transition list + anchor counts + selected mode. No audio file needed —
// pass the data as JSON. Used to verify production output matches local.
//
// POST /api/debug/transitions
//   { text, audioDuration, syncedLyrics?, photoCount, bpm? }
app.post('/api/debug/transitions', express.json({ limit: '5mb' }), async (req, res) => {
  try {
    const { text = '', audioDuration = 240, syncedLyrics, photoCount = 80, bpm = 120 } = req.body || {};

    // Re-parse to extract user anchors via the same path as the real flow
    const userCaptions = parseTextToCaptions(text, audioDuration, syncedLyrics);
    const userAnchors = userCaptions.map(c => c.start);
    const lrcAnchors = syncedLyrics
      ? parseLrcLines(syncedLyrics).filter(l => l.text.length > 0).map(l => l.time)
      : [];

    // Simulated downbeats (we have no audio file here)
    const beatInterval = (60 / bpm) * 4;
    const downbeats = [];
    for (let t = 0; t < audioDuration; t += beatInterval) downbeats.push(+t.toFixed(2));

    const TOL = 0.4;
    function mergeUnique(target, source) {
      for (const t of source) {
        if (t < 0 || t > audioDuration) continue;
        if (target.some(x => Math.abs(x - t) < TOL)) continue;
        target.push(t);
      }
    }
    let anchors = [0];
    mergeUnique(anchors, userAnchors);
    mergeUnique(anchors, lrcAnchors);
    anchors.sort((a, b) => a - b);
    const anchorsBeforeFill = anchors.length;

    if (downbeats.length) {
      const filled = [anchors[0]];
      for (let i = 1; i < anchors.length; i++) {
        const gap = anchors[i] - anchors[i - 1];
        if (gap > 3) {
          for (const db of downbeats) {
            if (db <= anchors[i - 1] + 1) continue;
            if (db >= anchors[i] - 0.5) break;
            if (filled.length === 0 || db - filled[filled.length - 1] > 1.5) {
              filled.push(db);
            }
          }
        }
        filled.push(anchors[i]);
      }
      anchors = filled;
    }
    const anchorsAfterFill = anchors.length;

    const TARGET_PER = 2.0, MIN_PER = 1.4, MAX_PER = 3.0;
    let transitionTimes = [0];
    let safety = 5000;
    while (transitionTimes[transitionTimes.length - 1] < audioDuration - 0.3 && safety-- > 0) {
      const last = transitionTimes[transitionTimes.length - 1];
      const ideal = last + TARGET_PER;
      const winLo = last + MIN_PER;
      const winHi = last + MAX_PER;
      const inWin = anchors.filter(a => a >= winLo && a <= winHi);
      let next;
      if (inWin.length > 0) {
        next = inWin.reduce((b, c) => Math.abs(c - ideal) < Math.abs(b - ideal) ? c : b);
      } else {
        next = Math.min(audioDuration, last + MAX_PER);
      }
      transitionTimes.push(next);
    }
    transitionTimes[transitionTimes.length - 1] = audioDuration;

    const gaps = [];
    for (let i = 1; i < transitionTimes.length; i++) gaps.push(transitionTimes[i] - transitionTimes[i-1]);
    gaps.sort((a, b) => a - b);

    res.json({
      inputs: { audioDuration, photoCount, bpm, textLen: (text || '').length, syncedLyricsLen: (syncedLyrics || '').length },
      anchors: {
        userAnchorsCount: userAnchors.length,
        lrcAnchorsCount: lrcAnchors.length,
        downbeatsCount: downbeats.length,
        anchorsBeforeFill,
        anchorsAfterFill,
      },
      transitions: {
        count: transitionTimes.length - 1,
        minGap: gaps.length ? +gaps[0].toFixed(2) : 0,
        maxGap: gaps.length ? +gaps[gaps.length-1].toFixed(2) : 0,
        medianGap: gaps.length ? +gaps[Math.floor(gaps.length/2)].toFixed(2) : 0,
        first20: transitionTimes.slice(0, 20).map(t => +t.toFixed(2)),
        last20: transitionTimes.slice(-20).map(t => +t.toFixed(2)),
      },
      versionRunning: VERSION,
    });
  } catch (e) {
    res.status(500).json({ error: e.message, stack: e.stack });
  }
});

// Health-check: returns whether each runtime tool is actually present on the
// host. Useful for verifying aubio / ffmpeg / yt-dlp install on a fresh deploy.
app.get('/api/healthz', async (_req, res) => {
  async function probe(cmd, arg) {
    return new Promise((resolve) => {
      const p = spawn(cmd, [arg]);
      let out = '';
      p.stdout.on('data', (c) => (out += c.toString()));
      p.stderr.on('data', (c) => (out += c.toString()));
      p.on('error', () => resolve({ ok: false, error: 'not found' }));
      p.on('close', (code) => resolve({
        ok: code === 0,
        version: (out.split(/\r?\n/)[0] || '').slice(0, 100),
      }));
    });
  }
  const tools = {
    ffmpeg:  await probe('ffmpeg',  '-version'),
    ffprobe: await probe('ffprobe', '-version'),
    aubio:   await probe('aubio',   '--version'),
    'yt-dlp': await probe('yt-dlp', '--version'),
  };
  res.json({ version: VERSION, tools });
});

app.listen(PORT, () => {
  console.log('');
  console.log('  Souvenir running at http://localhost:' + PORT);
  console.log('  Build #' + VERSION.build + ' (v' + VERSION.app + ', commit ' + VERSION.shortCommit + ', booted ' + VERSION.bootedAt + ')');
  console.log('  Jobs stored in: ' + JOBS_DIR);
  console.log('');
});
