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
  '--extractor-args', 'youtube:player_client=android,web,mweb',
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
  return baseUrls.map((u) => u + '=w2048');
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

// Parse the user's text input into timed caption blocks.
// A line starting with [mm:ss] or [mm:ss.xx] starts a new caption at that time;
// continuation lines (no leading [) get appended to the previous caption.
// If NO timestamps are present anywhere, fall back to even-distribution by
// blank-line-separated paragraphs.
function parseTextToCaptions(text, audioDuration) {
  const lines = text.split(/\r?\n/);
  const timedRe = /^\s*\[(\d+):(\d+)(?:\.(\d+))?\]\s*(.*)$/;

  // Detect: are there ANY timestamped lines?
  const hasTimestamps = lines.some((l) => timedRe.test(l));

  if (hasTimestamps) {
    const captions = [];
    let current = null;
    for (const raw of lines) {
      const m = raw.match(timedRe);
      if (m) {
        if (current) captions.push(current);
        const minutes = parseInt(m[1], 10);
        const seconds = parseInt(m[2], 10);
        const frac = m[3] ? parseFloat('0.' + m[3]) : 0;
        current = { start: minutes * 60 + seconds + frac, lines: [] };
        const tailText = (m[4] || '').trim();
        if (tailText) current.lines.push(tailText);
      } else if (current && raw.trim()) {
        // Continuation line for the current caption
        current.lines.push(raw.trim());
      }
      // Lines outside any caption (no timestamp seen yet, or blank) are ignored
    }
    if (current) captions.push(current);

    // Compute end times: each caption ends when the next begins (or at audio end)
    return captions
      .map((c, i) => {
        const next = captions[i + 1];
        const end = next ? next.start : audioDuration;
        return { start: c.start, end, text: c.lines.join('\n') };
      })
      // Drop empty-text captions (they create gaps — desired behavior)
      .filter((c) => c.text.length > 0 && c.end > c.start);
  }

  // No timestamps — even distribution by paragraph
  const segments = text.split(/\n\s*\n/).map((s) => s.trim()).filter(Boolean);
  if (segments.length === 0) return [];
  const segDur = audioDuration / segments.length;
  return segments.map((text, i) => ({
    start: i * segDur,
    end: (i + 1) * segDur,
    text,
  }));
}

async function buildVideoArtifacts(jobDir, photoPaths, text, audioDuration) {
  // For the framerate-input approach we just need the per-photo duration.
  const perPhotoSec = audioDuration / photoPaths.length;

  // captions.srt — built from parsed captions (timed or evenly distributed)
  const captions = parseTextToCaptions(text, audioDuration);
  let srt = '';
  for (let i = 0; i < captions.length; i++) {
    const c = captions[i];
    srt += `${i + 1}\n${srtTime(c.start)} --> ${srtTime(c.end)}\n${c.text}\n\n`;
  }
  const srtPath = path.join(jobDir, 'captions.srt');
  await fs.writeFile(srtPath, srt);

  // Photo input pattern - all photos are named photo_NNNN.jpg in photoDir
  const photoDir = path.dirname(photoPaths[0]);
  const photoPattern = path.join(photoDir, 'photo_%04d.jpg');

  return { photoPattern, srtPath, perPhotoSec, captionCount: captions.length };
}

function runFFmpeg(jobDir, photoPattern, perPhotoSec, srtPath, audioPath, audioDuration, options, emit) {
  return new Promise((resolve, reject) => {
    const outputPath = path.join(jobDir, 'output.mp4');
    const [W, H] = options.resolution.split('x').map(Number);

    // Subtitles ASS style
    // Alignment: 2 = bottom-center, 5 = middle-center
    // Colors are &HBBGGRR& or &HAABBGGRR& with alpha (where AA: 00=opaque, FF=transparent)
    // Font: Georgia is on every Mac/Windows. Linux/Docker may not have it —
    // override via FONT_NAME env var (docker-compose sets it to "Liberation Serif").
    const fontName = process.env.FONT_NAME || 'Georgia';
    const subStyle = [
      `Fontname=${fontName}`,
      `Fontsize=22`,
      `PrimaryColour=&H00F2F7FA&`,
      `BorderStyle=3`,
      `BackColour=&H99000000&`, // 0x99 = ~60% opaque black band
      `Outline=4`,
      `Shadow=0`,
      `Alignment=${options.textPosition === 'center' ? 5 : 2}`,
      `MarginV=${options.textPosition === 'center' ? 0 : 60}`,
      `MarginL=80`,
      `MarginR=80`,
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
      // Slideshow: framerate input, each image gets perPhotoSec seconds
      '-framerate', `1/${perPhotoSec.toFixed(6)}`,
      '-i', photoPattern,
      '-i', audioPath,
      '-vf', vf,
      '-map', '0:v',
      '-map', '1:a',
      '-c:v', 'libx264',
      '-preset', options.preset || 'medium',
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
app.post('/api/render', async (req, res) => {
  const emit = makeEmitter(res);

  const { photosUrl, audioUrl, text, options = {} } = req.body || {};
  const opts = {
    resolution: options.resolution === '720p' ? '1280x720' : '1920x1080',
    textPosition: options.textPosition || 'bottom', // 'bottom' | 'center'
    includeText: options.includeText !== false,
    preset: options.preset || 'medium',
  };

  try {
    if (!photosUrl) throw new Error('Missing photos URL.');
    if (!audioUrl) throw new Error('Missing audio URL.');
    if (!text || !text.trim()) throw new Error('Missing text.');

    const jobId = randomUUID().slice(0, 8);
    const jobDir = path.join(JOBS_DIR, jobId);
    await fs.mkdir(jobDir, { recursive: true });
    emit({ phase: 'init', jobId, dir: jobDir });

    // 1. Photos
    const photoUrls = await scrapeGooglePhotos(photosUrl, emit);
    const photoDir = path.join(jobDir, 'photos');
    const photoPaths = await downloadPhotos(photoUrls, photoDir, emit);

    // 2. Audio
    const audioPath = await downloadAudio(audioUrl, jobDir, emit);
    const audioDuration = await probeDuration(audioPath);
    emit({ phase: 'audio', status: 'duration', seconds: audioDuration });

    // 3. Build SRT from text (timestamped or evenly distributed) + photo timing
    const { photoPattern, srtPath, perPhotoSec, captionCount } = await buildVideoArtifacts(
      jobDir, photoPaths, text, audioDuration
    );
    emit({
      phase: 'plan',
      photos: photoPaths.length,
      captions: captionCount,
      audioDuration,
      perPhotoSec,
    });

    // 4. Render
    const outputPath = await runFFmpeg(
      jobDir, photoPattern, perPhotoSec, srtPath, audioPath, audioDuration, opts, emit
    );

    const stat = await fs.stat(outputPath);
    emit({
      phase: 'complete',
      jobId,
      downloadUrl: `/api/file/${jobId}`,
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
app.post('/api/analyze', async (req, res) => {
  try {
    const { audioUrl } = req.body || {};
    if (!audioUrl) return res.status(400).json({ error: 'Missing audioUrl' });
    const result = await analyzeSong(audioUrl);
    res.json(result);
  } catch (err) {
    console.error('Analyze error:', err);
    res.status(500).json({ error: err.message || String(err) });
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
  res.setHeader('Content-Type', 'video/mp4');
  res.setHeader('Content-Disposition', `attachment; filename="souvenir-${jobId}.mp4"`);
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
app.listen(PORT, () => {
  console.log('');
  console.log('  Souvenir running at http://localhost:' + PORT);
  console.log('  Jobs stored in: ' + JOBS_DIR);
  console.log('');
});
