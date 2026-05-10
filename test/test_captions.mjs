import { readFileSync } from 'fs';
const src = readFileSync('server.js', 'utf8');
function extractFn(name) {
  const i = src.indexOf('function ' + name);
  if (i < 0) throw new Error('missing ' + name);
  let j = i, depth = 0, started = false;
  while (j < src.length) {
    if (src[j] === '{') { depth++; started = true; }
    else if (src[j] === '}') { depth--; if (started && depth === 0) return src.slice(i, j + 1); }
    j++;
  }
  throw new Error('no closing brace');
}
const code = extractFn('parseLrcLines') + '\n\n' +
             extractFn('alignTextToSongStructure') + '\n\n' +
             extractFn('parseTextToCaptions') + '\n\n' +
             'export { parseTextToCaptions };';
const mod = await import('data:text/javascript,' + encodeURIComponent(code));
const fn = mod.parseTextToCaptions;

let pass = 0, fail = 0;
function t(name, ok, detail) {
  const tag = ok ? 'PASS' : 'FAIL';
  console.log(`  [${tag}] ${name}` + (detail ? `  -- ${detail}` : ''));
  ok ? pass++ : fail++;
}

const t1 = fn("Tu sais cela fait 13 ans [0:13] A prendre des photos [0:18] Tu rigoles [0:22] Tu profites", 270);
t('inline timestamps split into >= 3 captions', t1.length >= 3, `got ${t1.length}`);
t('inline timestamps stripped from text', !t1.some(c => /\[\d+:\d+\]/.test(c.text)), 'no [mm:ss] in text');

const t2 = fn("[cite_start]Tu sais[cite : 1] [0:13] A prendre[cite : 1]", 270);
t('AI citation noise removed', !t2.some(c => /cite/i.test(c.text)));

const t3 = fn("Tu sais cela fait 13 ans. A prendre des photos. Tu rigoles. C'est unique.", 240);
t('run-on line splits on sentences (>=3)', t3.length >= 3, `got ${t3.length}`);

const t4 = fn("[0:08] First\n[0:13] Second\n[0:20] Third", 270);
t('multi-line [mm:ss] format works', t4.length === 3 && t4[0].start === 8, `got ${JSON.stringify(t4.map(c => c.start))}`);

const t5 = fn("", 240);
t('empty input returns []', t5.length === 0);

const t6 = fn("   \n  ", 240);
t('whitespace-only returns []', t6.length === 0);

console.log(`\n${pass}/${pass+fail} passed`);
process.exit(fail ? 1 : 0);
