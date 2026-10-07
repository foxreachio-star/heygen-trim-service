const express = require("express");
const { spawn } = require("child_process");
const { Readable } = require("stream");
const fs = require("fs");
const path = require("path");
const os = require("os");
const crypto = require("crypto");

const app = express();
// Accept anything as text: bodies are parsed manually in /trim so a slightly
// malformed JSON body (or a bare URL) still works instead of erroring.
app.use(express.text({ type: "*/*", limit: "1mb" }));

const PORT = process.env.PORT || 8080;
const AUTH_TOKEN = process.env.AUTH_TOKEN || "";
const NOISE_DB = process.env.NOISE_DB || "-35dB";
const MIN_SILENCE = process.env.MIN_SILENCE || "0.35";
const KEEP_PAUSE = parseFloat(process.env.KEEP_PAUSE || "0.15");
const MAX_DOWNLOAD_MB = parseInt(process.env.MAX_DOWNLOAD_MB || "500", 10);
const FILE_TTL_MS = parseInt(process.env.FILE_TTL_MINUTES || "60", 10) * 60 * 1000;
// ffmpeg is memory/CPU heavy: bursts of simultaneous videos must queue, not crash.
const MAX_CONCURRENT = parseInt(process.env.MAX_CONCURRENT || "1", 10);
const FFMPEG_TIMEOUT_MS = parseInt(process.env.FFMPEG_TIMEOUT_SECONDS || "180", 10) * 1000;

const SERVE_DIR = path.join(os.tmpdir(), "served");
fs.mkdirSync(SERVE_DIR, { recursive: true });
for (const f of fs.readdirSync(SERVE_DIR)) {
  try { fs.unlinkSync(path.join(SERVE_DIR, f)); } catch {}
}

// ---- tiny job queue: at most MAX_CONCURRENT ffmpeg pipelines at a time ----
const queue = [];
let active = 0;
function runQueued(fn) {
  return new Promise((resolve, reject) => {
    queue.push({ fn, resolve, reject });
    pump();
  });
}
function pump() {
  while (active < MAX_CONCURRENT && queue.length) {
    const job = queue.shift();
    active++;
    job.fn().then(
      (v) => { active--; job.resolve(v); pump(); },
      (e) => { active--; job.reject(e); pump(); }
    );
  }
}

app.get("/health", (_req, res) => res.json({ ok: true, active, queued: queue.length }));

function runFfmpeg(args) {
  return new Promise((resolve, reject) => {
    const p = spawn("ffmpeg", ["-hide_banner", "-nostdin", "-nostats", ...args]);
    let stderr = "";
    const timer = setTimeout(() => p.kill("SIGKILL"), FFMPEG_TIMEOUT_MS);
    p.stderr.on("data", (d) => {
      stderr += d.toString();
      if (stderr.length > 65536) stderr = stderr.slice(-32768);
    });
    p.on("error", (e) => { clearTimeout(timer); reject(e); });
    p.on("close", (code, signal) => {
      clearTimeout(timer);
      if (signal) return reject(new Error(`ffmpeg killed (${signal}) after ${FFMPEG_TIMEOUT_MS / 1000}s`));
      if (code === 0) resolve(stderr);
      else reject(new Error(`ffmpeg exited ${code}: ${stderr.trim().slice(-600)}`));
    });
  });
}

// Stream to disk (no big RAM buffers) with a size cap and a not-a-video check.
async function download(url, dest) {
  const res = await fetch(url, { redirect: "follow" });
  if (!res.ok) throw new Error(`download failed: HTTP ${res.status} from source URL`);
  const ct = (res.headers.get("content-type") || "").toLowerCase();
  if (ct.includes("text/html") || ct.includes("application/json"))
    throw new Error(`source URL returned ${ct}, not a video — the link may be expired or wrong`);
  const cap = MAX_DOWNLOAD_MB * 1024 * 1024;
  let bytes = 0;
  await new Promise((resolve, reject) => {
    const ws = fs.createWriteStream(dest);
    const rs = Readable.fromWeb(res.body);
    rs.on("data", (chunk) => {
      bytes += chunk.length;
      if (bytes > cap) rs.destroy(new Error(`file too large (over ${MAX_DOWNLOAD_MB} MB)`));
    });
    rs.on("error", (e) => { ws.destroy(); reject(e); });
    ws.on("error", reject);
    ws.on("finish", resolve);
    rs.pipe(ws);
  });
  if (bytes === 0) throw new Error("downloaded 0 bytes from source URL");
}

function parseSilences(log) {
  const durMatch = log.match(/Duration: (\d+):(\d+):([\d.]+)/);
  if (!durMatch) throw new Error("could not read video duration — file is not a valid video");
  const duration =
    3600 * parseInt(durMatch[1], 10) +
    60 * parseInt(durMatch[2], 10) +
    parseFloat(durMatch[3]);
  const starts = [...log.matchAll(/silence_start: ([\d.]+)/g)].map((m) => parseFloat(m[1]));
  const ends = [...log.matchAll(/silence_end: ([\d.]+)/g)].map((m) => parseFloat(m[1]));
  return { duration, starts, ends };
}

function buildSegments(duration, starts, ends) {
  const segs = [];
  let t = 0.0;
  for (let i = 0; i < Math.min(starts.length, ends.length); i++) {
    const cutStart = starts[i] + KEEP_PAUSE;
    if (cutStart > t) segs.push([t, cutStart]);
    t = ends[i];
  }
  if (t < duration) segs.push([t, duration]);
  return segs;
}

function publish(filePath, req) {
  const name = crypto.randomBytes(16).toString("hex") + ".mp4";
  fs.renameSync(filePath, path.join(SERVE_DIR, name));
  setTimeout(() => {
    try { fs.unlinkSync(path.join(SERVE_DIR, name)); } catch {}
  }, FILE_TTL_MS);
  return `https://${req.get("host")}/files/${name}`;
}

app.get("/files/:name", (req, res) => {
  const name = req.params.name;
  if (!/^[0-9a-f]{32}\.mp4$/.test(name)) return res.status(404).end();
  const file = path.join(SERVE_DIR, name);
  if (!fs.existsSync(file)) return res.status(404).json({ error: "expired or not found" });
  res.setHeader("Content-Type", "video/mp4");
  fs.createReadStream(file).pipe(res);
});

// Pull a URL out of the body no matter how it arrived: proper JSON {"url": "..."},
// slightly broken JSON, or a bare URL as plain text.
function extractUrl(rawBody) {
  if (typeof rawBody !== "string" || !rawBody.trim()) return null;
  try {
    const parsed = JSON.parse(rawBody);
    if (parsed && typeof parsed.url === "string" && parsed.url.trim()) return parsed.url.trim();
  } catch {}
  const m = rawBody.match(/https?:\/\/[^\s"'\\}]+/);
  return m ? m[0] : null;
}

app.post("/trim", async (req, res) => {
  if (AUTH_TOKEN && req.headers["x-auth-token"] !== AUTH_TOKEN)
    return res.status(401).json({ error: "unauthorized" });

  let url = extractUrl(req.body);
  if (!url)
    return res.status(400).json({
      error: "no video URL found in request body — the mapped captioned_video_url in Make was probably empty for this record",
    });
  // Internal "minio" hostnames are not publicly resolvable; swap for the public IP
  url = url.replace("minio", "89.167.35.200");

  const id = crypto.randomBytes(8).toString("hex");
  const src = path.join(os.tmpdir(), `src-${id}.mp4`);
  const out = path.join(os.tmpdir(), `out-${id}.mp4`);
  const cleanup = () => {
    for (const f of [src, out]) { try { fs.existsSync(f) && fs.unlinkSync(f); } catch {} }
  };

  try {
    const result = await runQueued(async () => {
      await download(url, src);

      const log = await runFfmpeg([
        "-i", src,
        "-af", `silencedetect=noise=${NOISE_DB}:d=${MIN_SILENCE}`,
        "-f", "null", "-",
      ]);
      const { duration, starts, ends } = parseSilences(log);
      const segs = buildSegments(duration, starts, ends);

      if (starts.length === 0 || segs.length === 0) {
        return { file: src, trimmed: false, before: duration, after: duration, note: "no silences found" };
      }

      const sel = segs.map(([a, b]) => `between(t,${a.toFixed(3)},${b.toFixed(3)})`).join("+");
      try {
        await runFfmpeg([
          "-loglevel", "error",
          "-y", "-i", src,
          "-vf", `select='${sel}',setpts=N/FRAME_RATE/TB`,
          "-af", `aselect='${sel}',asetpts=N/SR/TB`,
          "-preset", "veryfast",
          "-movflags", "+faststart",
          out,
        ]);
      } catch (encErr) {
        // Trimming is best-effort: if the re-encode fails, deliver the original
        // untouched rather than breaking the pipeline.
        console.error(`[trim ${id}] encode failed, returning original:`, encErr.message);
        return { file: src, trimmed: false, before: duration, after: duration, note: `trim skipped: ${encErr.message.slice(0, 200)}` };
      }
      const after = segs.reduce((s, [a, b]) => s + (b - a), 0);
      return { file: out, trimmed: true, before: duration, after };
    });

    const fileUrl = publish(result.file, req);
    cleanup();
    res.setHeader("X-Trimmed", String(result.trimmed));
    res.setHeader("X-Duration-Before", result.before.toFixed(2));
    res.setHeader("X-Duration-After", result.after.toFixed(2));
    if (result.note) res.setHeader("X-Trim-Note", result.note.replace(/[^\x20-\x7e]/g, " "));
    res.type("text/plain").send(fileUrl);
  } catch (err) {
    cleanup();
    console.error(`[trim ${id}]`, err.message);
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => console.log(`trim service v2 listening on :${PORT} (max ${MAX_CONCURRENT} concurrent trims)`));
