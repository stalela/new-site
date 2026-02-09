/**
 * generate-images.ts
 * ──────────────────
 * Generates marketing-site images using Alibaba DashScope (Model Studio)
 * image generation API with the qwen-image-max model.
 *
 * Usage:
 *   npx tsx scripts/generate-images.ts              # generate all images
 *   npx tsx scripts/generate-images.ts --only hero   # generate one by key
 */

import * as fs from "fs";
import * as path from "path";
import * as https from "https";

// Load env before anything else
try {
  require("dotenv").config({ path: path.resolve(__dirname, "../.env.local") });
} catch {
  // dotenv not installed; env must be set manually
}

// ── Config ────────────────────────────────────────────────────────────
const API_KEY = process.env.DASHSCOPE_API_KEY;
if (!API_KEY) {
  console.error("❌  DASHSCOPE_API_KEY not set. Create .env.local first.");
  process.exit(1);
}

const BASE_URL = "https://dashscope-intl.aliyuncs.com";
const MODEL = "qwen-image-max";
const NEGATIVE_PROMPT =
  "low resolution, low quality, deformed limbs, deformed fingers, oversaturated, waxy, no facial details, overly smooth, AI-like, chaotic composition, blurry text, distorted text.";
const OUTPUT_DIR = path.resolve(__dirname, "../public/images/generated");

// Brand-style prefix injected into every prompt
const BRAND_PREFIX =
  "Modern, clean, professional illustration in a warm copper and charcoal color palette (#A4785A, #1C1C1C) with soft gradients and subtle geometric accents. South African business context.";

// ── Image Definitions ─────────────────────────────────────────────────
interface ImageDef {
  key: string;
  filename: string;
  prompt: string;
  size: string; // "1664*928" widescreen, "1024*1024" square
  priority: "high" | "medium" | "low";
}

const images: ImageDef[] = [
  // ─ Homepage ─
  {
    key: "hero",
    filename: "hero-banner.png",
    prompt: `${BRAND_PREFIX} A confident South African entrepreneur at a modern desk, city skyline visible through floor-to-ceiling windows. Laptop open, documents organized. Feeling of control and momentum. Wide cinematic composition.`,
    size: "1664*928",
    priority: "high",
  },
  {
    key: "how-it-works",
    filename: "how-it-works.png",
    prompt: `${BRAND_PREFIX} Four connected steps shown as a horizontal flow: submit details → register company → stay compliant → focus on business. Clean iconographic style with connecting arrows, isometric perspective.`,
    size: "1664*928",
    priority: "high",
  },
  {
    key: "trust",
    filename: "trust-indicators.png",
    prompt: `${BRAND_PREFIX} Shield icon surrounded by trust symbols: verified checkmark, padlock, handshake, stars. Abstract composition suggesting security and reliability for business services.`,
    size: "1664*928",
    priority: "medium",
  },

  // ─ Category pages ─
  {
    key: "cat-registration",
    filename: "category-registration.png",
    prompt: `${BRAND_PREFIX} Official company registration concept — a certificate with a golden seal, fountain pen, and CIPC-style document on a polished desk. Professional, authoritative.`,
    size: "1664*928",
    priority: "high",
  },
  {
    key: "cat-tax",
    filename: "category-tax-compliance.png",
    prompt: `${BRAND_PREFIX} Tax compliance illustration — organized financial documents, calculator, calendar with highlighted deadlines, and a checkmark. Neat, systematic, reassuring.`,
    size: "1664*928",
    priority: "high",
  },
  {
    key: "cat-banking",
    filename: "category-banking.png",
    prompt: `${BRAND_PREFIX} Business banking concept — modern bank building, digital banking interface on a tablet, credit cards, and a document folder. Professional and trustworthy.`,
    size: "1664*928",
    priority: "high",
  },
  {
    key: "cat-certifications",
    filename: "category-certifications.png",
    prompt: `${BRAND_PREFIX} Certifications and memberships — framed certificates on a wall, award badges, official stamps. Professional achievement and credibility theme.`,
    size: "1664*928",
    priority: "medium",
  },
  {
    key: "cat-digital",
    filename: "category-digital-services.png",
    prompt: `${BRAND_PREFIX} Digital services for business — laptop showing a website, email icons, domain name in a browser bar, brand logo mockup. Modern tech workspace.`,
    size: "1664*928",
    priority: "medium",
  },

  // ─ Static pages ─
  {
    key: "how-it-works-page",
    filename: "how-it-works-page.png",
    prompt: `${BRAND_PREFIX} A timeline of business setup steps: person filling a form on laptop, documents being reviewed, official stamp on certificate, business owner shaking hands. Four-panel horizontal layout.`,
    size: "1664*928",
    priority: "high",
  },
  {
    key: "pricing",
    filename: "pricing-page.png",
    prompt: `${BRAND_PREFIX} Three pricing tiers displayed as elegant cards: Starter, Growth, Managed. Each with distinct visual weight. Clean, minimalist, comparison layout feel.`,
    size: "1664*928",
    priority: "medium",
  },
  {
    key: "contact",
    filename: "contact-page.png",
    prompt: `${BRAND_PREFIX} Contact and communication — person at desk with headset, email envelope floating, phone icon, speech bubbles. Friendly, approachable support theme.`,
    size: "1664*928",
    priority: "medium",
  },
  {
    key: "register",
    filename: "register-page.png",
    prompt: `${BRAND_PREFIX} Business registration starting point — a form on a clipboard being filled out, pen in hand, with a "start here" arrow. Encouraging, first-step energy.`,
    size: "1664*928",
    priority: "medium",
  },

  // ─ Reusable / general ─
  {
    key: "cta-bg",
    filename: "cta-background.png",
    prompt: `${BRAND_PREFIX} Abstract warm gradient background with subtle geometric shapes — triangles and circles in copper tones fading to white. Suitable as a CTA section background. No text.`,
    size: "1664*928",
    priority: "low",
  },
  {
    key: "services-overview",
    filename: "services-overview.png",
    prompt: `${BRAND_PREFIX} Bird's-eye view of a business service ecosystem: icons for registration, tax, banking, compliance, digital all connected in a network diagram. Clean, schematic illustration.`,
    size: "1664*928",
    priority: "medium",
  },
  {
    key: "about-team",
    filename: "about-team.png",
    prompt: `${BRAND_PREFIX} Diverse South African professional team in a modern office — collaborative meeting scene, whiteboards, laptops. Warm lighting, inclusive, energetic.`,
    size: "1664*928",
    priority: "low",
  },
];

// ── API Helpers ───────────────────────────────────────────────────────

function request(
  method: string,
  urlPath: string,
  body?: object,
  extraHeaders?: Record<string, string>,
): Promise<any> {
  return new Promise((resolve, reject) => {
    const url = new URL(urlPath, BASE_URL);
    const data = body ? JSON.stringify(body) : undefined;

    const options: https.RequestOptions = {
      hostname: url.hostname,
      path: url.pathname + url.search,
      method,
      headers: {
        Authorization: `Bearer ${API_KEY}`,
        "Content-Type": "application/json",
        ...(data ? { "Content-Length": Buffer.byteLength(data) } : {}),
        ...extraHeaders,
      },
    };

    const req = https.request(options, (res) => {
      let chunks: Buffer[] = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        const raw = Buffer.concat(chunks).toString();
        try {
          resolve(JSON.parse(raw));
        } catch {
          reject(new Error(`Non-JSON response (HTTP ${res.statusCode}): ${raw.slice(0, 500)}`));
        }
      });
    });
    req.on("error", reject);
    if (data) req.write(data);
    req.end();
  });
}

const MAX_RETRIES = 5;
const INITIAL_BACKOFF_MS = 15_000; // 15s initial wait on rate limit

async function generateImage(prompt: string, size: string): Promise<string> {
  const body = {
    model: MODEL,
    input: {
      messages: [
        {
          role: "user",
          content: [
            { text: prompt },
          ],
        },
      ],
    },
    parameters: {
      size,
      negative_prompt: NEGATIVE_PROMPT,
      prompt_extend: true,
      watermark: false,
    },
  };

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const res = await request(
      "POST",
      "/api/v1/services/aigc/multimodal-generation/generation",
      body,
    );

    // Rate limit — back off and retry
    if (res.code === "Throttling.RateQuota") {
      if (attempt === MAX_RETRIES) {
        throw new Error(`Rate limited after ${MAX_RETRIES} retries`);
      }
      const waitMs = INITIAL_BACKOFF_MS * Math.pow(2, attempt);
      process.stdout.write(` ⏳ rate-limited, waiting ${Math.round(waitMs / 1000)}s (retry ${attempt + 1}/${MAX_RETRIES})`);
      await sleep(waitMs);
      continue;
    }

    // Synchronous response: image URL in output.choices[].message.content[]
    if (res.output?.choices) {
      const content = res.output.choices[0]?.message?.content;
      if (Array.isArray(content)) {
        const imgItem = content.find((c: any) => c.image);
        if (imgItem?.image) return imgItem.image;
      }
    }

    // Async task response: poll for completion
    if (res.output?.task_id) {
      process.stdout.write(` (task: ${res.output.task_id}) polling`);
      return await pollTask(res.output.task_id);
    }

    throw new Error(`Unexpected response: ${JSON.stringify(res).slice(0, 500)}`);
  }

  throw new Error("Max retries exceeded");
}

async function pollTask(taskId: string): Promise<string> {
  const maxAttempts = 120; // 10 min at 5s intervals
  for (let i = 0; i < maxAttempts; i++) {
    await sleep(5000);
    const res = await request(
      "GET",
      `/api/v1/tasks/${taskId}`,
    );

    const status = res.output?.task_status;
    if (status === "SUCCEEDED") {
      // Try results array first (wanx-style)
      const url = res.output?.results?.[0]?.url;
      if (url) return url;
      // Try choices array (qwen-style)
      const content = res.output?.choices?.[0]?.message?.content;
      if (Array.isArray(content)) {
        const imgItem = content.find((c: any) => c.image);
        if (imgItem?.image) return imgItem.image;
      }
      throw new Error(`No image URL in succeeded task: ${JSON.stringify(res).slice(0, 500)}`);
    }
    if (status === "FAILED") {
      throw new Error(`Task failed: ${JSON.stringify(res.output)}`);
    }
    // PENDING or RUNNING — keep polling
    process.stdout.write(".");
  }
  throw new Error(`Task ${taskId} timed out after 10 minutes`);
}

async function downloadFile(url: string, dest: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    https.get(url, (res) => {
      if (res.statusCode === 301 || res.statusCode === 302) {
        // Follow redirect
        https.get(res.headers.location!, (res2) => {
          res2.pipe(file);
          file.on("finish", () => {
            file.close();
            resolve();
          });
        });
        return;
      }
      res.pipe(file);
      file.on("finish", () => {
        file.close();
        resolve();
      });
    }).on("error", (err) => {
      fs.unlinkSync(dest);
      reject(err);
    });
  });
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// ── Main ──────────────────────────────────────────────────────────────

async function main() {
  // Parse --only flag
  const onlyArg = process.argv.find((a) => a.startsWith("--only"));
  const onlyKey = onlyArg
    ? process.argv[process.argv.indexOf(onlyArg) + 1]
    : undefined;

  // Ensure output dir
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // Filter images
  let targets = images;
  if (onlyKey) {
    targets = images.filter((img) => img.key === onlyKey);
    if (targets.length === 0) {
      console.error(`❌  No image found with key "${onlyKey}"`);
      console.log("Available keys:", images.map((i) => i.key).join(", "));
      process.exit(1);
    }
  }

  // Sort by priority
  const order = { high: 0, medium: 1, low: 2 };
  targets.sort((a, b) => order[a.priority] - order[b.priority]);

  console.log(`\n🎨  Generating ${targets.length} image(s)...\n`);

  for (const img of targets) {
    const dest = path.join(OUTPUT_DIR, img.filename);
    if (fs.existsSync(dest) && !onlyKey) {
      console.log(`⏭  ${img.key} — already exists, skipping`);
      continue;
    }

    process.stdout.write(`📸  ${img.key} — generating`);
    try {
      const imageUrl = await generateImage(img.prompt, img.size);
      process.stdout.write(" downloading");

      await downloadFile(imageUrl, dest);
      console.log(` ✅  saved → ${img.filename}`);
    } catch (err: any) {
      console.log(` ❌  ${err.message}`);
    }

    // Cooldown between requests to avoid rate limits
    await sleep(8000);
  }

  console.log("\n✨  Done!\n");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
