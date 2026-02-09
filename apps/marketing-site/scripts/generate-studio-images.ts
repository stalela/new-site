/**
 * generate-studio-images.ts
 * ─────────────────────────
 * Generates images for the new Tech & Digital Services studio pages.
 */

import * as fs from "fs";
import * as path from "path";
import * as https from "https";

try {
  require("dotenv").config({ path: path.resolve(__dirname, "../.env.local") });
} catch {}

const API_KEY = process.env.DASHSCOPE_API_KEY;
if (!API_KEY) {
  console.error("❌  DASHSCOPE_API_KEY not set.");
  process.exit(1);
}

const BASE_URL = "https://dashscope-intl.aliyuncs.com";
const MODEL = "qwen-image-max";
const NEGATIVE_PROMPT =
  "low resolution, low quality, deformed limbs, deformed fingers, oversaturated, waxy, no facial details, overly smooth, AI-like, chaotic composition, blurry text, distorted text.";
const OUTPUT_DIR = path.resolve(__dirname, "../public/images/generated/studio");

const BRAND =
  "Modern, clean, professional illustration in a warm copper and charcoal color palette (#A4785A, #1C1C1C) with soft gradients. South African business context.";

interface ImageDef {
  key: string;
  filename: string;
  prompt: string;
  size: string;
}

const images: ImageDef[] = [
  // ── Branding & Design ──
  {
    key: "branding-hero",
    filename: "branding-hero.png",
    prompt: `${BRAND} A brand identity mood board on a designer's desk — logo sketches, colour swatches, typography samples, and a brand guidelines booklet. Warm studio lighting, creative workspace.`,
    size: "1664*928",
  },
  {
    key: "branding-process",
    filename: "branding-process.png",
    prompt: `${BRAND} Brand design process: from rough pencil sketches to polished digital logo on a screen. Split composition showing creative evolution. Designer tools, colour wheels.`,
    size: "1664*928",
  },
  {
    key: "branding-deliverables",
    filename: "branding-deliverables.png",
    prompt: `${BRAND} Professional brand deliverables flat lay — business cards, letterhead, branded envelope, social media mockups, and a brand book. All on a marble surface.`,
    size: "1664*928",
  },

  // ── Logo Design ──
  {
    key: "logo-hero",
    filename: "logo-hero.png",
    prompt: `${BRAND} Close-up of a designer sketching logo concepts on paper with pencils and markers. Multiple logo variations visible. Creative studio environment.`,
    size: "1664*928",
  },
  {
    key: "logo-process",
    filename: "logo-process.png",
    prompt: `${BRAND} Logo design process in four stages: pencil sketch, refined drawing, vectorised on screen, final logo on merchandise mockup. Horizontal timeline layout.`,
    size: "1664*928",
  },
  {
    key: "logo-showcase",
    filename: "logo-showcase.png",
    prompt: `${BRAND} Various logos displayed on different applications — storefront sign, business card, website header, mobile app icon, branded packaging. Mockup collage.`,
    size: "1664*928",
  },

  // ── Business Cards ──
  {
    key: "cards-hero",
    filename: "cards-hero.png",
    prompt: `${BRAND} Elegant business cards scattered on a dark surface with gold foil accents, spot UV finish visible. Premium printing quality, multiple designs.`,
    size: "1664*928",
  },
  {
    key: "cards-styles",
    filename: "cards-styles.png",
    prompt: `${BRAND} Collection of different business card styles — minimalist, bold, classic, modern. Various finishes: matte, glossy, embossed, letterpress. Grid layout.`,
    size: "1664*928",
  },

  // ── Graphic Design ──
  {
    key: "graphic-hero",
    filename: "graphic-hero.png",
    prompt: `${BRAND} Designer workspace with large monitor showing social media graphics, surrounded by printed flyers, brochures, and a pull-up banner mockup. Creative studio.`,
    size: "1664*928",
  },
  {
    key: "graphic-portfolio",
    filename: "graphic-portfolio.png",
    prompt: `${BRAND} Collage of graphic design work — social media posts, flyers, brochures, exhibition banners, email templates, infographics. Clean grid layout on white background.`,
    size: "1664*928",
  },

  // ── SEO ──
  {
    key: "seo-hero",
    filename: "seo-hero.png",
    prompt: `${BRAND} SEO concept illustration — laptop showing Google search results with a website climbing to position #1. Growth charts, keyword bubbles, magnifying glass. Data-driven, modern.`,
    size: "1664*928",
  },
  {
    key: "seo-dashboard",
    filename: "seo-dashboard.png",
    prompt: `${BRAND} Analytics dashboard showing SEO metrics — organic traffic growth chart, keyword rankings table, page speed scores, backlink profile. Clean data visualisation.`,
    size: "1664*928",
  },

  // ── Digital Marketing ──
  {
    key: "marketing-hero",
    filename: "marketing-hero.png",
    prompt: `${BRAND} Digital marketing concept — social media icons, ad campaigns on screens, email marketing, analytics graphs. Connected ecosystem illustration. Vibrant but professional.`,
    size: "1664*928",
  },
  {
    key: "marketing-channels",
    filename: "marketing-channels.png",
    prompt: `${BRAND} Marketing channels overview — Google Ads interface, Facebook ads manager, Instagram feed, email newsletter, YouTube. Multiple screens showing campaign performance.`,
    size: "1664*928",
  },

  // ── Custom Software ──
  {
    key: "software-hero",
    filename: "software-hero.png",
    prompt: `${BRAND} Developer workspace with multiple monitors showing code, database diagrams, and a modern web application UI. Clean, professional tech environment.`,
    size: "1664*928",
  },
  {
    key: "software-architecture",
    filename: "software-architecture.png",
    prompt: `${BRAND} Software architecture diagram — cloud services, APIs, databases, and frontend interfaces connected by clean lines. Technical but visually appealing isometric illustration.`,
    size: "1664*928",
  },

  // ── Payment Integration ──
  {
    key: "payment-hero",
    filename: "payment-hero.png",
    prompt: `${BRAND} Online payment concept — smartphone and laptop showing checkout flow, credit card, secure payment icons, South African payment gateways. Modern e-commerce feel.`,
    size: "1664*928",
  },
  {
    key: "payment-gateways",
    filename: "payment-gateways.png",
    prompt: `${BRAND} Payment gateway integration dashboard — transaction list, payment methods (card, EFT, QR code), revenue charts. Clean, secure, professional interface mockup.`,
    size: "1664*928",
  },

  // ── Mobile Apps ──
  {
    key: "apps-hero",
    filename: "apps-hero.png",
    prompt: `${BRAND} Multiple smartphones floating in space showing different mobile app screens — dashboard, chat, booking, shopping. Modern UI design, clean app interfaces.`,
    size: "1664*928",
  },
  {
    key: "apps-process",
    filename: "apps-process.png",
    prompt: `${BRAND} Mobile app development process: wireframe sketch on paper, UI design on tablet, code on laptop, final app in hand on smartphone. Four-stage progression.`,
    size: "1664*928",
  },

  // ── AI Agents ──
  {
    key: "ai-hero",
    filename: "ai-hero.png",
    prompt: `${BRAND} AI agent concept — abstract neural network visualization connected to business tools (email, documents, CRM). Futuristic but professional. Data flows and automation arrows.`,
    size: "1664*928",
  },
  {
    key: "ai-workflow",
    filename: "ai-workflow.png",
    prompt: `${BRAND} AI automation workflow — documents being processed, data extracted, tasks completed automatically. Flow diagram with AI brain at center connected to business processes.`,
    size: "1664*928",
  },

  // ── Chatbots ──
  {
    key: "chatbot-hero",
    filename: "chatbot-hero.png",
    prompt: `${BRAND} AI chatbot on a website — chat window open on a modern laptop, conversation bubbles, friendly bot avatar. Also shown on mobile phone with WhatsApp integration.`,
    size: "1664*928",
  },
  {
    key: "chatbot-features",
    filename: "chatbot-features.png",
    prompt: `${BRAND} Chatbot capabilities illustration — customer support conversation, appointment booking calendar, lead form being filled, FAQ answers, handoff to human agent. Multi-panel layout.`,
    size: "1664*928",
  },
];

// ── API + helpers (same as main script) ──

function request(method: string, urlPath: string, body?: object): Promise<any> {
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
      },
    };
    const req = https.request(options, (res) => {
      let chunks: Buffer[] = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        const raw = Buffer.concat(chunks).toString();
        try { resolve(JSON.parse(raw)); }
        catch { reject(new Error(`Non-JSON (HTTP ${res.statusCode}): ${raw.slice(0, 500)}`)); }
      });
    });
    req.on("error", reject);
    if (data) req.write(data);
    req.end();
  });
}

function sleep(ms: number) { return new Promise((r) => setTimeout(r, ms)); }

const MAX_RETRIES = 5;
const BACKOFF_MS = 15_000;

async function generateImage(prompt: string, size: string): Promise<string> {
  const body = {
    model: MODEL,
    input: { messages: [{ role: "user", content: [{ text: prompt }] }] },
    parameters: { size, negative_prompt: NEGATIVE_PROMPT, prompt_extend: true, watermark: false },
  };

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const res = await request("POST", "/api/v1/services/aigc/multimodal-generation/generation", body);
    if (res.code === "Throttling.RateQuota") {
      if (attempt === MAX_RETRIES) throw new Error(`Rate limited after ${MAX_RETRIES} retries`);
      const waitMs = BACKOFF_MS * Math.pow(2, attempt);
      process.stdout.write(` ⏳ rate-limited, waiting ${Math.round(waitMs / 1000)}s`);
      await sleep(waitMs);
      continue;
    }
    if (res.output?.choices) {
      const content = res.output.choices[0]?.message?.content;
      if (Array.isArray(content)) {
        const imgItem = content.find((c: any) => c.image);
        if (imgItem?.image) return imgItem.image;
      }
    }
    throw new Error(`Unexpected: ${JSON.stringify(res).slice(0, 500)}`);
  }
  throw new Error("Max retries exceeded");
}

async function downloadFile(url: string, dest: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    https.get(url, (res) => {
      if (res.statusCode === 301 || res.statusCode === 302) {
        https.get(res.headers.location!, (res2) => { res2.pipe(file); file.on("finish", () => { file.close(); resolve(); }); });
        return;
      }
      res.pipe(file);
      file.on("finish", () => { file.close(); resolve(); });
    }).on("error", (err) => { fs.unlinkSync(dest); reject(err); });
  });
}

async function main() {
  const onlyArg = process.argv.find((a) => a.startsWith("--only"));
  const onlyKey = onlyArg ? process.argv[process.argv.indexOf(onlyArg) + 1] : undefined;

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  let targets = onlyKey ? images.filter((img) => img.key === onlyKey) : images;
  if (onlyKey && targets.length === 0) {
    console.error(`❌  No image with key "${onlyKey}"`);
    console.log("Keys:", images.map((i) => i.key).join(", "));
    process.exit(1);
  }

  console.log(`\n🎨  Generating ${targets.length} studio image(s)...\n`);

  for (const img of targets) {
    const dest = path.join(OUTPUT_DIR, img.filename);
    if (fs.existsSync(dest) && !onlyKey) {
      console.log(`⏭  ${img.key} — exists, skipping`);
      continue;
    }
    process.stdout.write(`📸  ${img.key} — generating`);
    try {
      const imageUrl = await generateImage(img.prompt, img.size);
      process.stdout.write(" downloading");
      await downloadFile(imageUrl, dest);
      console.log(` ✅  ${img.filename}`);
    } catch (err: any) {
      console.log(` ❌  ${err.message}`);
    }
    await sleep(8000);
  }

  console.log("\n✨  Done!\n");
}

main().catch((err) => { console.error("Fatal:", err); process.exit(1); });
