# Stalela Copilot Instructions

## Project Overview
Stalela is a marketing & lead-capture site for a South African SME compliance platform. The only active codebase lives in `apps/marketing-site/` — a Next.js 16 (App Router) + TypeScript + Tailwind CSS v4 site targeting Vercel. The `docs/` folder contains the future SaaS platform design (domain model, workflows, integrations) — reference it for context but **do not build** dashboards, auth, compliance engines, or APIs beyond the existing lead-capture route.

## Developer Workflow
```bash
cd apps/marketing-site
npm run dev          # next dev on localhost:3000
npm run build        # production build
npm run lint         # eslint
npm run generate-images        # AI image generation (DashScope)
npm run generate-images:only   # generate specific images only
```

## Tech Stack & Key Config
- **Next.js 16** with React Compiler enabled (`reactCompiler: true` in next.config.ts)
- **Tailwind CSS v4** via `@tailwindcss/postcss` — styles in `src/app/globals.css` using `@theme inline` block
- **Path alias**: `@/*` → `./src/*` (configured in tsconfig.json)
- **Fonts**: Geist Sans + Geist Mono via `next/font/google`, exposed as CSS vars `--font-geist-sans`/`--font-geist-mono`
- **Icons**: `lucide-react` exclusively — icon components stored in data objects and rendered dynamically (e.g., `<service.icon className="h-6 w-6" />`)
- **Utility**: `cn()` helper in `src/lib/utils.ts` (clsx + tailwind-merge) for conditional class merging
- **Internationalization**: next-intl with locales `en`, `zu`, `af`, `xh`; middleware handles locale routing

## Brand Colour Palette
Custom `copper` palette defined as CSS custom properties in `globals.css` and registered with `@theme inline`:
- Accents: `copper-600` (#a4785a), `copper-700` (#8a6349)
- Backgrounds: `copper-50` (#faf5f0), `copper-100` (#f0e4d6)
- Text: `charcoal` (#1c1c1c)
Use `bg-copper-600`, `text-copper-700`, `border-copper-200`, etc. in Tailwind classes. Do NOT use arbitrary colour values.

## Component Patterns
- **Server components by default** — only add `"use client"` when component needs state, effects, or event handlers
- **Layout primitives**: `Container` (max-width wrapper), `Section` (py + Container), `Card` (bordered box) — compose pages from these
- **Props over hardcoded content**: Components receive typed data via explicit TypeScript interfaces; page-level const arrays define content
- **`StudioPage`**: Mega-template component for rich service pages — receives ~20 props covering hero, gallery, features, process steps, tech stack, pricing, FAQ, cross-links. Use it for any new "Tech & Digital Services" page
- **`LeadForm`**: Reusable form driven by a `FormField[]` descriptor array + `source` string identifier. Fields support types: `text`, `email`, `tel`, `number`, `select`, `textarea`
- **`Button`**: Renders as `<Link>` when `href` is provided, `<button>` otherwise. Variants: `primary`, `secondary`, `outline`, `ghost`

## Services Data Model
All service/category data lives in `src/lib/services-data.ts`:
- `allServices: ServiceItem[]` — flat list with `icon`, `slug`, `title`, `description`, `longDescription`, `features`
- `serviceCategories: ServiceCategory[]` — groups built via `buildCategory()` referencing service titles
- Lookup helpers: `getServiceBySlug()`, `getCategoryBySlug()`, `getCategoryForService()`
- Routes: `/services` → `/services/[categorySlug]` → `/services/[categorySlug]/[serviceSlug]`
- Custom rich pages registered in the `customPages` map in the `[serviceSlug]/page.tsx` route — use `StudioPage` or custom components for detailed service pages

## Image Handling
- All images via `next/image` with `fill` mode + `object-cover` inside aspect-ratio containers
- Always provide responsive `sizes` attribute
- AI-generated images stored in `public/images/generated/` — created via `scripts/generate-images.ts` (DashScope qwen-image-max with brand prefix in prompts)
- Hero images use `priority` prop; others use default lazy loading

## API Route
Single endpoint: `POST /api/lead` — validates `email` + `source`, inserts into Supabase `leads` table. Returns `{ success: true, id }`. Do NOT add auth or complex backend logic.

## Build & Deployment
- Static generation with `generateStaticParams()` for dynamic routes and `generateMetadata()` for SEO
- Deployed to Vercel; `vercel.json` sets build config
- Security headers in `next.config.ts` for all routes

## Hard Constraints
- Do NOT build authentication, dashboards, compliance engines, or banking integrations
- Do NOT add APIs beyond simple lead capture
- Do NOT invent marketing claims — use simple, factual copy
- Keep all work inside `apps/marketing-site/`
- Tone: clear, practical, trustworthy — no hype or buzzwords