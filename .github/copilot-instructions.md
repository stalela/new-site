# Stalela Copilot Instructions

## Scope and boundaries
- Monorepo (Turbo + npm workspaces): `apps/marketing-site` (public site), `apps/admin` (internal dashboard), `packages/supabase` (shared data layer).
- Treat `docs/` as future-design reference only; do not implement those workflows/APIs unless explicitly requested.
- `scripts/` and `output/` at repo root are data-scraping/import utilities, not app runtime code.

## Core architecture
- Primary flow: UI → Next.js route/server component → `@stalela/supabase` factory API → Supabase.
- Keep database logic in `packages/supabase/src/*` (`createBlogApi`, `createLeadsApi`, `createCustomersApi`, `createSeoApi`, `createMetricsApi`, `createCompaniesApi`, `createResearchApi`).
- In app routes, prefer orchestration + validation only; avoid direct table logic unless an existing route already follows that pattern.

## Supabase client split
- Marketing uses anon/public client only: `apps/marketing-site/src/lib/supabase.ts` → `createPublicClient()`.
- Admin uses service-role client via lazy proxies: `apps/admin/src/lib/api.ts` (`lazy()` prevents `next build` crashes when env vars are missing).
- `createAdminClient()` must remain server-only; never expose `SUPABASE_SERVICE_ROLE_KEY` to client components.

## Key integration paths
- Lead capture path: `apps/marketing-site/src/components/LeadForm.tsx` → `POST /api/lead` (`apps/marketing-site/src/app/api/lead/route.ts`) with required `source`.
- Admin auth gate: `apps/admin/src/middleware.ts` + `apps/admin/src/lib/supabase-middleware.ts` (redirect unauthenticated users to `/login`, redirect logged-in users away from `/login`).
- Admin company intelligence endpoints (`apps/admin/src/app/api/companies/*`) integrate Supabase, optional Neo4j (`NEO4J_*`), and DashScope (`DASHSCOPE_API_KEY`).

## Framework conventions
- Next.js App Router + React 19; `params` in pages/layouts are often `Promise<...>` and must be awaited.
- `reactCompiler: true` is enabled in both apps; marketing also sets `transpilePackages: ["@stalela/supabase"]`.
- Tailwind CSS v4 setup is CSS-first (`@import "tailwindcss"`, `@plugin`, `@theme inline`) in `apps/marketing-site/src/app/globals.css`.

## Marketing-site specifics
- i18n uses `next-intl` with locales `en`, `zu`, `af`, `xh` (`apps/marketing-site/src/i18n/routing.ts`).
- Use `Link` from `@/i18n/navigation` (not `next/link`) for locale-aware navigation.
- Root fonts/meta live in `src/app/layout.tsx`; locale wiring (`NextIntlClientProvider`, `setRequestLocale`) lives in `src/app/[locale]/layout.tsx`.
- Services structure is data-driven from `src/lib/services-data.ts`; service pages may render rich custom components via `customPages` in `src/app/[locale]/services/[categorySlug]/[serviceSlug]/page.tsx`.
- SEO helpers are centralized in `src/lib/seo.ts` (`buildPageMetadata`, `buildAlternates`, `SITE_URL`).

## Shared package conventions (`@stalela/supabase`)
- Use subpath imports (`/client`, `/blog`, `/leads`, `/customers`, `/seo`, `/metrics`, `/companies`, `/research`, `/types`).
- Types in `packages/supabase/src/types.ts` are hand-maintained interfaces (not generated from Supabase schema).

## Dev workflows and env
- Root workflows: `npm run dev`, `npm run build`, `npm run lint`, `npm run dev:marketing`, `npm run dev:admin`.
- Marketing image generation scripts: `cd apps/marketing-site && npm run generate-images` (or `generate-images:only`) require `DASHSCOPE_API_KEY`.
- Core env vars: `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY`, `SUPABASE_SERVICE_ROLE_KEY` (admin only), optional `NEO4J_URI/USER/PASSWORD`, `DASHSCOPE_API_KEY`.

## Change constraints
- Prefer targeted changes that match existing patterns over new abstractions.
- Do not add auth/dashboard/compliance-engine features to marketing pages unless requested.
- Keep marketing copy factual; do not invent legal/compliance claims.
- Supabase remote image host is `hwfhtdlbtjhmwzyvejxd.supabase.co` in both Next.js configs.