# Stalela Copilot Instructions

## Scope & layout
- Monorepo with Turbo: apps/marketing-site (public site) and apps/admin (internal admin), plus shared Supabase helpers in packages/supabase.
- docs/ contains future SaaS design; reference for context only, do not build the SaaS or expand APIs beyond lead capture.

## Architecture & data flow
- Marketing site is Next.js App Router with locale middleware (next-intl). Entry routes live in [apps/marketing-site/src/app/[locale]](apps/marketing-site/src/app/%5Blocale%5D).
- Lead capture: `LeadForm` submits to POST /api/lead → Supabase insert in [apps/marketing-site/src/app/api/lead/route.ts](apps/marketing-site/src/app/api/lead/route.ts) using the public client from [apps/marketing-site/src/lib/supabase.ts](apps/marketing-site/src/lib/supabase.ts).
- Admin app uses Supabase SSR session middleware in [apps/admin/src/middleware.ts](apps/admin/src/middleware.ts) and the service-role client from [packages/supabase/src/client.ts](packages/supabase/src/client.ts) for full CRUD.

## Shared Supabase package
- Use `@stalela/supabase` helpers (e.g., `createPublicClient`, `createAdminClient`, `createLeadsApi`) from [packages/supabase/src](packages/supabase/src).
- Required env vars: `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY`, `SUPABASE_SERVICE_ROLE_KEY` (server-only).

## UI & content conventions (marketing-site)
- Server components by default; add "use client" only for state/effects/handlers.
- Layout primitives live in [apps/marketing-site/src/components](apps/marketing-site/src/components): `Container`, `Section`, `Card`.
- Service content is data-driven in [apps/marketing-site/src/lib/services-data.ts](apps/marketing-site/src/lib/services-data.ts); routes: /services → /services/[categorySlug]/[serviceSlug]. Use `StudioPage` for rich service pages.
- Icons must be from `lucide-react` and stored in data objects (rendered dynamically).

## Styling & assets
- Tailwind v4 with @theme inline in [apps/marketing-site/src/app/globals.css](apps/marketing-site/src/app/globals.css); use copper palette classes (no arbitrary colors).
- Images use next/image with `fill` + `sizes`; AI images live in [apps/marketing-site/public/images/generated](apps/marketing-site/public/images/generated).

## Developer workflows
- Root: `npm run dev|build|lint` (Turbo). App-specific: `npm run dev` in each app; admin runs on port 3001.
- Marketing-site image generation scripts in [apps/marketing-site/scripts](apps/marketing-site/scripts).

## Hard constraints
- Do not add auth/dashboards/compliance engines to the marketing site.
- Keep copy factual and grounded; avoid new marketing claims.