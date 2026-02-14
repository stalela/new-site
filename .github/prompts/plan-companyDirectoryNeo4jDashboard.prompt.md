# Plan: Company Directory — Supabase + Neo4j + Admin Dashboard

**TL;DR:** Import the 174K merged company records into a Supabase `companies` table for fast search/filter/paginate, and into Neo4j for graph relationship queries (nearby, competitors, clusters). Build five admin dashboard views — stats overview, paginated list, detail page, map view, and graph explorer — following the existing admin patterns exactly (factory API, lazy proxy, server components, DataTable, StatCard).

---

## Steps

### Phase 1 — Data Layer (Supabase)

1. **Define `Company` types** in `packages/supabase/src/types.ts`: Add `Company` (full row), `CompanyInsert`, `CompanyUpdate` interfaces. Map the 39 unified columns from `output/merge_stats.json` — `id` (UUID, PK), `source` (enum: yep/bizcommunity/bestdirectory), `source_id`, `name`, `description`, `category`, `categories` (text[]), `type`, `phone`, `alt_phone`, `mobile`, `whatsapp`, `email`, `contact_email`, `contact_name`, `address`, `address_line1`, `suburb`, `city`, `province`, `postal_code`, `country`, `latitude` (float), `longitude` (float), `website`, `logo`, `source_url`, `registration_number`, `vat_number`, `seller_id`, `is_open` (bool), `service_range_km` (int), `premium_seller` (bool), `subscription_status` (int), `operation_hours` (jsonb), `short_description`, `created_at`, `updated_at`.

2. **Create Supabase migration** — SQL file to create the `companies` table with appropriate indexes:
   - `CREATE INDEX idx_companies_source ON companies(source)`
   - `CREATE INDEX idx_companies_province ON companies(province)`
   - `CREATE INDEX idx_companies_city ON companies(city)`
   - `CREATE INDEX idx_companies_name_trgm ON companies USING gin(name gin_trgm_ops)` — trigram index for fuzzy search
   - `CREATE INDEX idx_companies_geo ON companies(latitude, longitude)` — for bounding-box map queries
   - Composite index on `(source, province)` for filtered list queries

3. **Create import script** `scripts/import_companies_to_supabase.py` — reads `output/merged_companies.json`, chunks into batches of 500, upserts via Supabase Python client (`supabase-py`). Uses `source + source_id` as the natural unique key. Reports progress every 1000 records.

4. **Create `createCompaniesApi` factory** in new file `packages/supabase/src/companies.ts`, following the pattern from `packages/supabase/src/leads.ts`:
   - `list(opts)` — paginated with `{ count: "exact" }`, filters: `source`, `province`, `city`, `search` (ilike on name+email+phone), `has_gps` (not null lat/lng). Returns `{ companies: Company[], total: number }`.
   - `getById(id)` — single record by UUID.
   - `sources()` — distinct source values.
   - `provinces()` — distinct province values.
   - `cities(province?)` — distinct city values, optionally filtered by province.
   - `stats()` — total count, count by source, count by province, coverage percentages (phone, email, website, GPS, description, category).
   - `nearby(lat, lng, radiusKm, limit)` — Haversine distance query for records within radius.
   - `boundingBox(minLat, maxLat, minLng, maxLng, limit)` — for map viewport queries with clustering support.

5. **Register the export** — add `"./companies": "./src/companies.ts"` to `packages/supabase/package.json` exports, and re-export from `packages/supabase/src/index.ts`.

6. **Add lazy proxy** — add `export const companiesApi = lazy(() => createCompaniesApi(getClient()));` in `apps/admin/src/lib/api.ts`.

### Phase 2 — Neo4j Graph Layer

7. **Create `packages/neo4j/`** (or simpler: `apps/admin/src/lib/neo4j.ts` since only admin uses it) with a `createNeo4jClient()` function using the `neo4j-driver` npm package. Env vars: `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`.

8. **Create Neo4j import script** `scripts/import_companies_to_neo4j.py`:
   - Node types: `Company` (id, name, lat, lng, category), `Province`, `City`, `Suburb`, `Category`
   - Relationships: `IN_PROVINCE`, `IN_CITY`, `IN_SUBURB`, `IN_CATEGORY`, `NEAR` (within 500m, computed from GPS), `SAME_STREET` (matching address_line1), `COMPETES_WITH` (same category + within 2km)
   - Batch load with `UNWIND` Cypher for performance
   - Create spatial indexes for proximity queries

9. **Create `createNeo4jApi()` factory** in `apps/admin/src/lib/neo4j-api.ts`:
   - `getRelated(companyId)` — neighbors, competitors, same-category businesses
   - `getGraph(companyId, depth)` — subgraph for visual explorer
   - `getClusters(province?, city?)` — aggregated cluster data
   - `getCompetitors(companyId, radiusKm)` — nearby same-category businesses
   - Returns data shaped for the graph visualization library

### Phase 3 — Admin Dashboard Views

10. **Add navigation** — in `apps/admin/src/components/Sidebar.tsx`, add a `Companies` `NavGroup` to the `navigation` array (after Contacts):
    - "All Companies" → `/companies` (icon: `Building2`)
    - "Map" → `/companies/map` (icon: `MapPin`)
    - "Graph" → `/companies/graph` (icon: `Network`)

11. **Add title mappings** in `apps/admin/src/components/Header.tsx`: `/companies` → "Companies", `/companies/map` → "Company Map", `/companies/graph` → "Graph Explorer".

12. **Stats overview page** at `apps/admin/src/app/(dashboard)/companies/page.tsx`:
    - Server component, `force-dynamic`
    - Top: 4-column `StatCard` grid — Total Companies, With GPS, With Email, With Website
    - Middle: 2-column grid — Source breakdown bar chart (recharts `BarChart`), Province distribution pie chart (recharts `PieChart`)
    - Bottom: Recent companies `DataTable` (top 20 by created_at)
    - Data fetched via `companiesApi.stats()` + `companiesApi.list({ limit: 20 })`

13. **Company list page** at `apps/admin/src/app/(dashboard)/companies/list/page.tsx`:
    - Following the leads page pattern exactly
    - Server-side pagination — 50 per page (not 20, given 174K records)
    - `CompanyFilters.tsx` client component: search input (name/email/phone), source dropdown, province dropdown, city dropdown (dependent on province)
    - `DataTable` with columns: Name, Phone, Email, City, Province, Source, with `render` functions for links/badges
    - **Windowed paginator** — show first/last + 5 pages around current (not all 3,400+ page buttons). New `Pagination.tsx` component.

14. **Company detail page** at `apps/admin/src/app/(dashboard)/companies/[id]/page.tsx`:
    - Following the lead detail page pattern
    - Top: Back link, company name + badge for source
    - Left card: Contact details (phone, alt_phone, mobile, whatsapp, email, website)
    - Right card: Location (address, suburb, city, province, postal_code, lat/lng)
    - Bottom-left: Business details (description, categories, registration_number, vat_number, operation_hours)
    - Bottom-right: Mini map (Leaflet) centered on the company's GPS coords
    - Below: "Nearby Companies" section — `companiesApi.nearby(lat, lng, 2, 10)` → DataTable of 10 nearest

15. **Map view** at `apps/admin/src/app/(dashboard)/companies/map/page.tsx`:
    - Install `leaflet`, `react-leaflet`, `@types/leaflet` as dependencies in admin
    - `"use client"` `CompanyMap.tsx` component with `react-leaflet` `MapContainer` + `TileLayer` (OpenStreetMap)
    - **Marker clustering** via `react-leaflet-cluster` (Leaflet.markercluster) — essential for 161K markers
    - Server component fetches initial viewport data via `companiesApi.boundingBox()` (limit 5000 for initial load)
    - Client-side `onMoveEnd` callback fetches new viewport bounds, loads markers via a lightweight API route `POST /api/companies/map` that calls `companiesApi.boundingBox()`
    - Popup on marker click: name, phone, category, link to detail page
    - Filter panel (sidebar or top bar): source, province dropdown
    - Initial center: South Africa center `[-30.5595, 22.9375]`, zoom 5

16. **Graph explorer** at `apps/admin/src/app/(dashboard)/companies/graph/page.tsx`:
    - Install `react-force-graph-2d` (lightweight, 2D force-directed graph)
    - `"use client"` `GraphExplorer.tsx`:
      - Search bar to find a company by name → select → load its Neo4j subgraph
      - `ForceGraph2D` component renders nodes (companies as circles, sized by connection count) and edges (colored by relationship type)
      - Node colors: copper for central company, by category for others
      - Edge legend: NEAR (gray), COMPETES_WITH (red), SAME_STREET (blue), IN_CATEGORY (green)
      - Click a node → navigate to detail page or re-center graph
      - Depth slider (1-3 hops)
    - API route `POST /api/companies/graph` calls `neo4jApi.getGraph(companyId, depth)` → returns `{ nodes[], edges[] }`

17. **API routes for client-side data fetching**:
    - `apps/admin/src/app/api/companies/map/route.ts` — POST, accepts `{ minLat, maxLat, minLng, maxLng, limit, source? }`, returns marker array
    - `apps/admin/src/app/api/companies/graph/route.ts` — POST, accepts `{ companyId, depth }`, returns graph data from Neo4j
    - `apps/admin/src/app/api/companies/search/route.ts` — GET, accepts `?q=...&limit=10`, returns name matches for autocomplete in graph explorer

### Phase 4 — New Dependencies

18. **Admin `package.json`** — add:
    - `neo4j-driver` — Neo4j JavaScript driver
    - `leaflet` + `react-leaflet` + `@types/leaflet` — map rendering
    - `react-leaflet-cluster` — marker clustering
    - `react-force-graph-2d` — graph visualization

19. **Admin `next.config.ts`** — no changes needed (these are all client-side libraries)

20. **Environment variables** — add to admin `.env`:
    - `NEO4J_URI` (e.g., `bolt://localhost:7687` or Neo4j Aura cloud URI)
    - `NEO4J_USER` (default: `neo4j`)
    - `NEO4J_PASSWORD`

### Phase 5 — Infrastructure

21. **Neo4j deployment** — two options:
    - **Local dev**: `docker run -d -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password neo4j:5`
    - **Production**: Neo4j Aura Free tier (50K nodes, 175K relationships — may be tight for 174K companies; Aura Pro if needed) or self-host on a VPS

22. **Supabase migration** — run via Supabase dashboard or CLI: `supabase db push`

---

## Verification

- **Supabase import**: Run import script, verify `SELECT count(*) FROM companies` = 174,612
- **Neo4j import**: Run import script, verify node/relationship counts in Neo4j Browser at `:7474`
- **Companies list**: Navigate to `/companies/list`, verify pagination (50/page = 3,493 pages), search by name, filter by province
- **Detail page**: Click any company, verify all fields render, mini-map shows correct location, nearby companies table populated
- **Map view**: Load `/companies/map`, verify marker clusters appear, zoom in to see individual pins, click popup links to detail
- **Graph explorer**: Search for a company, verify force graph renders with correct relationship types and colors
- **Build test**: `npm run build` from monorepo root — all lazy proxies should prevent Neo4j/Supabase initialization errors during build

---

## Decisions

- **Supabase + Neo4j dual storage**: Supabase handles all tabular CRUD/pagination/search (where Postgres excels). Neo4j handles graph traversal only (competitors, clusters, nearby chains). Avoids forcing Neo4j into tabular workloads it's not optimized for.
- **Leaflet over Mapbox**: Free, no API key, sufficient for internal admin use. Can upgrade later.
- **react-force-graph-2d over 3D**: Lighter, faster, better for a dashboard panel. 3D is overkill for relationship exploration.
- **Server-side pagination**: 174K records cannot be loaded client-side. All list/map/search queries are server-paginated with `{ count: "exact" }`.
- **Windowed paginator**: New `Pagination.tsx` component showing first/last + window of 5 around current page, replacing the "render all page numbers" pattern from the leads page.
- **Map marker limit**: Viewport queries capped at 5,000 markers with clustering — browser can't handle 161K DOM elements. Client refetches on pan/zoom.
- **Neo4j Aura free tier risk**: 50K node limit may not fit 174K companies. If so, either self-host or use Aura Pro. The Supabase layer works independently regardless.
