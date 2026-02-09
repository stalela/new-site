/* ── Database types ──────────────────────────────────────────────── */

export interface BlogPost {
  id: string;
  slug: string;
  title: string;
  excerpt: string | null;
  content: string;
  cover_image: string | null;
  author: string;
  published: boolean;
  published_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface BlogPostInsert {
  slug: string;
  title: string;
  excerpt?: string | null;
  content: string;
  cover_image?: string | null;
  author?: string;
  published?: boolean;
}

export interface BlogPostUpdate {
  title?: string;
  excerpt?: string | null;
  content?: string;
  cover_image?: string | null;
  author?: string;
  published?: boolean;
}

export interface Lead {
  id: string;
  email: string;
  source: string;
  name: string | null;
  phone: string | null;
  data: Record<string, unknown> | null;
  created_at: string;
}

export interface LeadInsert {
  email: string;
  source: string;
  name?: string | null;
  phone?: string | null;
  data?: Record<string, unknown> | null;
}

export interface Customer {
  id: string;
  lead_id: string | null;
  name: string;
  email: string;
  phone: string | null;
  company: string | null;
  status: "active" | "inactive" | "prospect";
  notes: string | null;
  created_at: string;
  updated_at: string;
}

export interface CustomerInsert {
  lead_id?: string | null;
  name: string;
  email: string;
  phone?: string | null;
  company?: string | null;
  status?: "active" | "inactive" | "prospect";
  notes?: string | null;
}

export interface CustomerUpdate {
  name?: string;
  email?: string;
  phone?: string | null;
  company?: string | null;
  status?: "active" | "inactive" | "prospect";
  notes?: string | null;
}

export interface SeoOverride {
  id: string;
  page_path: string;
  title_override: string | null;
  meta_description: string | null;
  keywords: string[];
  og_image_url: string | null;
  updated_at: string;
}

export interface SeoOverrideUpsert {
  page_path: string;
  title_override?: string | null;
  meta_description?: string | null;
  keywords?: string[];
  og_image_url?: string | null;
}
