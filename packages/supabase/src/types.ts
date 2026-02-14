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

/* ── Company directory ──────────────────────────────────────── */

export type CompanySource = "yep" | "bizcommunity" | "bestdirectory";

export interface Company {
  id: string;
  source: CompanySource;
  source_id: string;
  name: string;
  description: string | null;
  category: string | null;
  categories: string[];
  type: string | null;
  phone: string | null;
  alt_phone: string | null;
  mobile: string | null;
  whatsapp: string | null;
  email: string | null;
  contact_email: string | null;
  contact_name: string | null;
  address: string | null;
  address_line1: string | null;
  suburb: string | null;
  city: string | null;
  province: string | null;
  postal_code: string | null;
  country: string | null;
  latitude: number | null;
  longitude: number | null;
  website: string | null;
  logo: string | null;
  source_url: string | null;
  registration_number: string | null;
  vat_number: string | null;
  seller_id: string | null;
  is_open: boolean | null;
  service_range_km: number | null;
  premium_seller: boolean | null;
  subscription_status: number | null;
  operation_hours: Record<string, unknown>[] | null;
  short_description: string | null;
  created_at: string;
  updated_at: string;
}

export interface CompanyInsert {
  source: CompanySource;
  source_id: string;
  name: string;
  description?: string | null;
  category?: string | null;
  categories?: string[];
  type?: string | null;
  phone?: string | null;
  alt_phone?: string | null;
  mobile?: string | null;
  whatsapp?: string | null;
  email?: string | null;
  contact_email?: string | null;
  contact_name?: string | null;
  address?: string | null;
  address_line1?: string | null;
  suburb?: string | null;
  city?: string | null;
  province?: string | null;
  postal_code?: string | null;
  country?: string | null;
  latitude?: number | null;
  longitude?: number | null;
  website?: string | null;
  logo?: string | null;
  source_url?: string | null;
  registration_number?: string | null;
  vat_number?: string | null;
  seller_id?: string | null;
  is_open?: boolean | null;
  service_range_km?: number | null;
  premium_seller?: boolean | null;
  subscription_status?: number | null;
  operation_hours?: Record<string, unknown>[] | null;
  short_description?: string | null;
}

export interface CompanyUpdate {
  name?: string;
  description?: string | null;
  category?: string | null;
  categories?: string[];
  type?: string | null;
  phone?: string | null;
  alt_phone?: string | null;
  mobile?: string | null;
  whatsapp?: string | null;
  email?: string | null;
  contact_email?: string | null;
  contact_name?: string | null;
  address?: string | null;
  address_line1?: string | null;
  suburb?: string | null;
  city?: string | null;
  province?: string | null;
  postal_code?: string | null;
  country?: string | null;
  latitude?: number | null;
  longitude?: number | null;
  website?: string | null;
  logo?: string | null;
  source_url?: string | null;
  registration_number?: string | null;
  vat_number?: string | null;
  seller_id?: string | null;
  is_open?: boolean | null;
  service_range_km?: number | null;
  premium_seller?: boolean | null;
  subscription_status?: number | null;
  operation_hours?: Record<string, unknown>[] | null;
  short_description?: string | null;
}
