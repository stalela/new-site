import type { SupabaseClient } from "@supabase/supabase-js";
import type { Company } from "./types";

export function createCompaniesApi(supabase: SupabaseClient) {
  return {
    /** List companies with pagination, filters, and search. */
    async list(opts?: {
      source?: string;
      province?: string;
      city?: string;
      search?: string;
      has_gps?: boolean;
      limit?: number;
      offset?: number;
    }) {
      let query = supabase
        .from("companies")
        .select("*", { count: "exact" });

      if (opts?.source) query = query.eq("source", opts.source);
      if (opts?.province) query = query.eq("province", opts.province);
      if (opts?.city) query = query.eq("city", opts.city);
      if (opts?.search) {
        query = query.or(
          `name.ilike.%${opts.search}%,email.ilike.%${opts.search}%,phone.ilike.%${opts.search}%`
        );
      }
      if (opts?.has_gps) {
        query = query.not("latitude", "is", null).not("longitude", "is", null);
      }

      const limit = opts?.limit ?? 50;
      const final = query.order("name", { ascending: true }).limit(limit);
      if (opts?.offset)
        final.range(opts.offset, opts.offset + limit - 1);

      const { data, error, count } = await final;
      if (error) throw error;
      return { companies: data as Company[], total: count ?? 0 };
    },

    /** Get a single company by ID. */
    async getById(id: string) {
      const { data, error } = await supabase
        .from("companies")
        .select("*")
        .eq("id", id)
        .single();
      if (error) throw error;
      return data as Company;
    },

    /** Get distinct source values. */
    async sources() {
      const { data, error } = await supabase.rpc("distinct_company_sources");
      if (error) throw error;
      return (data ?? []) as string[];
    },

    /** Get distinct province values. */
    async provinces() {
      const { data, error } = await supabase.rpc("distinct_company_provinces");
      if (error) throw error;
      return (data ?? []) as string[];
    },

    /** Get distinct cities, optionally filtered by province. */
    async cities(province?: string) {
      const { data, error } = await supabase.rpc("distinct_company_cities", {
        p_province: province ?? null,
      });
      if (error) throw error;
      return (data ?? []) as string[];
    },

    /** Dashboard summary statistics (single RPC call). */
    async stats() {
      const { data, error } = await supabase.rpc("company_stats");
      if (error) throw error;
      const result = data as {
        totalCompanies: number;
        withPhone: number;
        withEmail: number;
        withWebsite: number;
        withGps: number;
        bySource: { source: string; count: number }[];
        byProvince: { province: string; count: number }[];
      };
      return result;
    },

    /** Find companies near a GPS point (Haversine approximation via Postgres). */
    async nearby(lat: number, lng: number, radiusKm: number, limit = 10) {
      // Use the Haversine formula in raw SQL via RPC or a simple bounding box + JS sort
      const degPerKm = 1 / 111.32;
      const latDelta = radiusKm * degPerKm;
      const lngDelta = radiusKm * degPerKm / Math.cos((lat * Math.PI) / 180);

      const { data, error } = await supabase
        .from("companies")
        .select("*")
        .gte("latitude", lat - latDelta)
        .lte("latitude", lat + latDelta)
        .gte("longitude", lng - lngDelta)
        .lte("longitude", lng + lngDelta)
        .limit(limit * 3); // Over-fetch, then sort by actual distance

      if (error) throw error;

      // Calculate true Haversine distance and sort
      const withDist = (data as Company[]).map((c) => {
        const dLat = ((c.latitude! - lat) * Math.PI) / 180;
        const dLng = ((c.longitude! - lng) * Math.PI) / 180;
        const a =
          Math.sin(dLat / 2) ** 2 +
          Math.cos((lat * Math.PI) / 180) *
            Math.cos((c.latitude! * Math.PI) / 180) *
            Math.sin(dLng / 2) ** 2;
        const distKm = 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return { ...c, distance_km: Math.round(distKm * 100) / 100 };
      });

      return withDist
        .filter((c) => c.distance_km <= radiusKm)
        .sort((a, b) => a.distance_km - b.distance_km)
        .slice(0, limit);
    },

    /** Get companies within a map viewport bounding box. */
    async boundingBox(
      minLat: number,
      maxLat: number,
      minLng: number,
      maxLng: number,
      opts?: { limit?: number; source?: string }
    ) {
      let query = supabase
        .from("companies")
        .select("id, name, latitude, longitude, category, source, phone")
        .gte("latitude", minLat)
        .lte("latitude", maxLat)
        .gte("longitude", minLng)
        .lte("longitude", maxLng);

      if (opts?.source) query = query.eq("source", opts.source);
      query = query.limit(opts?.limit ?? 5000);

      const { data, error } = await query;
      if (error) throw error;
      return data as Pick<Company, "id" | "name" | "latitude" | "longitude" | "category" | "source" | "phone">[];
    },

    /** Search companies by name (for autocomplete). */
    async search(q: string, limit = 10) {
      const { data, error } = await supabase
        .from("companies")
        .select("id, name, city, province, source")
        .ilike("name", `%${q}%`)
        .limit(limit);
      if (error) throw error;
      return data as Pick<Company, "id" | "name" | "city" | "province" | "source">[];
    },
  };
}
