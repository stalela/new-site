import type { SupabaseClient } from "@supabase/supabase-js";
import type { Lead, LeadInsert } from "./types";

export function createLeadsApi(supabase: SupabaseClient) {
  return {
    /** List leads with optional filters. */
    async list(opts?: {
      source?: string;
      search?: string;
      from?: string;
      to?: string;
      limit?: number;
      offset?: number;
    }) {
      let query = supabase
        .from("leads")
        .select("*", { count: "exact" });

      if (opts?.source) query = query.eq("source", opts.source);
      if (opts?.search) {
        query = query.or(
          `email.ilike.%${opts.search}%,name.ilike.%${opts.search}%`
        );
      }
      if (opts?.from) query = query.gte("created_at", opts.from);
      if (opts?.to) query = query.lte("created_at", opts.to);

      const final = query.order("created_at", { ascending: false });
      if (opts?.limit) final.limit(opts.limit);
      if (opts?.offset) final.range(opts.offset, opts.offset + (opts.limit ?? 20) - 1);

      const { data, error, count } = await final;
      if (error) throw error;
      return { leads: data as Lead[], total: count ?? 0 };
    },

    /** Get a single lead by ID. */
    async getById(id: string) {
      const { data, error } = await supabase
        .from("leads")
        .select("*")
        .eq("id", id)
        .single();
      if (error) throw error;
      return data as Lead;
    },

    /** Create a lead (used by marketing site). */
    async create(lead: LeadInsert) {
      const { data, error } = await supabase
        .from("leads")
        .insert(lead)
        .select("id")
        .single();
      if (error) throw error;
      return data as { id: string };
    },

    /** Get distinct lead sources for filter dropdowns. */
    async sources() {
      const { data, error } = await supabase
        .from("leads")
        .select("source")
        .order("source");
      if (error) throw error;
      const unique = [...new Set((data ?? []).map((d) => d.source))];
      return unique as string[];
    },

    /** Delete a lead by ID. */
    async delete(id: string) {
      const { error } = await supabase
        .from("leads")
        .delete()
        .eq("id", id);
      if (error) throw error;
    },
  };
}
