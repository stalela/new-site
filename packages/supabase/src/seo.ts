import type { SupabaseClient } from "@supabase/supabase-js";
import type { SeoOverride, SeoOverrideUpsert } from "./types";

export function createSeoApi(supabase: SupabaseClient) {
  return {
    /** List all SEO overrides. */
    async list() {
      const { data, error } = await supabase
        .from("seo_overrides")
        .select("*")
        .order("page_path");
      if (error) throw error;
      return data as SeoOverride[];
    },

    /** Get SEO override for a specific page path. */
    async getByPath(pagePath: string) {
      const { data, error } = await supabase
        .from("seo_overrides")
        .select("*")
        .eq("page_path", pagePath)
        .maybeSingle();
      if (error) throw error;
      return data as SeoOverride | null;
    },

    /** Get a single override by ID. */
    async getById(id: string) {
      const { data, error } = await supabase
        .from("seo_overrides")
        .select("*")
        .eq("id", id)
        .single();
      if (error) throw error;
      return data as SeoOverride;
    },

    /** Create or update an SEO override. */
    async upsert(override: SeoOverrideUpsert) {
      const { data, error } = await supabase
        .from("seo_overrides")
        .upsert(
          {
            ...override,
            keywords: override.keywords ?? [],
            updated_at: new Date().toISOString(),
          },
          { onConflict: "page_path" }
        )
        .select()
        .single();
      if (error) throw error;
      return data as SeoOverride;
    },

    /** Delete an override. */
    async delete(id: string) {
      const { error } = await supabase
        .from("seo_overrides")
        .delete()
        .eq("id", id);
      if (error) throw error;
    },
  };
}
