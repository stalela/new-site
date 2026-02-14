import type { SupabaseClient } from "@supabase/supabase-js";

export interface CompanyResearch {
  id: string;
  company_id: string;
  report: string;
  model: string;
  created_at: string;
}

export function createResearchApi(supabase: SupabaseClient) {
  return {
    /** Get the latest cached research report (within maxAgeDays). */
    async getLatest(
      companyId: string,
      maxAgeDays = 7
    ): Promise<CompanyResearch | null> {
      const { data, error } = await supabase.rpc("get_latest_research", {
        p_company_id: companyId,
        p_max_age_days: maxAgeDays,
      });
      if (error) throw error;
      const rows = data as CompanyResearch[] | null;
      return rows && rows.length > 0 ? rows[0] : null;
    },

    /** Save a new research report. */
    async save(companyId: string, report: string, model = "qwen3-max") {
      const { data, error } = await supabase
        .from("company_research")
        .insert({ company_id: companyId, report, model })
        .select()
        .single();
      if (error) throw error;
      return data as CompanyResearch;
    },

    /** List all past research reports for a company (newest first). */
    async list(companyId: string, limit = 10) {
      const { data, error } = await supabase
        .from("company_research")
        .select("*")
        .eq("company_id", companyId)
        .order("created_at", { ascending: false })
        .limit(limit);
      if (error) throw error;
      return data as CompanyResearch[];
    },
  };
}
