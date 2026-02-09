import type { SupabaseClient } from "@supabase/supabase-js";

export function createMetricsApi(supabase: SupabaseClient) {
  return {
    /** Summary counts for the dashboard. */
    async summary() {
      const [leads, leadsMonth, blogStats] = await Promise.all([
        supabase.from("leads").select("*", { count: "exact", head: true }),
        supabase
          .from("leads")
          .select("*", { count: "exact", head: true })
          .gte(
            "created_at",
            new Date(
              new Date().getFullYear(),
              new Date().getMonth(),
              1
            ).toISOString()
          ),
        supabase.from("blog_posts").select("published", { count: "exact" }),
      ]);

      const published =
        blogStats.data?.filter((p) => p.published).length ?? 0;
      const totalPosts = blogStats.data?.length ?? 0;

      return {
        totalLeads: leads.count ?? 0,
        leadsThisMonth: leadsMonth.count ?? 0,
        publishedPosts: published,
        draftPosts: totalPosts - published,
      };
    },

    /** Leads grouped by source. */
    async leadsBySource() {
      const { data, error } = await supabase
        .from("leads")
        .select("source");
      if (error) throw error;

      const counts: Record<string, number> = {};
      for (const row of data ?? []) {
        counts[row.source] = (counts[row.source] || 0) + 1;
      }
      return Object.entries(counts)
        .map(([source, count]) => ({ source, count }))
        .sort((a, b) => b.count - a.count);
    },

    /** Leads per day for the last N days (default 30). */
    async leadsOverTime(days = 30) {
      const since = new Date();
      since.setDate(since.getDate() - days);

      const { data, error } = await supabase
        .from("leads")
        .select("created_at")
        .gte("created_at", since.toISOString())
        .order("created_at");
      if (error) throw error;

      const byDay: Record<string, number> = {};
      for (const row of data ?? []) {
        const day = row.created_at.slice(0, 10); // YYYY-MM-DD
        byDay[day] = (byDay[day] || 0) + 1;
      }

      // Fill in missing days with 0
      const result: { date: string; count: number }[] = [];
      const cursor = new Date(since);
      const today = new Date();
      while (cursor <= today) {
        const key = cursor.toISOString().slice(0, 10);
        result.push({ date: key, count: byDay[key] || 0 });
        cursor.setDate(cursor.getDate() + 1);
      }
      return result;
    },

    /** Recent leads (last N). */
    async recentLeads(limit = 10) {
      const { data, error } = await supabase
        .from("leads")
        .select("*")
        .order("created_at", { ascending: false })
        .limit(limit);
      if (error) throw error;
      return data;
    },

    /** Recent blog activity (last N). */
    async recentBlogPosts(limit = 5) {
      const { data, error } = await supabase
        .from("blog_posts")
        .select("id, slug, title, published, updated_at")
        .order("updated_at", { ascending: false })
        .limit(limit);
      if (error) throw error;
      return data;
    },
  };
}
