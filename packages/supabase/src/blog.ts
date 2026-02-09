import type { SupabaseClient } from "@supabase/supabase-js";
import type { BlogPost, BlogPostInsert, BlogPostUpdate } from "./types";

export function createBlogApi(supabase: SupabaseClient) {
  return {
    /** List all posts. If `publishedOnly`, filters to published. */
    async list(publishedOnly = false) {
      const base = supabase
        .from("blog_posts")
        .select("*")
        .order("created_at", { ascending: false });

      const { data, error } = publishedOnly
        ? await base.eq("published", true)
        : await base;
      if (error) throw error;
      return data as BlogPost[];
    },

    /** Get a single post by slug. */
    async getBySlug(slug: string, publishedOnly = false) {
      const base = supabase
        .from("blog_posts")
        .select("*")
        .eq("slug", slug);

      const final = publishedOnly
        ? base.eq("published", true).single()
        : base.single();
      const { data, error } = await final;
      if (error) throw error;
      return data as BlogPost;
    },

    /** Create a new blog post. */
    async create(post: BlogPostInsert) {
      const now = new Date().toISOString();
      const { data, error } = await supabase
        .from("blog_posts")
        .insert({
          ...post,
          author: post.author || "Stalela",
          published: post.published ?? false,
          published_at: post.published ? now : null,
          created_at: now,
          updated_at: now,
        })
        .select()
        .single();
      if (error) throw error;
      return data as BlogPost;
    },

    /** Update an existing post by slug. */
    async update(slug: string, updates: BlogPostUpdate) {
      const payload: Record<string, unknown> = {
        ...updates,
        updated_at: new Date().toISOString(),
      };

      // Auto-set published_at when publishing for the first time
      if (updates.published === true) {
        const existing = await supabase
          .from("blog_posts")
          .select("published_at")
          .eq("slug", slug)
          .single();
        if (!existing.data?.published_at) {
          payload.published_at = new Date().toISOString();
        }
      }

      const { data, error } = await supabase
        .from("blog_posts")
        .update(payload)
        .eq("slug", slug)
        .select()
        .single();
      if (error) throw error;
      return data as BlogPost;
    },

    /** Delete a post by slug. */
    async delete(slug: string) {
      const { error } = await supabase
        .from("blog_posts")
        .delete()
        .eq("slug", slug);
      if (error) throw error;
    },

    /** Toggle published status. */
    async togglePublish(slug: string) {
      const post = await supabase
        .from("blog_posts")
        .select("published, published_at")
        .eq("slug", slug)
        .single();
      if (post.error) throw post.error;

      const nowPublished = !post.data.published;
      const payload: Record<string, unknown> = {
        published: nowPublished,
        updated_at: new Date().toISOString(),
      };
      if (nowPublished && !post.data.published_at) {
        payload.published_at = new Date().toISOString();
      }

      const { data, error } = await supabase
        .from("blog_posts")
        .update(payload)
        .eq("slug", slug)
        .select()
        .single();
      if (error) throw error;
      return data as BlogPost;
    },

    /** Count posts by status. */
    async stats() {
      const { count: total } = await supabase
        .from("blog_posts")
        .select("*", { count: "exact", head: true });
      const { count: published } = await supabase
        .from("blog_posts")
        .select("*", { count: "exact", head: true })
        .eq("published", true);
      return {
        total: total ?? 0,
        published: published ?? 0,
        drafts: (total ?? 0) - (published ?? 0),
      };
    },
  };
}
