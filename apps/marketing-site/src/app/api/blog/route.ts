import { NextResponse } from "next/server";
import { supabase } from "@/lib/supabase";

// GET /api/blog — list published posts (public, read-only)
export async function GET() {
  const { data, error } = await supabase
    .from("blog_posts")
    .select("id, slug, title, excerpt, cover_image_url, author, published, published_at, created_at, updated_at")
    .eq("published", true)
    .order("created_at", { ascending: false });

  if (error) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }

  return NextResponse.json(data);
}
