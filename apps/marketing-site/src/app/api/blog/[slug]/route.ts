import { NextRequest, NextResponse } from "next/server";
import { supabase } from "@/lib/supabase";

interface RouteParams {
  params: Promise<{ slug: string }>;
}

// GET /api/blog/[slug] — fetch a single published post (public, read-only)
export async function GET(_request: NextRequest, { params }: RouteParams) {
  const { slug } = await params;

  const { data, error } = await supabase
    .from("blog_posts")
    .select("*")
    .eq("slug", slug)
    .eq("published", true)
    .single();

  if (error || !data) {
    return NextResponse.json({ error: "Post not found." }, { status: 404 });
  }

  return NextResponse.json(data);
}
