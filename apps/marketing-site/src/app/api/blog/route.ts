import { NextRequest, NextResponse } from "next/server";
import { supabase } from "@/lib/supabase";

function isAuthorized(request: NextRequest): boolean {
  const auth = request.headers.get("authorization");
  return auth === process.env.ADMIN_PASSWORD;
}

// GET /api/blog — list all posts (admin: includes drafts)
export async function GET(request: NextRequest) {
  const admin = isAuthorized(request);

  let query = supabase
    .from("blog_posts")
    .select("id, slug, title, excerpt, cover_image_url, author, published, published_at, created_at, updated_at")
    .order("created_at", { ascending: false });

  if (!admin) {
    query = query.eq("published", true);
  }

  const { data, error } = await query;

  if (error) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }

  return NextResponse.json(data);
}

// POST /api/blog — create a new post (admin only)
export async function POST(request: NextRequest) {
  if (!isAuthorized(request)) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    const body = await request.json();

    if (!body.title || !body.slug) {
      return NextResponse.json(
        { error: "Title and slug are required." },
        { status: 400 }
      );
    }

    const { data, error } = await supabase
      .from("blog_posts")
      .insert({
        slug: body.slug,
        title: body.title,
        excerpt: body.excerpt || null,
        content: body.content || "",
        cover_image_url: body.cover_image_url || null,
        author: body.author || "Stalela",
        published: body.published || false,
        published_at: body.published ? new Date().toISOString() : null,
      })
      .select()
      .single();

    if (error) {
      return NextResponse.json({ error: error.message }, { status: 500 });
    }

    return NextResponse.json(data, { status: 201 });
  } catch {
    return NextResponse.json({ error: "Invalid request." }, { status: 400 });
  }
}
