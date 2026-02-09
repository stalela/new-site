import { NextRequest, NextResponse } from "next/server";
import { supabase } from "@/lib/supabase";

function isAuthorized(request: NextRequest): boolean {
  const auth = request.headers.get("authorization");
  return auth === process.env.ADMIN_PASSWORD;
}

interface RouteParams {
  params: Promise<{ slug: string }>;
}

// GET /api/blog/[slug] — fetch a single post
export async function GET(request: NextRequest, { params }: RouteParams) {
  const { slug } = await params;
  const admin = isAuthorized(request);

  let query = supabase
    .from("blog_posts")
    .select("*")
    .eq("slug", slug)
    .single();

  if (!admin) {
    query = supabase
      .from("blog_posts")
      .select("*")
      .eq("slug", slug)
      .eq("published", true)
      .single();
  }

  const { data, error } = await query;

  if (error || !data) {
    return NextResponse.json({ error: "Post not found." }, { status: 404 });
  }

  return NextResponse.json(data);
}

// PUT /api/blog/[slug] — update a post (admin only)
export async function PUT(request: NextRequest, { params }: RouteParams) {
  if (!isAuthorized(request)) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { slug } = await params;

  try {
    const body = await request.json();

    const updates: Record<string, unknown> = {
      updated_at: new Date().toISOString(),
    };

    if (body.title !== undefined) updates.title = body.title;
    if (body.slug !== undefined) updates.slug = body.slug;
    if (body.excerpt !== undefined) updates.excerpt = body.excerpt;
    if (body.content !== undefined) updates.content = body.content;
    if (body.cover_image_url !== undefined) updates.cover_image_url = body.cover_image_url;
    if (body.author !== undefined) updates.author = body.author;
    if (body.published !== undefined) {
      updates.published = body.published;
      // Set published_at when first published
      if (body.published) {
        updates.published_at = body.published_at || new Date().toISOString();
      }
    }

    const { data, error } = await supabase
      .from("blog_posts")
      .update(updates)
      .eq("slug", slug)
      .select()
      .single();

    if (error) {
      return NextResponse.json({ error: error.message }, { status: 500 });
    }

    if (!data) {
      return NextResponse.json({ error: "Post not found." }, { status: 404 });
    }

    return NextResponse.json(data);
  } catch {
    return NextResponse.json({ error: "Invalid request." }, { status: 400 });
  }
}

// DELETE /api/blog/[slug] — delete a post (admin only)
export async function DELETE(request: NextRequest, { params }: RouteParams) {
  if (!isAuthorized(request)) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { slug } = await params;

  const { error } = await supabase
    .from("blog_posts")
    .delete()
    .eq("slug", slug);

  if (error) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }

  return NextResponse.json({ success: true });
}
