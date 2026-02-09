"use client";

import { useState, useEffect, useCallback } from "react";
import { Container } from "@/components/Container";
import { Card } from "@/components/Card";
import { Button } from "@/components/Button";
import { Lock, Plus, Edit, Trash2, Eye, EyeOff } from "lucide-react";
import Link from "next/link";
import { useParams } from "next/navigation";

interface BlogPost {
  id: string;
  slug: string;
  title: string;
  excerpt: string | null;
  published: boolean;
  published_at: string | null;
  created_at: string;
  updated_at: string;
}

export default function AdminPage() {
  const params = useParams();
  const locale = params.locale as string;
  const [password, setPassword] = useState("");
  const [authenticated, setAuthenticated] = useState(false);
  const [posts, setPosts] = useState<BlogPost[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const storedPassword = typeof window !== "undefined"
    ? sessionStorage.getItem("admin_password") || ""
    : "";

  useEffect(() => {
    if (storedPassword) {
      setPassword(storedPassword);
      setAuthenticated(true);
    }
  }, [storedPassword]);

  const fetchPosts = useCallback(async (pw: string) => {
    setLoading(true);
    try {
      const res = await fetch("/api/blog", {
        headers: { Authorization: pw },
      });
      if (!res.ok) throw new Error("Failed to fetch posts");
      const data = await res.json();
      setPosts(data);
    } catch {
      setError("Failed to load posts.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (authenticated && password) {
      fetchPosts(password);
    }
  }, [authenticated, password, fetchPosts]);

  function handleLogin(e: React.FormEvent) {
    e.preventDefault();
    sessionStorage.setItem("admin_password", password);
    setAuthenticated(true);
  }

  async function handleDelete(slug: string) {
    if (!confirm(`Delete "${slug}"? This cannot be undone.`)) return;

    const res = await fetch(`/api/blog/${slug}`, {
      method: "DELETE",
      headers: { Authorization: password },
    });

    if (res.ok) {
      setPosts((prev) => prev.filter((p) => p.slug !== slug));
    }
  }

  async function handleTogglePublish(post: BlogPost) {
    const res = await fetch(`/api/blog/${post.slug}`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        Authorization: password,
      },
      body: JSON.stringify({ published: !post.published }),
    });

    if (res.ok) {
      const updated = await res.json();
      setPosts((prev) => prev.map((p) => (p.slug === post.slug ? updated : p)));
    }
  }

  if (!authenticated) {
    return (
      <div className="flex min-h-[60vh] items-center justify-center">
        <Card className="w-full max-w-sm">
          <div className="mb-6 flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-copper-100">
              <Lock className="h-5 w-5 text-copper-600" />
            </div>
            <h1 className="text-xl font-bold text-gray-900">Blog Admin</h1>
          </div>
          {error && <p className="mb-4 text-sm text-red-600">{error}</p>}
          <form onSubmit={handleLogin} className="space-y-4">
            <div>
              <label htmlFor="password" className="mb-1.5 block text-sm font-medium text-gray-700">
                Password
              </label>
              <input
                id="password"
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm text-gray-900 placeholder-gray-400 focus:border-copper-600 focus:outline-none focus:ring-1 focus:ring-copper-600"
                placeholder="Enter admin password"
              />
            </div>
            <Button type="submit" className="w-full">
              Sign In
            </Button>
          </form>
        </Card>
      </div>
    );
  }

  return (
    <section className="py-12">
      <Container>
        <div className="mb-8 flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Blog Admin</h1>
            <p className="mt-1 text-sm text-gray-500">
              {posts.length} post{posts.length !== 1 ? "s" : ""}
            </p>
          </div>
          <Link
            href={`/${locale}/admin/editor`}
            className="inline-flex items-center gap-2 rounded-lg bg-copper-600 px-4 py-2.5 text-sm font-medium text-white transition-colors hover:bg-copper-700"
          >
            <Plus className="h-4 w-4" />
            New Post
          </Link>
        </div>

        {loading ? (
          <div className="py-12 text-center text-gray-500">Loading…</div>
        ) : posts.length === 0 ? (
          <Card className="py-12 text-center">
            <p className="text-gray-500">No posts yet. Create your first one!</p>
          </Card>
        ) : (
          <div className="space-y-3">
            {posts.map((post) => (
              <Card key={post.id} className="flex items-center justify-between gap-4">
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2">
                    <h2 className="truncate text-sm font-semibold text-gray-900">
                      {post.title}
                    </h2>
                    <span
                      className={`inline-flex shrink-0 rounded-full px-2 py-0.5 text-xs font-medium ${
                        post.published
                          ? "bg-green-100 text-green-700"
                          : "bg-yellow-100 text-yellow-700"
                      }`}
                    >
                      {post.published ? "Published" : "Draft"}
                    </span>
                  </div>
                  <p className="mt-0.5 truncate text-xs text-gray-500">
                    /{post.slug} · Updated{" "}
                    {new Date(post.updated_at).toLocaleDateString("en-ZA")}
                  </p>
                </div>
                <div className="flex shrink-0 items-center gap-2">
                  <button
                    onClick={() => handleTogglePublish(post)}
                    className="rounded-lg p-2 text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-600"
                    title={post.published ? "Unpublish" : "Publish"}
                  >
                    {post.published ? (
                      <EyeOff className="h-4 w-4" />
                    ) : (
                      <Eye className="h-4 w-4" />
                    )}
                  </button>
                  <Link
                    href={`/${locale}/admin/editor?slug=${post.slug}`}
                    className="rounded-lg p-2 text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-600"
                    title="Edit"
                  >
                    <Edit className="h-4 w-4" />
                  </Link>
                  <button
                    onClick={() => handleDelete(post.slug)}
                    className="rounded-lg p-2 text-gray-400 transition-colors hover:bg-red-50 hover:text-red-600"
                    title="Delete"
                  >
                    <Trash2 className="h-4 w-4" />
                  </button>
                </div>
              </Card>
            ))}
          </div>
        )}
      </Container>
    </section>
  );
}
