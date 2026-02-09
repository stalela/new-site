"use client";

import { useState, useEffect, useCallback, Suspense } from "react";
import { useRouter, useSearchParams, useParams } from "next/navigation";
import { Container } from "@/components/Container";
import { Card } from "@/components/Card";
import { Button } from "@/components/Button";
import { BlogContent } from "@/components/BlogContent";
import { ArrowLeft, Save, Eye } from "lucide-react";
import Link from "next/link";

interface PostForm {
  title: string;
  slug: string;
  excerpt: string;
  author: string;
  cover_image_url: string;
  content: string;
  published: boolean;
}

const emptyForm: PostForm = {
  title: "",
  slug: "",
  excerpt: "",
  author: "Stalela",
  cover_image_url: "",
  content: "",
  published: false,
};

function slugify(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function EditorContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const params = useParams();
  const locale = params.locale as string;
  const editSlug = searchParams.get("slug");

  const [form, setForm] = useState<PostForm>(emptyForm);
  const [preview, setPreview] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [isEdit, setIsEdit] = useState(false);

  const password =
    typeof window !== "undefined"
      ? sessionStorage.getItem("admin_password") || ""
      : "";

  const fetchPost = useCallback(async () => {
    if (!editSlug || !password) return;

    const res = await fetch(`/api/blog/${editSlug}`, {
      headers: { Authorization: password },
    });

    if (res.ok) {
      const data = await res.json();
      setForm({
        title: data.title || "",
        slug: data.slug || "",
        excerpt: data.excerpt || "",
        author: data.author || "Stalela",
        cover_image_url: data.cover_image_url || "",
        content: data.content || "",
        published: data.published || false,
      });
      setIsEdit(true);
    }
  }, [editSlug, password]);

  useEffect(() => {
    if (!password) {
      router.push(`/${locale}/admin`);
      return;
    }
    fetchPost();
  }, [password, fetchPost, router, locale]);

  function updateField(field: keyof PostForm, value: string | boolean) {
    setForm((prev) => {
      const next = { ...prev, [field]: value };
      // Auto-generate slug from title when creating
      if (field === "title" && !isEdit) {
        next.slug = slugify(value as string);
      }
      return next;
    });
  }

  async function handleSave() {
    setError("");
    if (!form.title || !form.slug) {
      setError("Title and slug are required.");
      return;
    }

    setSaving(true);
    try {
      const url = isEdit ? `/api/blog/${editSlug}` : "/api/blog";
      const method = isEdit ? "PUT" : "POST";

      const res = await fetch(url, {
        method,
        headers: {
          "Content-Type": "application/json",
          Authorization: password,
        },
        body: JSON.stringify(form),
      });

      if (!res.ok) {
        const data = await res.json().catch(() => null);
        throw new Error(data?.error || "Failed to save");
      }

      router.push(`/${locale}/admin`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save post.");
    } finally {
      setSaving(false);
    }
  }

  const inputClasses =
    "w-full rounded-lg border border-gray-300 px-3 py-2 text-sm text-gray-900 placeholder-gray-400 focus:border-copper-600 focus:outline-none focus:ring-1 focus:ring-copper-600";

  return (
    <section className="py-12">
      <Container>
        {/* Header */}
        <div className="mb-8 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link
              href={`/${locale}/admin`}
              className="rounded-lg p-2 text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-600"
            >
              <ArrowLeft className="h-5 w-5" />
            </Link>
            <h1 className="text-2xl font-bold text-gray-900">
              {isEdit ? "Edit Post" : "New Post"}
            </h1>
          </div>
          <div className="flex items-center gap-3">
            <button
              onClick={() => setPreview(!preview)}
              className={`inline-flex items-center gap-2 rounded-lg px-3 py-2 text-sm font-medium transition-colors ${
                preview
                  ? "bg-copper-100 text-copper-700"
                  : "text-gray-600 hover:bg-gray-100"
              }`}
            >
              <Eye className="h-4 w-4" />
              Preview
            </button>
            <Button
              type="button"
              onClick={handleSave}
              disabled={saving}
            >
              <Save className="mr-2 h-4 w-4" />
              {saving ? "Saving…" : "Save"}
            </Button>
          </div>
        </div>

        {error && (
          <div className="mb-6 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
            {error}
          </div>
        )}

        <div className="grid gap-8 lg:grid-cols-3">
          {/* Main editor */}
          <div className="lg:col-span-2">
            <Card>
              {preview ? (
                <div>
                  <h2 className="mb-4 text-2xl font-bold text-gray-900">{form.title || "Untitled"}</h2>
                  <BlogContent content={form.content || "*No content yet*"} />
                </div>
              ) : (
                <div className="space-y-5">
                  <div>
                    <label htmlFor="title" className="mb-1.5 block text-sm font-medium text-gray-700">
                      Title <span className="text-red-500">*</span>
                    </label>
                    <input
                      id="title"
                      type="text"
                      value={form.title}
                      onChange={(e) => updateField("title", e.target.value)}
                      className={inputClasses}
                      placeholder="Post title"
                    />
                  </div>

                  <div>
                    <label htmlFor="slug" className="mb-1.5 block text-sm font-medium text-gray-700">
                      Slug <span className="text-red-500">*</span>
                    </label>
                    <div className="flex items-center gap-2">
                      <span className="text-sm text-gray-400">/blog/</span>
                      <input
                        id="slug"
                        type="text"
                        value={form.slug}
                        onChange={(e) => updateField("slug", e.target.value)}
                        className={inputClasses}
                        placeholder="post-slug"
                      />
                    </div>
                  </div>

                  <div>
                    <label htmlFor="content" className="mb-1.5 block text-sm font-medium text-gray-700">
                      Content (Markdown)
                    </label>
                    <textarea
                      id="content"
                      value={form.content}
                      onChange={(e) => updateField("content", e.target.value)}
                      rows={20}
                      className={`${inputClasses} font-mono text-xs`}
                      placeholder="Write your blog post in Markdown…"
                    />
                  </div>
                </div>
              )}
            </Card>
          </div>

          {/* Sidebar */}
          <div className="space-y-6">
            <Card>
              <h3 className="mb-4 text-sm font-semibold text-gray-900">Post Settings</h3>
              <div className="space-y-4">
                <div>
                  <label htmlFor="excerpt" className="mb-1.5 block text-sm font-medium text-gray-700">
                    Excerpt
                  </label>
                  <textarea
                    id="excerpt"
                    value={form.excerpt}
                    onChange={(e) => updateField("excerpt", e.target.value)}
                    rows={3}
                    className={inputClasses}
                    placeholder="Brief description for listings and SEO"
                  />
                </div>

                <div>
                  <label htmlFor="author" className="mb-1.5 block text-sm font-medium text-gray-700">
                    Author
                  </label>
                  <input
                    id="author"
                    type="text"
                    value={form.author}
                    onChange={(e) => updateField("author", e.target.value)}
                    className={inputClasses}
                  />
                </div>

                <div>
                  <label htmlFor="cover_image_url" className="mb-1.5 block text-sm font-medium text-gray-700">
                    Cover Image URL
                  </label>
                  <input
                    id="cover_image_url"
                    type="url"
                    value={form.cover_image_url}
                    onChange={(e) => updateField("cover_image_url", e.target.value)}
                    className={inputClasses}
                    placeholder="https://…"
                  />
                </div>

                <div className="flex items-center gap-3 rounded-lg bg-gray-50 px-4 py-3">
                  <input
                    id="published"
                    type="checkbox"
                    checked={form.published}
                    onChange={(e) => updateField("published", e.target.checked)}
                    className="h-4 w-4 rounded border-gray-300 text-copper-600 focus:ring-copper-600"
                  />
                  <label htmlFor="published" className="text-sm font-medium text-gray-700">
                    Published
                  </label>
                </div>
              </div>
            </Card>
          </div>
        </div>
      </Container>
    </section>
  );
}

export default function EditorPage() {
  return (
    <Suspense fallback={<div className="flex min-h-[60vh] items-center justify-center text-gray-500">Loading editor…</div>}>
      <EditorContent />
    </Suspense>
  );
}
