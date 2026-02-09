import type { Metadata } from "next";
import Image from "next/image";
import { getTranslations, setRequestLocale } from "next-intl/server";
import { Link } from "@/i18n/navigation";
import { Section } from "@/components/Section";
import { Card } from "@/components/Card";
import { buildPageMetadata } from "@/lib/seo";
import { supabase } from "@/lib/supabase";
import { Calendar, User, ArrowRight } from "lucide-react";
import type { Locale } from "@/i18n/routing";

interface Props {
  params: Promise<{ locale: string }>;
}

export const revalidate = 3600; // ISR: revalidate every hour

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: "Metadata" });
  return buildPageMetadata({
    title: t("blog.title"),
    description: t("blog.description"),
    path: "/blog",
    locale: locale as Locale,
  });
}

export default async function BlogPage({ params }: Props) {
  const { locale } = await params;
  setRequestLocale(locale);
  const t = await getTranslations("Blog");

  const { data: posts } = await supabase
    .from("blog_posts")
    .select("slug, title, excerpt, cover_image_url, author, published_at")
    .eq("published", true)
    .order("published_at", { ascending: false });

  return (
    <Section>
      <div className="mx-auto max-w-2xl text-center">
        <h1 className="text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
          {t("heading")}
        </h1>
        <p className="mt-4 text-lg text-gray-600">
          {t("description")}
        </p>
      </div>

      {posts && posts.length > 0 ? (
        <div className="mt-12 grid gap-8 sm:grid-cols-2 lg:grid-cols-3">
          {posts.map((post) => (
            <Link key={post.slug} href={`/blog/${post.slug}`} className="group">
              <Card className="h-full transition-shadow group-hover:shadow-md">
                {post.cover_image_url && (
                  <div className="relative -mx-6 -mt-6 mb-4 aspect-[16/9] overflow-hidden rounded-t-xl">
                    <Image
                      src={post.cover_image_url}
                      alt={post.title}
                      fill
                      className="object-cover transition-transform group-hover:scale-105"
                      sizes="(max-width: 640px) 100vw, (max-width: 1024px) 50vw, 33vw"
                    />
                  </div>
                )}
                <h2 className="text-lg font-semibold text-gray-900 group-hover:text-copper-700">
                  {post.title}
                </h2>
                {post.excerpt && (
                  <p className="mt-2 line-clamp-3 text-sm text-gray-600">
                    {post.excerpt}
                  </p>
                )}
                <div className="mt-4 flex items-center gap-4 text-xs text-gray-400">
                  <span className="flex items-center gap-1">
                    <User className="h-3.5 w-3.5" />
                    {post.author}
                  </span>
                  {post.published_at && (
                    <span className="flex items-center gap-1">
                      <Calendar className="h-3.5 w-3.5" />
                      {new Date(post.published_at).toLocaleDateString("en-ZA", {
                        year: "numeric",
                        month: "short",
                        day: "numeric",
                      })}
                    </span>
                  )}
                </div>
                <span className="mt-4 inline-flex items-center gap-1 text-sm font-medium text-copper-600 group-hover:text-copper-700">
                  {t("readMore")}
                  <ArrowRight className="h-3.5 w-3.5 transition-transform group-hover:translate-x-0.5" />
                </span>
              </Card>
            </Link>
          ))}
        </div>
      ) : (
        <div className="mt-12 rounded-xl border border-gray-200 bg-gray-50 p-12 text-center">
          <p className="text-gray-500">{t("noPosts")}</p>
        </div>
      )}
    </Section>
  );
}
