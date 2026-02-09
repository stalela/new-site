import type { MetadataRoute } from "next";
import { routing } from "@/i18n/routing";
import { serviceCategories } from "@/lib/services-data";
import { SITE_URL } from "@/lib/seo";
import { supabase } from "@/lib/supabase";

/**
 * Dynamic sitemap covering every locale × every route.
 * Next.js serves this at /sitemap.xml automatically.
 */
export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
  const locales = routing.locales;

  // Static pages
  const staticPaths = [
    "",
    "/services",
    "/pricing",
    "/how-it-works",
    "/contact",
    "/register",
    "/blog",
  ];

  const staticEntries: MetadataRoute.Sitemap = staticPaths.flatMap((path) =>
    locales.map((locale) => ({
      url: `${SITE_URL}/${locale}${path}`,
      lastModified: new Date(),
      changeFrequency: path === "" ? "weekly" as const : "monthly" as const,
      priority: path === "" ? 1.0 : 0.8,
      alternates: {
        languages: Object.fromEntries(
          locales.map((loc) => [loc, `${SITE_URL}/${loc}${path}`]),
        ),
      },
    })),
  );

  // Category pages
  const categoryEntries: MetadataRoute.Sitemap = serviceCategories.flatMap(
    (category) =>
      locales.map((locale) => ({
        url: `${SITE_URL}/${locale}/services/${category.slug}`,
        lastModified: new Date(),
        changeFrequency: "monthly" as const,
        priority: 0.7,
        alternates: {
          languages: Object.fromEntries(
            locales.map((loc) => [
              loc,
              `${SITE_URL}/${loc}/services/${category.slug}`,
            ]),
          ),
        },
      })),
  );

  // Service pages
  const serviceEntries: MetadataRoute.Sitemap = serviceCategories.flatMap(
    (category) =>
      category.services.flatMap((service) =>
        locales.map((locale) => ({
          url: `${SITE_URL}/${locale}/services/${category.slug}/${service.slug}`,
          lastModified: new Date(),
          changeFrequency: "monthly" as const,
          priority: 0.6,
          alternates: {
            languages: Object.fromEntries(
              locales.map((loc) => [
                loc,
                `${SITE_URL}/${loc}/services/${category.slug}/${service.slug}`,
              ]),
            ),
          },
        })),
      ),
  );

  // Blog post pages
  const { data: posts } = await supabase
    .from("blog_posts")
    .select("slug")
    .eq("published", true);

  const blogEntries: MetadataRoute.Sitemap = (posts || []).flatMap((post) =>
    locales.map((locale) => ({
      url: `${SITE_URL}/${locale}/blog/${post.slug}`,
      lastModified: new Date(),
      changeFrequency: "weekly" as const,
      priority: 0.6,
      alternates: {
        languages: Object.fromEntries(
          locales.map((loc) => [
            loc,
            `${SITE_URL}/${loc}/blog/${post.slug}`,
          ]),
        ),
      },
    })),
  );

  return [...staticEntries, ...categoryEntries, ...serviceEntries, ...blogEntries];
}
