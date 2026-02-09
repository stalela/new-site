import type { MetadataRoute } from "next";
import { routing } from "@/i18n/routing";
import { serviceCategories } from "@/lib/services-data";
import { SITE_URL } from "@/lib/seo";

/**
 * Dynamic sitemap covering every locale × every route.
 * Next.js serves this at /sitemap.xml automatically.
 */
export default function sitemap(): MetadataRoute.Sitemap {
  const locales = routing.locales;

  // Static pages
  const staticPaths = [
    "",
    "/services",
    "/pricing",
    "/how-it-works",
    "/contact",
    "/register",
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

  return [...staticEntries, ...categoryEntries, ...serviceEntries];
}
