import type { Metadata } from "next";
import { routing, type Locale } from "@/i18n/routing";

const SITE_URL = "https://stalela.co.za";

/**
 * Build `alternates` metadata with canonical + hreflang for all locales.
 * @param path — the locale-free path, e.g. "/services" or "/pricing"
 * @param locale — the current page locale (used for canonical)
 */
export function buildAlternates(path: string, locale: Locale) {
  const languages: Record<string, string> = {};
  for (const loc of routing.locales) {
    languages[loc] = `${SITE_URL}/${loc}${path}`;
  }
  // x-default points to default locale
  languages["x-default"] = `${SITE_URL}/${routing.defaultLocale}${path}`;

  return {
    canonical: `${SITE_URL}/${locale}${path}`,
    languages,
  };
}

/**
 * Build standard OG + Twitter metadata for a page.
 */
export function buildPageMetadata({
  title,
  description,
  path,
  locale,
}: {
  title: string;
  description: string;
  path: string;
  locale: Locale;
}): Metadata {
  const alternates = buildAlternates(path, locale);

  return {
    title,
    description,
    alternates,
    openGraph: {
      title,
      description,
      url: alternates.canonical,
      locale: locale === "en" ? "en_ZA" : locale,
    },
    twitter: {
      title,
      description,
    },
  };
}

/**
 * Resolve a full URL from a locale-free path.
 */
export function absoluteUrl(path: string, locale: Locale = "en"): string {
  return `${SITE_URL}/${locale}${path}`;
}

export { SITE_URL };
