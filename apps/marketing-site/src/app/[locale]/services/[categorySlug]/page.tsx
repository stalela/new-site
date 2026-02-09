import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { getTranslations, setRequestLocale } from "next-intl/server";
import { Section } from "@/components/Section";
import { CTA } from "@/components/CTA";
import { CategoryPageContent } from "@/components/CategoryPageContent";
import { serviceCategories, getCategoryBySlug } from "@/lib/services-data";
import { buildPageMetadata } from "@/lib/seo";
import type { Locale } from "@/i18n/routing";

interface Props {
  params: Promise<{ locale: string; categorySlug: string }>;
}

export async function generateStaticParams() {
  return serviceCategories.map((c) => ({ categorySlug: c.slug }));
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { locale, categorySlug } = await params;
  const category = getCategoryBySlug(categorySlug);
  if (!category) return {};
  return buildPageMetadata({
    title: category.name,
    description: category.description,
    path: `/services/${categorySlug}`,
    locale: locale as Locale,
  });
}

export default async function CategoryPage({ params }: Props) {
  const { locale, categorySlug } = await params;
  setRequestLocale(locale);
  const category = getCategoryBySlug(categorySlug);
  if (!category) notFound();

  const t = await getTranslations("CategoryPage");

  return (
    <>
      <Section>
        <CategoryPageContent categorySlug={categorySlug} />
      </Section>

      <CTA
        headline={t("ctaHeadline")}
        description={t("ctaDescription")}
        ctaText={t("ctaText")}
        ctaHref="/register"
      />
    </>
  );
}
