import type { Metadata } from "next";
import { getTranslations, setRequestLocale } from "next-intl/server";
import { Section } from "@/components/Section";
import { CTA } from "@/components/CTA";
import { ServicesContent } from "@/components/ServicesContent";
import { buildPageMetadata } from "@/lib/seo";
import type { Locale } from "@/i18n/routing";

interface Props {
  params: Promise<{ locale: string }>;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: "Metadata" });
  return buildPageMetadata({
    title: t("services.title"),
    description: t("services.description"),
    path: "/services",
    locale: locale as Locale,
  });
}

export default async function ServicesPage({ params }: Props) {
  const { locale } = await params;
  setRequestLocale(locale);
  const t = await getTranslations("Services");

  return (
    <>
      <Section>
        <div className="mx-auto max-w-2xl text-center">
          <h1 className="text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
            {t("heading")}
          </h1>
          <p className="mt-4 text-lg text-gray-600">
            {t("description")}
          </p>
        </div>
        <ServicesContent />
      </Section>

      <CTA
        headline={t("ctaHeadline")}
        description={t("ctaDescription")}
        ctaText={t("ctaText")}
        ctaHref="/contact"
      />
    </>
  );
}
