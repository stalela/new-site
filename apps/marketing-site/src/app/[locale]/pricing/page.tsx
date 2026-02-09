import type { Metadata } from "next";
import Image from "next/image";
import { getTranslations, setRequestLocale } from "next-intl/server";
import { Section } from "@/components/Section";
import { PricingTable } from "@/components/PricingTable";
import { CTA } from "@/components/CTA";
import { buildPageMetadata } from "@/lib/seo";
import type { Locale } from "@/i18n/routing";

interface Props {
  params: Promise<{ locale: string }>;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: "Metadata" });
  return buildPageMetadata({
    title: t("pricing.title"),
    description: t("pricing.description"),
    path: "/pricing",
    locale: locale as Locale,
  });
}

const tierKeys = ["starter", "growth", "managed"] as const;

export default async function PricingPage({ params }: Props) {
  const { locale } = await params;
  setRequestLocale(locale);
  const t = await getTranslations("Pricing");

  const tiers = tierKeys.map((key) => ({
    name: t(`tiers.${key}.name`),
    price: t(`tiers.${key}.price`),
    description: t(`tiers.${key}.description`),
    features: t.raw(`tiers.${key}.features`) as string[],
    ctaText: t(`tiers.${key}.ctaText`),
    ctaHref: key === "managed" ? "/contact" : "/register",
    highlighted: key === "growth",
  }));

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
        <div className="relative mx-auto mt-10 aspect-[16/7] max-w-3xl overflow-hidden rounded-2xl">
          <Image
            src="/images/generated/pricing-page.png"
            alt={t("imageAlt")}
            fill
            className="object-cover"
            sizes="(max-width: 768px) 100vw, 768px"
          />
        </div>
        <div className="mt-12">
          <PricingTable tiers={tiers} />
        </div>
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
