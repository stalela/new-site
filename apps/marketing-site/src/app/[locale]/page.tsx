import type { Metadata } from "next";
import Image from "next/image";
import { getTranslations, setRequestLocale } from "next-intl/server";
import { HeroSlider } from "@/components/HeroSlider";
import { Section } from "@/components/Section";
import { ServiceGrid } from "@/components/ServiceGrid";
import { CTA } from "@/components/CTA";
import { Button } from "@/components/Button";
import { HomePageTrustIndicators } from "@/components/HomePageTrustIndicators";
import { JsonLd } from "@/components/JsonLd";
import { buildPageMetadata, SITE_URL } from "@/lib/seo";
import type { Locale } from "@/i18n/routing";

interface Props {
  params: Promise<{ locale: string }>;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: "Metadata" });
  return buildPageMetadata({
    title: t("home.title"),
    description: t("home.description"),
    path: "",
    locale: locale as Locale,
  });
}

const stepNumbers = ["1", "2", "3", "4"] as const;

export default async function HomePage({ params }: Props) {
  const { locale } = await params;
  setRequestLocale(locale);
  const t = await getTranslations("HomePage");

  const organizationJsonLd = {
    "@context": "https://schema.org",
    "@type": "Organization",
    name: "Stalela",
    url: SITE_URL,
    logo: `${SITE_URL}/images/logo-icon.svg`,
    contactPoint: {
      "@type": "ContactPoint",
      email: "hello@stalela.co.za",
      contactType: "customer service",
      areaServed: "ZA",
      availableLanguage: ["English", "isiZulu", "Afrikaans", "isiXhosa"],
    },
    sameAs: [],
  };

  const websiteJsonLd = {
    "@context": "https://schema.org",
    "@type": "WebSite",
    name: "Stalela",
    url: SITE_URL,
  };

  return (
    <>
      <JsonLd data={organizationJsonLd} />
      <JsonLd data={websiteJsonLd} />

      {/* Hero Slider */}
      <HeroSlider />

      {/* Services overview */}
      <Section className="bg-gray-50" id="services">
        <div className="mx-auto max-w-2xl text-center">
          <h2 className="text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
            {t("servicesHeading")}
          </h2>
          <p className="mt-4 text-lg text-gray-600">
            {t("servicesSubheading")}
          </p>
        </div>
        <div className="relative mx-auto mt-10 aspect-[16/7] max-w-4xl overflow-hidden rounded-2xl">
          <Image
            src="/images/generated/category-digital-services.png"
            alt={t("servicesImageAlt")}
            fill
            className="object-cover"
            sizes="(max-width: 768px) 100vw, 896px"
          />
        </div>
        <div className="mt-12">
          <ServiceGrid categorySlug="tech-and-digital-services" />
        </div>
        <div className="mt-8 text-center">
          <Button href="/services" variant="outline">
            {t("viewAllServices")}
          </Button>
        </div>
      </Section>

      {/* How it works */}
      <Section id="how-it-works">
        <div className="mx-auto max-w-2xl text-center">
          <h2 className="text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
            {t("howItWorksHeading")}
          </h2>
          <p className="mt-4 text-lg text-gray-600">
            {t("howItWorksSubheading")}
          </p>
        </div>
        <div className="relative mx-auto mt-10 aspect-[16/7] max-w-4xl overflow-hidden rounded-2xl">
          <Image
            src="/images/generated/how-it-works.png"
            alt={t("howItWorksImageAlt")}
            fill
            className="object-cover"
            sizes="(max-width: 768px) 100vw, 896px"
          />
        </div>
        <div className="mt-12 grid gap-8 sm:grid-cols-2 lg:grid-cols-4">
          {stepNumbers.map((step) => (
            <div key={step} className="text-center">
              <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-full bg-copper-600 text-lg font-bold text-white">
                {step}
              </div>
              <h3 className="mt-4 font-semibold text-gray-900">{t(`steps.${step}.title`)}</h3>
              <p className="mt-2 text-sm leading-relaxed text-gray-600">
                {t(`steps.${step}.description`)}
              </p>
            </div>
          ))}
        </div>
      </Section>

      {/* Trust indicators */}
      <Section className="bg-gray-50">
        <div className="relative mx-auto mb-10 aspect-[16/7] max-w-3xl overflow-hidden rounded-2xl">
          <Image
            src="/images/generated/trust-indicators.png"
            alt={t("trustImageAlt")}
            fill
            className="object-cover"
            sizes="(max-width: 768px) 100vw, 768px"
          />
        </div>
        <HomePageTrustIndicators />
      </Section>

      {/* CTA */}
      <CTA
        headline={t("ctaHeadline")}
        description={t("ctaDescription")}
        ctaText={t("ctaText")}
        ctaHref="/contact"
      />
    </>
  );
}
