import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { getTranslations, setRequestLocale } from "next-intl/server";
import { Section } from "@/components/Section";
import { CTA } from "@/components/CTA";
import { ServicePageContent } from "@/components/ServicePageContent";
import { WebDevContent } from "@/components/WebDevContent";
import { BrandingContent } from "@/components/studio/BrandingContent";
import { LogoContent } from "@/components/studio/LogoContent";
import { BusinessCardsContent } from "@/components/studio/BusinessCardsContent";
import { GraphicDesignContent } from "@/components/studio/GraphicDesignContent";
import { GraphicDesignCourseContent } from "@/components/studio/GraphicDesignCourseContent";
import { SEOContent } from "@/components/studio/SEOContent";
import { DigitalMarketingContent } from "@/components/studio/DigitalMarketingContent";
import { CustomSoftwareContent } from "@/components/studio/CustomSoftwareContent";
import { PaymentContent } from "@/components/studio/PaymentContent";
import { MobileAppsContent } from "@/components/studio/MobileAppsContent";
import { AIAgentsContent } from "@/components/studio/AIAgentsContent";
import { ChatbotsContent } from "@/components/studio/ChatbotsContent";
import {
  serviceCategories,
  getCategoryBySlug,
  getServiceBySlug,
} from "@/lib/services-data";
import { JsonLd } from "@/components/JsonLd";
import { buildPageMetadata, SITE_URL } from "@/lib/seo";
import type { Locale } from "@/i18n/routing";

interface Props {
  params: Promise<{ locale: string; categorySlug: string; serviceSlug: string }>;
}

export async function generateStaticParams() {
  return serviceCategories.flatMap((c) =>
    c.services.map((s) => ({ categorySlug: c.slug, serviceSlug: s.slug })),
  );
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { locale, categorySlug, serviceSlug } = await params;
  const service = getServiceBySlug(serviceSlug);
  if (!service) return {};
  return buildPageMetadata({
    title: service.title,
    description: service.description,
    path: `/services/${categorySlug}/${serviceSlug}`,
    locale: locale as Locale,
  });
}

/** Custom rich pages for specific services */
const customPages: Record<string, React.ComponentType> = {
  "website-development": WebDevContent,
  "branding-and-design": BrandingContent,
  "logo-design": LogoContent,
  "business-cards": BusinessCardsContent,
  "graphic-design": GraphicDesignContent,
  "graphic-design-course": GraphicDesignCourseContent,
  "seo": SEOContent,
  "digital-marketing": DigitalMarketingContent,
  "custom-software": CustomSoftwareContent,
  "payment-integration": PaymentContent,
  "mobile-apps": MobileAppsContent,
  "ai-agents": AIAgentsContent,
  "chatbots": ChatbotsContent,
};

export default async function ServicePage({ params }: Props) {
  const { locale, categorySlug, serviceSlug } = await params;
  setRequestLocale(locale);
  const category = getCategoryBySlug(categorySlug);
  const service = getServiceBySlug(serviceSlug);
  if (!category || !service) notFound();

  const serviceJsonLd = {
    "@context": "https://schema.org",
    "@type": "Service",
    name: service.title,
    description: service.description,
    provider: {
      "@type": "Organization",
      name: "Stalela",
      url: SITE_URL,
    },
  };

  const breadcrumbJsonLd = {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    itemListElement: [
      {
        "@type": "ListItem",
        position: 1,
        name: "Services",
        item: `${SITE_URL}/${locale}/services`,
      },
      {
        "@type": "ListItem",
        position: 2,
        name: category.name,
        item: `${SITE_URL}/${locale}/services/${categorySlug}`,
      },
      {
        "@type": "ListItem",
        position: 3,
        name: service.title,
        item: `${SITE_URL}/${locale}/services/${categorySlug}/${serviceSlug}`,
      },
    ],
  };

  const CustomPage = customPages[serviceSlug];

  if (CustomPage) {
    return (
      <>
        <JsonLd data={serviceJsonLd} />
        <JsonLd data={breadcrumbJsonLd} />
        <Section>
          <CustomPage />
        </Section>
      </>
    );
  }

  const t = await getTranslations("ServicePage");

  return (
    <>
      <JsonLd data={serviceJsonLd} />
      <JsonLd data={breadcrumbJsonLd} />
      <Section>
        <ServicePageContent serviceSlug={serviceSlug} />
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
