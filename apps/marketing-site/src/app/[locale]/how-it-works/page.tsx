import type { Metadata } from "next";
import Image from "next/image";
import { getTranslations, setRequestLocale } from "next-intl/server";
import { Section } from "@/components/Section";
import { CTA } from "@/components/CTA";
import { FileText, UserCheck, Send, TrendingUp } from "lucide-react";
import { buildPageMetadata } from "@/lib/seo";
import type { Locale } from "@/i18n/routing";

interface Props {
  params: Promise<{ locale: string }>;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: "Metadata" });
  return buildPageMetadata({
    title: t("howItWorks.title"),
    description: t("howItWorks.description"),
    path: "/how-it-works",
    locale: locale as Locale,
  });
}

const stepIcons = [FileText, UserCheck, Send, TrendingUp];
const stepKeys = ["01", "02", "03", "04"] as const;

export default async function HowItWorksPage({ params }: Props) {
  const { locale } = await params;
  setRequestLocale(locale);
  const t = await getTranslations("HowItWorks");

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
        <div className="relative mx-auto mt-10 aspect-[16/7] max-w-4xl overflow-hidden rounded-2xl">
          <Image
            src="/images/generated/how-it-works-page.png"
            alt={t("imageAlt")}
            fill
            className="object-cover"
            sizes="(max-width: 768px) 100vw, 896px"
          />
        </div>
      </Section>

      <Section className="bg-gray-50 pt-0 sm:pt-0">
        <div className="space-y-16">
          {stepKeys.map((key, i) => {
            const Icon = stepIcons[i];
            const details = t.raw(`steps.${key}.details`) as string[];
            return (
              <div
                key={key}
                className={`grid items-center gap-10 lg:grid-cols-2 ${
                  i % 2 !== 0 ? "lg:direction-rtl" : ""
                }`}
              >
                <div className={i % 2 !== 0 ? "lg:order-2" : ""}>
                  <div className="flex items-center gap-3">
                    <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-copper-100">
                      <Icon className="h-5 w-5 text-copper-600" />
                    </div>
                    <span className="text-sm font-semibold text-copper-600">
                      {t("step", { number: key })}
                    </span>
                  </div>
                  <h2 className="mt-4 text-2xl font-bold text-gray-900">
                    {t(`steps.${key}.title`)}
                  </h2>
                  <p className="mt-3 leading-relaxed text-gray-600">
                    {t(`steps.${key}.description`)}
                  </p>
                </div>
                <div
                  className={`rounded-xl border border-gray-200 bg-white p-6 ${
                    i % 2 !== 0 ? "lg:order-1" : ""
                  }`}
                >
                  <ul className="space-y-3">
                    {details.map((d) => (
                      <li
                        key={d}
                        className="flex items-start gap-2 text-sm text-gray-600"
                      >
                        <span className="mt-1.5 h-1.5 w-1.5 flex-shrink-0 rounded-full bg-copper-600" />
                        {d}
                      </li>
                    ))}
                  </ul>
                </div>
              </div>
            );
          })}
        </div>
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
