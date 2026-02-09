import type { Metadata } from "next";
import Image from "next/image";
import { getTranslations, setRequestLocale } from "next-intl/server";
import { Section } from "@/components/Section";
import { Card } from "@/components/Card";
import { LeadForm } from "@/components/LeadForm";
import { buildPageMetadata } from "@/lib/seo";
import type { Locale } from "@/i18n/routing";

interface Props {
  params: Promise<{ locale: string }>;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: "Metadata" });
  return buildPageMetadata({
    title: t("register.title"),
    description: t("register.description"),
    path: "/register",
    locale: locale as Locale,
  });
}

export default async function RegisterPage({ params }: Props) {
  const { locale } = await params;
  setRequestLocale(locale);
  const t = await getTranslations("Register");

  const registerFields = [
    { name: "fullName", label: t("fields.fullName.label"), type: "text" as const, required: true, placeholder: t("fields.fullName.placeholder") },
    { name: "email", label: t("fields.email.label"), type: "email" as const, required: true, placeholder: t("fields.email.placeholder") },
    { name: "phone", label: t("fields.phone.label"), type: "tel" as const, required: true, placeholder: t("fields.phone.placeholder") },
    { name: "businessName", label: t("fields.businessName.label"), type: "text" as const, required: true, placeholder: t("fields.businessName.placeholder") },
    { name: "directorsCount", label: t("fields.directorsCount.label"), type: "number" as const, required: true, placeholder: t("fields.directorsCount.placeholder") },
    { name: "notes", label: t("fields.notes.label"), type: "textarea" as const, required: false, placeholder: t("fields.notes.placeholder") },
  ];

  return (
    <Section>
      <div className="mx-auto max-w-2xl">
        <div className="text-center">
          <h1 className="text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
            {t("heading")}
          </h1>
          <p className="mt-4 text-lg text-gray-600">
            {t("description")}
          </p>
        </div>

        <div className="relative mt-8 aspect-[16/7] overflow-hidden rounded-2xl">
          <Image
            src="/images/generated/register-page.png"
            alt={t("imageAlt")}
            fill
            className="object-cover"
            sizes="(max-width: 768px) 100vw, 672px"
          />
        </div>

        <Card className="mt-10">
          <LeadForm
            fields={registerFields}
            submitLabel={t("submitLabel")}
            source="register"
          />
        </Card>

        <p className="mt-6 text-center text-sm text-gray-500">
          {t("disclaimer")}
        </p>
      </div>
    </Section>
  );
}
