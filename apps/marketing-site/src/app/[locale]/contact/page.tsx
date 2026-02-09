import type { Metadata } from "next";
import Image from "next/image";
import { getTranslations, setRequestLocale } from "next-intl/server";
import { Section } from "@/components/Section";
import { Card } from "@/components/Card";
import { LeadForm } from "@/components/LeadForm";
import { Mail, Phone, MapPin } from "lucide-react";
import { buildPageMetadata } from "@/lib/seo";
import type { Locale } from "@/i18n/routing";

interface Props {
  params: Promise<{ locale: string }>;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: "Metadata" });
  return buildPageMetadata({
    title: t("contact.title"),
    description: t("contact.description"),
    path: "/contact",
    locale: locale as Locale,
  });
}

export default async function ContactPage({ params }: Props) {
  const { locale } = await params;
  setRequestLocale(locale);
  const t = await getTranslations("Contact");

  const contactFields = [
    { name: "name", label: t("fields.name.label"), type: "text" as const, required: true, placeholder: t("fields.name.placeholder") },
    { name: "email", label: t("fields.email.label"), type: "email" as const, required: true, placeholder: t("fields.email.placeholder") },
    { name: "phone", label: t("fields.phone.label"), type: "tel" as const, required: false, placeholder: t("fields.phone.placeholder") },
    {
      name: "service",
      label: t("fields.service.label"),
      type: "select" as const,
      required: true,
      options: t.raw("serviceOptions") as string[],
    },
    { name: "message", label: t("fields.message.label"), type: "textarea" as const, required: false, placeholder: t("fields.message.placeholder") },
  ];

  const contactInfo = [
    { icon: Mail, label: t("info.email.label"), value: t("info.email.value") },
    { icon: Phone, label: t("info.phone.label"), value: t("info.phone.value") },
    { icon: MapPin, label: t("info.location.label"), value: t("info.location.value") },
  ];

  return (
    <Section>
      <div className="mx-auto max-w-2xl text-center">
        <h1 className="text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
          {t("heading")}
        </h1>
        <p className="mt-4 text-lg text-gray-600">
          {t("description")}
        </p>
      </div>

      <div className="mt-12 grid gap-12 lg:grid-cols-5">
        <div className="lg:col-span-3">
          <Card>
            <LeadForm
              fields={contactFields}
              submitLabel={t("submitLabel")}
              source="contact"
            />
          </Card>
        </div>

        <div className="lg:col-span-2">
          <div className="relative mb-8 aspect-[16/10] overflow-hidden rounded-2xl">
            <Image
              src="/images/generated/contact-page.png"
              alt={t("heading")}
              fill
              className="object-cover"
              sizes="(max-width: 1024px) 100vw, 400px"
            />
          </div>
          <div className="space-y-6">
            {contactInfo.map((item) => (
              <div key={item.label} className="flex items-start gap-3">
                <div className="flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-lg bg-copper-100">
                  <item.icon className="h-5 w-5 text-copper-600" />
                </div>
                <div>
                  <p className="text-sm font-medium text-gray-900">{item.label}</p>
                  <p className="mt-0.5 text-sm text-gray-600">{item.value}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </Section>
  );
}
