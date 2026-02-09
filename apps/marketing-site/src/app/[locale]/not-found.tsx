import { useTranslations } from "next-intl";
import { Section } from "@/components/Section";
import { Button } from "@/components/Button";

export default function NotFound() {
  const t = useTranslations("NotFound");

  return (
    <Section>
      <div className="mx-auto max-w-md text-center">
        <p className="text-6xl font-bold text-copper-600">404</p>
        <h1 className="mt-4 text-2xl font-bold tracking-tight text-gray-900 sm:text-3xl">
          {t("heading")}
        </h1>
        <p className="mt-4 text-gray-600">
          {t("description")}
        </p>
        <div className="mt-8 flex flex-col items-center justify-center gap-4 sm:flex-row">
          <Button href="/">{t("homeLink")}</Button>
          <Button href="/services" variant="outline">
            {t("servicesLink")}
          </Button>
        </div>
      </div>
    </Section>
  );
}
