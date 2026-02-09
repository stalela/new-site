"use client";

import { useLocale, useTranslations } from "next-intl";
import { usePathname, useRouter } from "@/i18n/navigation";
import { routing, type Locale } from "@/i18n/routing";
import { useTransition } from "react";
import Image from "next/image";

export function LanguageSwitcher() {
  const t = useTranslations("LanguageSwitcher");
  const locale = useLocale();
  const router = useRouter();
  const pathname = usePathname();
  const [isPending, startTransition] = useTransition();

  function handleChange(nextLocale: string) {
    startTransition(() => {
      router.replace(pathname, { locale: nextLocale as Locale });
    });
  }

  return (
    <div className="relative flex items-center gap-1.5">
      <Image
        src="/images/sa-flag-small.png"
        alt="South African flag"
        width={20}
        height={14}
        className="flex-shrink-0 rounded-[2px]"
      />
      <label htmlFor="locale-select" className="sr-only">
        {t("label")}
      </label>
      <select
        id="locale-select"
        value={locale}
        onChange={(e) => handleChange(e.target.value)}
        disabled={isPending}
        className="cursor-pointer appearance-none rounded-md border border-gray-200 bg-white py-1 pl-2 pr-6 text-xs font-medium text-gray-700 transition-colors hover:border-copper-300 focus:border-copper-500 focus:outline-none focus:ring-1 focus:ring-copper-500 disabled:opacity-50"
      >
        {routing.locales.map((loc) => (
          <option key={loc} value={loc}>
            {t(loc)}
          </option>
        ))}
      </select>
    </div>
  );
}
