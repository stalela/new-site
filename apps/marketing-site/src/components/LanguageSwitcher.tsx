"use client";

import { useLocale, useTranslations } from "next-intl";
import { usePathname, useRouter } from "@/i18n/navigation";
import { routing, type Locale } from "@/i18n/routing";
import { useTransition } from "react";

function SAFlag({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 900 600"
      className={className}
      aria-hidden="true"
      role="img"
    >
      {/* Red top */}
      <rect width="900" height="600" fill="#DE3831" />
      {/* Blue bottom */}
      <rect y="300" width="900" height="300" fill="#002395" />
      {/* White border stripes */}
      <polygon points="0,160 0,200 500,300 0,400 0,440 600,300" fill="#FFF" />
      {/* Green Y-band */}
      <polygon points="0,200 0,400 500,300" fill="#007A4D" />
      <polygon points="500,300 900,200 900,240 600,300 900,360 900,400" fill="#007A4D" />
      {/* White thin bands along Y */}
      <rect y="160" width="900" height="40" fill="#FFF" opacity="0" />
      {/* Black triangle */}
      <polygon points="0,0 0,200 300,300 0,400 0,600 300,600 500,300 300,0" fill="transparent" />
      <polygon points="0,160 300,300 0,440" fill="#000" />
      {/* Gold borders around black */}
      <polygon points="0,120 0,160 300,300 0,440 0,480 350,300" fill="#FFB612" />
      <polygon points="0,160 0,440 300,300" fill="#000" />
    </svg>
  );
}

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
      <SAFlag className="h-4 w-5 flex-shrink-0 rounded-[2px]" />
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
