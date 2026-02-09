"use client";

import { useTranslations } from "next-intl";
import { FileText, ClipboardCheck, Users, Award } from "lucide-react";

const trustIcons = [
  { key: "cipc" as const, icon: FileText },
  { key: "audit" as const, icon: ClipboardCheck },
  { key: "smes" as const, icon: Users },
  { key: "sa" as const, icon: Award },
];

export function HomePageTrustIndicators() {
  const t = useTranslations("HomePage");

  return (
    <div className="grid grid-cols-2 gap-8 md:grid-cols-4">
      {trustIcons.map((item) => (
        <div key={item.key} className="flex flex-col items-center text-center">
          <item.icon className="h-8 w-8 text-copper-600" />
          <span className="mt-3 text-sm font-medium text-gray-700">
            {t(`trust.${item.key}`)}
          </span>
        </div>
      ))}
    </div>
  );
}
