import { defineRouting } from "next-intl/routing";

export const routing = defineRouting({
  locales: ["en", "zu", "af", "xh"],
  defaultLocale: "en",
});

export type Locale = (typeof routing.locales)[number];
