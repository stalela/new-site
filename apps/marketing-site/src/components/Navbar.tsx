"use client";

import Image from "next/image";
import { Link } from "@/i18n/navigation";
import { Container } from "./Container";
import { Button } from "./Button";
import { LanguageSwitcher } from "./LanguageSwitcher";
import {
  Menu,
  X,
  ChevronDown,
  Building2,
  ShieldCheck,
  Landmark,
  BadgeCheck,
  Globe,
  ArrowRight,
} from "lucide-react";
import { useState, useRef, useEffect } from "react";
import { useTranslations } from "next-intl";
import { serviceCategories } from "@/lib/services-data";

const navLinkKeys = [
  { key: "howItWorks" as const, href: "/how-it-works" },
  { key: "pricing" as const, href: "/pricing" },
  { key: "blog" as const, href: "/blog" },
  { key: "contact" as const, href: "/contact" },
];

/* Colored icon config per category — matches the screenshot style */
const categoryIcons: Record<
  string,
  { icon: typeof Building2; bg: string; text: string }
> = {
  "company-registration-and-changes": {
    icon: Building2,
    bg: "bg-copper-100",
    text: "text-copper-600",
  },
  "tax-and-compliance": {
    icon: ShieldCheck,
    bg: "bg-blue-100",
    text: "text-blue-600",
  },
  "banking-and-finance": {
    icon: Landmark,
    bg: "bg-amber-100",
    text: "text-amber-600",
  },
  "certifications-and-memberships": {
    icon: BadgeCheck,
    bg: "bg-rose-100",
    text: "text-rose-600",
  },
  "tech-and-digital-services": {
    icon: Globe,
    bg: "bg-violet-100",
    text: "text-violet-600",
  },
};

export function Navbar() {
  const t = useTranslations("Nav");
  const [mobileOpen, setMobileOpen] = useState(false);
  const [servicesOpen, setServicesOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const timeoutRef = useRef<NodeJS.Timeout>(null);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(e.target as Node)
      ) {
        setServicesOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  function handleMouseEnter() {
    if (timeoutRef.current) clearTimeout(timeoutRef.current);
    setServicesOpen(true);
  }

  function handleMouseLeave() {
    timeoutRef.current = setTimeout(() => setServicesOpen(false), 200);
  }

  return (
    <header className="sticky top-0 z-50 border-b border-gray-100 bg-white/95 backdrop-blur supports-[backdrop-filter]:bg-white/80">
      <Container>
        <nav className="flex h-16 items-center justify-between">
          <Link href="/" className="flex-shrink-0">
            <Image
              src="/images/logo.svg"
              alt="Stalela"
              width={240}
              height={66}
              priority
              className="h-16 w-auto"
            />
          </Link>

          {/* Desktop nav */}
          <div className="hidden items-center gap-8 md:flex">
            {/* Services mega-dropdown */}
            <div
              ref={dropdownRef}
              className="relative"
              onMouseEnter={handleMouseEnter}
              onMouseLeave={handleMouseLeave}
            >
              <Link
                href="/services"
                className="inline-flex items-center gap-1 text-sm font-medium text-gray-600 transition-colors hover:text-gray-900"
              >
                {t("services")}
                <ChevronDown
                  className={`h-3.5 w-3.5 transition-transform ${servicesOpen ? "rotate-180" : ""}`}
                />
              </Link>

              {servicesOpen && (
                <div className="absolute left-1/2 top-full z-50 mt-3 w-[640px] -translate-x-1/2 rounded-2xl border border-gray-200 bg-white shadow-xl">
                  <div className="grid grid-cols-5">
                    {/* Left column — categories */}
                    <div className="col-span-3 p-5">
                      <p className="mb-3 text-xs font-semibold uppercase tracking-wider text-gray-400">
                        {t("ourServices")}
                      </p>
                      <div className="space-y-1">
                        {serviceCategories.map((cat) => {
                          const cfg = categoryIcons[cat.slug];
                          const Icon = cfg?.icon ?? Building2;
                          return (
                            <Link
                              key={cat.slug}
                              href={`/services/${cat.slug}`}
                              className="group flex items-center gap-3.5 rounded-xl px-3 py-3 transition-colors hover:bg-gray-50"
                              onClick={() => setServicesOpen(false)}
                            >
                              <div
                                className={`flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-xl ${cfg?.bg ?? "bg-gray-100"}`}
                              >
                                <Icon
                                  className={`h-5 w-5 ${cfg?.text ?? "text-gray-600"}`}
                                />
                              </div>
                              <div>
                                <span className="text-sm font-semibold text-gray-900 group-hover:text-copper-700">
                                  {cat.name}
                                </span>
                                <span className="mt-0.5 block text-xs text-gray-500 line-clamp-1">
                                  {cat.services.length} service{cat.services.length !== 1 ? "s" : ""}
                                  {" · "}
                                  {cat.services
                                    .slice(0, 2)
                                    .map((s) => s.title)
                                    .join(", ")}
                                  {cat.services.length > 2 ? " …" : ""}
                                </span>
                              </div>
                            </Link>
                          );
                        })}
                      </div>
                    </div>

                    {/* Right column — featured highlight */}
                    <div className="col-span-2 flex flex-col rounded-r-2xl bg-gray-50 p-5">
                      <p className="mb-3 text-xs font-semibold uppercase tracking-wider text-gray-400">
                        {t("mostPopular")}
                      </p>
                      <Link
                        href="/services/company-registration-and-changes/company-registration"
                        className="group flex-1 rounded-xl bg-white p-4 shadow-sm ring-1 ring-gray-200 transition-all hover:shadow-md hover:ring-copper-200"
                        onClick={() => setServicesOpen(false)}
                      >
                        <div className="relative mb-3 aspect-[16/9] overflow-hidden rounded-lg">
                          <Image
                            src="/images/generated/category-registration.png"
                            alt="Company Registration"
                            fill
                            className="object-cover"
                            sizes="240px"
                          />
                        </div>
                        <h3 className="text-sm font-bold text-gray-900 group-hover:text-copper-700">
                          {t("featuredTitle")}
                        </h3>
                        <p className="mt-1.5 text-xs leading-relaxed text-gray-500">
                          {t("featuredDescription")}
                        </p>
                        <span className="mt-3 inline-flex items-center gap-1 text-xs font-semibold text-copper-600">
                          {t("learnMore")}{" "}
                          <ArrowRight className="h-3 w-3 transition-transform group-hover:translate-x-0.5" />
                        </span>
                      </Link>

                      <div className="mt-3 border-t border-gray-200 pt-3">
                        <Link
                          href="/services"
                          className="inline-flex items-center gap-1 text-sm font-medium text-copper-600 transition-colors hover:text-copper-700"
                          onClick={() => setServicesOpen(false)}
                        >
                          {t("viewAllServices")}
                          <ArrowRight className="h-3.5 w-3.5" />
                        </Link>
                      </div>
                    </div>
                  </div>
                </div>
              )}
            </div>

            {navLinkKeys.map((link) => (
              <Link
                key={link.href}
                href={link.href}
                className="text-sm font-medium text-gray-600 transition-colors hover:text-gray-900"
              >
                {t(link.key)}
              </Link>
            ))}
            <LanguageSwitcher />
            <Button href="/register" size="sm">
              {t("getStarted")}
            </Button>
          </div>

          {/* Mobile toggle */}
          <button
            className="md:hidden"
            onClick={() => setMobileOpen(!mobileOpen)}
            aria-label={mobileOpen ? t("closeMenu") : t("openMenu")}
          >
            {mobileOpen ? (
              <X className="h-6 w-6" />
            ) : (
              <Menu className="h-6 w-6" />
            )}
          </button>
        </nav>

        {/* Mobile nav */}
        {mobileOpen && (
          <div className="border-t border-gray-100 pb-4 md:hidden">
            <div className="border-b border-gray-100 pb-2">
              <Link
                href="/services"
                className="block px-2 py-3 text-sm font-medium text-gray-900"
                onClick={() => setMobileOpen(false)}
              >
                {t("services")}
              </Link>
              <div className="space-y-1 pb-2 pl-2">
                {serviceCategories.map((cat) => {
                  const cfg = categoryIcons[cat.slug];
                  const Icon = cfg?.icon ?? Building2;
                  return (
                    <Link
                      key={cat.slug}
                      href={`/services/${cat.slug}`}
                      className="flex items-center gap-3 rounded-lg px-2 py-2.5 text-sm text-gray-600 hover:bg-gray-50 hover:text-gray-900"
                      onClick={() => setMobileOpen(false)}
                    >
                      <div
                        className={`flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-lg ${cfg?.bg ?? "bg-gray-100"}`}
                      >
                        <Icon
                          className={`h-4 w-4 ${cfg?.text ?? "text-gray-600"}`}
                        />
                      </div>
                      {cat.name}
                    </Link>
                  );
                })}
              </div>
            </div>

            {navLinkKeys.map((link) => (
              <Link
                key={link.href}
                href={link.href}
                className="block px-2 py-3 text-sm font-medium text-gray-600 hover:text-gray-900"
                onClick={() => setMobileOpen(false)}
              >
                {t(link.key)}
              </Link>
            ))}
            <div className="mt-2 px-2">
              <LanguageSwitcher />
              <Button href="/register" size="sm" className="mt-2 w-full">
                {t("getStarted")}
              </Button>
            </div>
          </div>
        )}
      </Container>
    </header>
  );
}
