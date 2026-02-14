"use client";

import { useState, useCallback, useEffect, useRef } from "react";
import Image from "next/image";
import {
  ChevronLeft,
  ChevronRight,
  Building2,
  ShieldCheck,
  Landmark,
  BadgeCheck,
  Globe,
  type LucideIcon,
} from "lucide-react";
import { Container } from "./Container";
import { Button } from "./Button";
import { useTranslations } from "next-intl";

/* ── Static slide metadata (non-translatable) ── */

interface SlideMeta {
  key: string;
  slug: string;
  image: string;
  icon: LucideIcon;
  ctaHref: string;
}

const slideMeta: SlideMeta[] = [
  {
    key: "techDigital",
    slug: "tech-and-digital-services",
    image: "/images/generated/category-digital-services.png",
    icon: Globe,
    ctaHref: "/services/tech-and-digital-services",
  },
  {
    key: "companyRegistration",
    slug: "company-registration-and-changes",
    image: "/images/generated/category-registration.png",
    icon: Building2,
    ctaHref: "/register",
  },
  {
    key: "taxCompliance",
    slug: "tax-and-compliance",
    image: "/images/generated/category-tax-compliance.png",
    icon: ShieldCheck,
    ctaHref: "/services/tax-and-compliance",
  },
  {
    key: "banking",
    slug: "banking-and-finance",
    image: "/images/generated/category-banking.png",
    icon: Landmark,
    ctaHref: "/services/banking-and-finance",
  },
  {
    key: "certifications",
    slug: "certifications-and-memberships",
    image: "/images/generated/category-certifications.png",
    icon: BadgeCheck,
    ctaHref: "/services/certifications-and-memberships",
  },
];

/* ── Component ── */

export function HeroSlider() {
  const t = useTranslations("HeroSlider");
  const [current, setCurrent] = useState(0);
  const [isPaused, setIsPaused] = useState(false);
  const len = slideMeta.length;
  const intervalRef = useRef<NodeJS.Timeout | null>(null);

  const goTo = useCallback((i: number) => setCurrent(i), []);
  const next = useCallback(() => setCurrent((i) => (i + 1) % len), [len]);
  const prev = useCallback(() => setCurrent((i) => (i - 1 + len) % len), [len]);

  /* Auto-advance */
  useEffect(() => {
    if (isPaused) return;
    intervalRef.current = setInterval(next, 6000);
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [isPaused, next]);

  const meta = slideMeta[current];
  const slideTitle = t(`slides.${meta.key}.title`);
  const slideHeadline = t(`slides.${meta.key}.headline`);
  const slideDescription = t(`slides.${meta.key}.description`);
  const slideCtaText = t(`slides.${meta.key}.ctaText`);

  return (
    <section
      className="group relative min-h-[600px] overflow-hidden bg-gray-900 pt-20 sm:min-h-[640px] sm:pt-28 lg:min-h-[680px]"
      onMouseEnter={() => setIsPaused(true)}
      onMouseLeave={() => setIsPaused(false)}
    >
      {/* Background images — all rendered, opacity toggled */}
      {slideMeta.map((s, i) => (
        <div
          key={s.slug}
          className={`absolute inset-0 transition-opacity duration-700 ease-in-out ${
            i === current ? "opacity-100" : "pointer-events-none opacity-0"
          }`}
        >
          <Image
            src={s.image}
            alt={`Stalela ${s.key} services`}
            fill
            className="object-cover"
            priority={i === 0}
            sizes="100vw"
          />
        </div>
      ))}

      {/* Dark overlay */}
      <div className="absolute inset-0 bg-gradient-to-b from-gray-900/70 via-gray-900/50 to-gray-900/80" />

      {/* Content */}
      <Container className="relative z-10 flex min-h-[480px] flex-col justify-center pb-20 sm:min-h-[500px] sm:pb-24">
        {/* Category badge */}
        <div className="flex items-center gap-2">
          <meta.icon className="h-4 w-4 text-copper-400" />
          <span className="text-xs font-semibold uppercase tracking-widest text-copper-400">
            {slideTitle}
          </span>
        </div>

        {/* Headline */}
        <h1
          className="mt-4 max-w-2xl text-4xl font-bold leading-tight tracking-tight text-white transition-opacity duration-500 sm:text-5xl lg:text-6xl"
          style={{ whiteSpace: "pre-line" }}
        >
          {slideHeadline}
        </h1>

        {/* Description */}
        <p className="mt-5 max-w-xl text-base leading-relaxed text-gray-200 sm:text-lg">
          {slideDescription}
        </p>

        {/* CTAs */}
        <div className="mt-8 flex flex-col gap-3 sm:flex-row sm:gap-4">
          <Button href="/contact" size="lg">
            {t("getInTouch")}
          </Button>
          <Button href={meta.ctaHref} variant="ghost" size="lg">
            {slideCtaText}
          </Button>
        </div>

        {/* Bottom bar: dots + arrows */}
        <div className="mt-12 flex items-center gap-6">
          {/* Dots / category pills */}
          <div className="flex items-center gap-2">
            {slideMeta.map((s, i) => {
              const pillTitle = t(`slides.${s.key}.title`);
              return (
              <button
                key={s.slug}
                onClick={() => goTo(i)}
                aria-label={pillTitle}
                className={`group/dot flex items-center gap-1.5 rounded-full border px-3 py-1.5 text-xs font-medium transition-all ${
                  i === current
                    ? "border-copper-500/60 bg-copper-600/20 text-white backdrop-blur-sm"
                    : "border-white/20 bg-white/5 text-white/60 backdrop-blur-sm hover:border-white/40 hover:text-white/90"
                }`}
              >
                <s.icon className={`h-3 w-3 ${i === current ? "text-copper-400" : "text-white/40 group-hover/dot:text-white/70"}`} />
                <span className="hidden sm:inline">{pillTitle}</span>
              </button>
              );
            })}
          </div>

          {/* Arrows */}
          <div className="ml-auto flex items-center gap-2">
            <button
              onClick={prev}
              aria-label={t("previousSlide")}
              className="flex h-9 w-9 items-center justify-center rounded-full border border-white/20 bg-white/5 text-white/70 backdrop-blur-sm transition-colors hover:border-white/40 hover:bg-white/10 hover:text-white"
            >
              <ChevronLeft className="h-4 w-4" />
            </button>
            <button
              onClick={next}
              aria-label={t("nextSlide")}
              className="flex h-9 w-9 items-center justify-center rounded-full border border-white/20 bg-white/5 text-white/70 backdrop-blur-sm transition-colors hover:border-white/40 hover:bg-white/10 hover:text-white"
            >
              <ChevronRight className="h-4 w-4" />
            </button>
          </div>
        </div>

        {/* Progress bar */}
        <div className="mt-4 flex gap-1.5">
          {slideMeta.map((s, i) => (
            <div key={s.slug} className="h-0.5 flex-1 overflow-hidden rounded-full bg-white/15">
              <div
                className={`h-full rounded-full transition-all ${
                  i === current
                    ? "bg-copper-500 duration-[6000ms] ease-linear"
                    : i < current
                      ? "bg-white/30"
                      : "bg-transparent"
                }`}
                style={{
                  width: i === current ? "100%" : i < current ? "100%" : "0%",
                  transitionProperty: i === current ? "width" : "none",
                }}
              />
            </div>
          ))}
        </div>
      </Container>
    </section>
  );
}
