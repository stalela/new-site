"use client";

import { Link } from "@/i18n/navigation";
import Image from "next/image";
import {
  ArrowRight,
  Check,
  type LucideIcon,
} from "lucide-react";
import { Button } from "./Button";
import { ImageCarousel, type CarouselSlide } from "./ImageCarousel";
import { getCategoryForService } from "@/lib/services-data";

/* ── Shared types ── */

export interface StudioProcessStep {
  step: string;
  icon: LucideIcon;
  title: string;
  description: string;
  deliverables: string[];
}

export interface StudioFeature {
  icon: LucideIcon;
  title: string;
  description: string;
}

export interface StudioPackage {
  name: string;
  price: string;
  description: string;
  includes: string[];
  highlighted?: boolean;
}

export interface StudioFAQ {
  q: string;
  a: string;
}

export interface StudioPageProps {
  serviceSlug: string;
  badge: string;
  badgeIcon: LucideIcon;
  headline: string;
  headlineAccent: string;
  description: string;
  heroImage: string;
  heroImageAlt: string;
  /** Carousel slides shown in the gallery section */
  gallery?: CarouselSlide[];
  /** Gallery section heading */
  galleryTitle?: string;
  galleryDescription?: string;
  features: StudioFeature[];
  process: StudioProcessStep[];
  packages?: StudioPackage[];
  techStack?: string[];
  faqs: StudioFAQ[];
  ctaHeadline: string;
  ctaDescription: string;
  ctaIcon: LucideIcon;
}

export function StudioPage({
  serviceSlug,
  badge,
  badgeIcon: BadgeIcon,
  headline,
  headlineAccent,
  description,
  heroImage,
  heroImageAlt,
  gallery,
  galleryTitle,
  galleryDescription,
  features,
  process: steps,
  packages,
  techStack,
  faqs,
  ctaHeadline,
  ctaDescription,
  ctaIcon: CtaIcon,
}: StudioPageProps) {
  const category = getCategoryForService(serviceSlug);

  return (
    <div>
      {/* Breadcrumbs */}
      <nav className="flex items-center gap-2 text-sm text-gray-500">
        <Link href="/services" className="transition-colors hover:text-gray-700">
          Services
        </Link>
        <span>/</span>
        <Link
          href={`/services/${category?.slug}`}
          className="transition-colors hover:text-gray-700"
        >
          {category?.name}
        </Link>
        <span>/</span>
        <span className="font-medium text-gray-900">{headline.replace(headlineAccent, "").trim() || badge}</span>
      </nav>

      {/* ─── Hero ─── */}
      <div className="mt-10 grid items-center gap-10 lg:grid-cols-2">
        <div>
          <div className="inline-flex items-center gap-2 rounded-full bg-copper-100 px-3 py-1 text-xs font-semibold text-copper-700">
            <BadgeIcon className="h-3.5 w-3.5" />
            {badge}
          </div>
          <h1 className="mt-4 text-4xl font-bold tracking-tight text-gray-900 sm:text-5xl">
            {headline.replace(headlineAccent, "").trim()}{" "}
            <span className="text-copper-600">{headlineAccent}</span>
          </h1>
          <p className="mt-4 text-lg leading-relaxed text-gray-600">
            {description}
          </p>
          <div className="mt-8 flex flex-wrap gap-4">
            <Button href="/contact" size="lg">
              Get a free quote
            </Button>
            {packages && packages.length > 0 && (
              <Button href="#packages" variant="outline" size="lg">
                View packages
              </Button>
            )}
          </div>
        </div>
        <div className="relative aspect-[4/3] overflow-hidden rounded-2xl bg-gray-100">
          <Image
            src={heroImage}
            alt={heroImageAlt}
            fill
            className="object-cover"
            sizes="(max-width: 1024px) 100vw, 50vw"
            priority
          />
        </div>
      </div>

      {/* ─── Gallery / Carousel ─── */}
      {gallery && gallery.length > 0 && (
        <div className="mt-24">
          {galleryTitle && (
            <div className="mx-auto max-w-2xl text-center">
              <h2 className="text-3xl font-bold tracking-tight text-gray-900">
                {galleryTitle}
              </h2>
              {galleryDescription && (
                <p className="mt-3 text-lg text-gray-600">{galleryDescription}</p>
              )}
            </div>
          )}
          <div className="mx-auto mt-8 max-w-4xl">
            <ImageCarousel slides={gallery} autoPlay={5000} />
          </div>
        </div>
      )}

      {/* ─── What you get ─── */}
      <div className="mt-24">
        <div className="mx-auto max-w-2xl text-center">
          <h2 className="text-3xl font-bold tracking-tight text-gray-900">
            What you get
          </h2>
          <p className="mt-3 text-lg text-gray-600">
            Every project includes the essentials, baked in.
          </p>
        </div>
        <div className="mt-12 grid gap-8 sm:grid-cols-2 lg:grid-cols-3">
          {features.map((feature) => (
            <div key={feature.title} className="group">
              <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-copper-100 transition-colors group-hover:bg-copper-200">
                <feature.icon className="h-5 w-5 text-copper-600" />
              </div>
              <h3 className="mt-4 font-semibold text-gray-900">{feature.title}</h3>
              <p className="mt-2 text-sm leading-relaxed text-gray-600">
                {feature.description}
              </p>
            </div>
          ))}
        </div>
      </div>

      {/* ─── Our process ─── */}
      <div className="mt-24">
        <div className="mx-auto max-w-2xl text-center">
          <h2 className="text-3xl font-bold tracking-tight text-gray-900">
            Our process
          </h2>
          <p className="mt-3 text-lg text-gray-600">
            From first conversation to delivery — here&apos;s how we work.
          </p>
        </div>
        <div className="mt-12 space-y-12">
          {steps.map((item, i) => (
            <div
              key={item.step}
              className={`grid items-start gap-8 lg:grid-cols-2 ${i % 2 !== 0 ? "lg:direction-rtl" : ""}`}
            >
              <div className={i % 2 !== 0 ? "lg:order-2" : ""}>
                <div className="flex items-center gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-copper-600 text-white">
                    <item.icon className="h-5 w-5" />
                  </div>
                  <span className="text-sm font-bold text-copper-600">Step {item.step}</span>
                </div>
                <h3 className="mt-4 text-2xl font-bold text-gray-900">{item.title}</h3>
                <p className="mt-3 leading-relaxed text-gray-600">{item.description}</p>
              </div>
              <div className={`rounded-xl border border-gray-200 bg-gray-50 p-6 ${i % 2 !== 0 ? "lg:order-1" : ""}`}>
                <p className="mb-3 text-xs font-semibold uppercase tracking-wider text-gray-400">
                  Deliverables
                </p>
                <ul className="space-y-2.5">
                  {item.deliverables.map((d) => (
                    <li key={d} className="flex items-center gap-2.5 text-sm text-gray-700">
                      <Check className="h-4 w-4 flex-shrink-0 text-copper-600" />
                      {d}
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* ─── Tech stack ─── */}
      {techStack && techStack.length > 0 && (
        <div className="mt-24 rounded-2xl bg-gray-900 px-6 py-12 sm:px-12">
          <div className="mx-auto max-w-2xl text-center">
            <h2 className="text-2xl font-bold text-white">Built with modern technology</h2>
            <p className="mt-2 text-gray-400">
              Industry-leading tools for reliability, performance, and security.
            </p>
          </div>
          <div className="mx-auto mt-8 flex max-w-2xl flex-wrap items-center justify-center gap-3">
            {techStack.map((tech) => (
              <span
                key={tech}
                className="rounded-full border border-gray-700 bg-gray-800 px-4 py-2 text-sm font-medium text-gray-300"
              >
                {tech}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* ─── Packages ─── */}
      {packages && packages.length > 0 && (
        <div id="packages" className="mt-24 scroll-mt-24">
          <div className="mx-auto max-w-2xl text-center">
            <h2 className="text-3xl font-bold tracking-tight text-gray-900">
              Packages
            </h2>
            <p className="mt-3 text-lg text-gray-600">
              Straightforward pricing. No hidden fees.
            </p>
          </div>
          <div className={`mt-12 grid gap-8 ${packages.length === 3 ? "lg:grid-cols-3" : packages.length === 2 ? "lg:grid-cols-2 max-w-3xl mx-auto" : "max-w-md mx-auto"}`}>
            {packages.map((pkg) => (
              <div
                key={pkg.name}
                className={`relative flex flex-col rounded-2xl border p-8 ${
                  pkg.highlighted ? "border-copper-600 ring-1 ring-copper-600" : "border-gray-200"
                }`}
              >
                {pkg.highlighted && (
                  <span className="absolute -top-3 left-6 rounded-full bg-copper-600 px-3 py-0.5 text-xs font-semibold text-white">
                    Most popular
                  </span>
                )}
                <h3 className="text-lg font-bold text-gray-900">{pkg.name}</h3>
                <p className="mt-1 text-2xl font-bold text-copper-600">{pkg.price}</p>
                <p className="mt-3 text-sm text-gray-600">{pkg.description}</p>
                <ul className="mt-6 flex-1 space-y-3">
                  {pkg.includes.map((item) => (
                    <li key={item} className="flex items-start gap-2.5 text-sm text-gray-700">
                      <Check className="mt-0.5 h-4 w-4 flex-shrink-0 text-copper-600" />
                      {item}
                    </li>
                  ))}
                </ul>
                <div className="mt-8">
                  <Button
                    href="/contact"
                    variant={pkg.highlighted ? "primary" : "outline"}
                    className="w-full"
                  >
                    Get started
                  </Button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ─── FAQ ─── */}
      {faqs.length > 0 && (
        <div className="mt-24">
          <div className="mx-auto max-w-2xl text-center">
            <h2 className="text-3xl font-bold tracking-tight text-gray-900">
              Frequently asked questions
            </h2>
          </div>
          <div className="mx-auto mt-12 max-w-3xl divide-y divide-gray-200">
            {faqs.map((faq) => (
              <details key={faq.q} className="group py-5">
                <summary className="flex cursor-pointer items-center justify-between text-left font-semibold text-gray-900 [&::-webkit-details-marker]:hidden">
                  {faq.q}
                  <span className="ml-4 flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-gray-100 text-gray-500 transition-transform group-open:rotate-45">
                    +
                  </span>
                </summary>
                <p className="mt-3 leading-relaxed text-gray-600">{faq.a}</p>
              </details>
            ))}
          </div>
        </div>
      )}

      {/* ─── Final CTA ─── */}
      <div className="mt-24 rounded-2xl bg-copper-50 p-8 text-center sm:p-12">
        <CtaIcon className="mx-auto h-10 w-10 text-copper-600" />
        <h2 className="mt-4 text-2xl font-bold text-gray-900 sm:text-3xl">
          {ctaHeadline}
        </h2>
        <p className="mx-auto mt-3 max-w-lg text-gray-600">{ctaDescription}</p>
        <div className="mt-8 flex flex-wrap items-center justify-center gap-4">
          <Button href="/contact" size="lg">
            Get a free quote
          </Button>
          <Button href="/register" variant="outline" size="lg">
            Register your business first
          </Button>
        </div>
      </div>

      {/* ─── Other digital services ─── */}
      {category && category.services.length > 1 && (
        <div className="mt-16 border-t border-gray-200 pt-10">
          <h2 className="text-lg font-bold text-gray-900">
            Other {category.name.toLowerCase()} services
          </h2>
          <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {category.services
              .filter((s) => s.slug !== serviceSlug)
              .slice(0, 6)
              .map((s) => {
                const SIcon = s.icon;
                return (
                  <Link
                    key={s.slug}
                    href={`/services/${category.slug}/${s.slug}`}
                    className="flex items-center gap-3 rounded-xl border border-gray-200 p-4 transition-colors hover:border-copper-200 hover:bg-copper-50"
                  >
                    <SIcon className="h-5 w-5 text-copper-600" />
                    <span className="text-sm font-medium text-gray-900">{s.title}</span>
                    <ArrowRight className="ml-auto h-4 w-4 text-gray-400" />
                  </Link>
                );
              })}
          </div>
        </div>
      )}
    </div>
  );
}
