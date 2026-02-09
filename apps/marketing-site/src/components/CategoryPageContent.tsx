"use client";

import { Link } from "@/i18n/navigation";
import Image from "next/image";
import { ArrowLeft, Check } from "lucide-react";
import { getCategoryBySlug } from "@/lib/services-data";

/** Map category slugs to their generated header images */
const categoryImages: Record<string, string> = {
  "company-registration-and-changes": "/images/generated/category-registration.png",
  "tax-and-compliance": "/images/generated/category-tax-compliance.png",
  "banking-and-finance": "/images/generated/category-banking.png",
  "certifications-and-memberships": "/images/generated/category-certifications.png",
  "tech-and-digital-services": "/images/generated/category-digital-services.png",
};

interface CategoryPageContentProps {
  categorySlug: string;
}

export function CategoryPageContent({ categorySlug }: CategoryPageContentProps) {
  const category = getCategoryBySlug(categorySlug);

  if (!category) return null;

  const headerImage = categoryImages[categorySlug];

  return (
    <div>
      <Link
        href="/services"
        className="inline-flex items-center gap-1 text-sm text-gray-500 transition-colors hover:text-gray-700"
      >
        <ArrowLeft className="h-4 w-4" />
        All services
      </Link>

      <h1 className="mt-4 text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
        {category.name}
      </h1>
      <p className="mt-4 text-lg text-gray-600">{category.description}</p>

      {headerImage && (
        <div className="relative mt-8 aspect-[16/7] overflow-hidden rounded-2xl">
          <Image
            src={headerImage}
            alt={category.name}
            fill
            className="object-cover"
            sizes="(max-width: 768px) 100vw, 896px"
          />
        </div>
      )}

      <div className="mt-12 space-y-6">
        {category.services.map((service) => {
          const Icon = service.icon;
          return (
            <Link
              key={service.slug}
              href={`/services/${categorySlug}/${service.slug}`}
              className="block rounded-2xl border border-gray-200 bg-white p-6 shadow-sm transition-all hover:border-copper-200 hover:shadow-md sm:p-8"
            >
              <div className="flex items-center gap-4">
                <div className="flex h-12 w-12 flex-shrink-0 items-center justify-center rounded-lg bg-copper-100">
                  <Icon className="h-6 w-6 text-copper-600" />
                </div>
                <div>
                  <h2 className="text-xl font-bold text-gray-900">
                    {service.title}
                  </h2>
                  <p className="mt-1 text-gray-600">{service.description}</p>
                </div>
              </div>

              {service.features.length > 0 && (
                <ul className="mt-4 grid gap-2 sm:grid-cols-2">
                  {service.features.slice(0, 4).map((feature) => (
                    <li
                      key={feature}
                      className="flex items-start gap-2 text-sm text-gray-600"
                    >
                      <Check className="mt-0.5 h-4 w-4 flex-shrink-0 text-copper-600" />
                      {feature}
                    </li>
                  ))}
                </ul>
              )}
            </Link>
          );
        })}
      </div>
    </div>
  );
}
