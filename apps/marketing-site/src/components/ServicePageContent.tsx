"use client";

import { Link } from "@/i18n/navigation";
import { Check } from "lucide-react";
import { Button } from "./Button";
import { getServiceBySlug, getCategoryForService } from "@/lib/services-data";

interface ServicePageContentProps {
  serviceSlug: string;
}

export function ServicePageContent({ serviceSlug }: ServicePageContentProps) {
  const service = getServiceBySlug(serviceSlug);
  const category = getCategoryForService(serviceSlug);

  if (!service || !category) return null;

  const Icon = service.icon;

  return (
    <div>
      {/* Breadcrumbs */}
      <nav className="flex items-center gap-2 text-sm text-gray-500">
        <Link
          href="/services"
          className="transition-colors hover:text-gray-700"
        >
          Services
        </Link>
        <span>/</span>
        <Link
          href={`/services/${category.slug}`}
          className="transition-colors hover:text-gray-700"
        >
          {category.name}
        </Link>
        <span>/</span>
        <span className="font-medium text-gray-900">{service.title}</span>
      </nav>

      {/* Header */}
      <div className="mt-8 flex items-start gap-4">
        <div className="flex h-14 w-14 flex-shrink-0 items-center justify-center rounded-xl bg-copper-100">
          <Icon className="h-7 w-7 text-copper-600" />
        </div>
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
            {service.title}
          </h1>
          <p className="mt-2 text-lg text-gray-600">{service.description}</p>
        </div>
      </div>

      {/* Long description */}
      <div className="mt-8 max-w-3xl">
        <p className="text-lg leading-relaxed text-gray-700">
          {service.longDescription}
        </p>
      </div>

      {/* Features */}
      {service.features.length > 0 && (
        <div className="mt-10">
          <h2 className="text-xl font-bold text-gray-900">
            What&apos;s included
          </h2>
          <ul className="mt-4 grid gap-3 sm:grid-cols-2">
            {service.features.map((feature) => (
              <li
                key={feature}
                className="flex items-start gap-3 text-gray-700"
              >
                <Check className="mt-0.5 h-5 w-5 flex-shrink-0 text-copper-600" />
                {feature}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* CTA */}
      <div className="mt-12 flex flex-wrap gap-4">
        <Button href="/register" size="lg">
          Get started
        </Button>
        <Button href="/contact" variant="outline" size="lg">
          Contact us
        </Button>
      </div>

      {/* Other services in category */}
      {category.services.length > 1 && (
        <div className="mt-16 border-t border-gray-200 pt-10">
          <h2 className="text-lg font-bold text-gray-900">
            Other {category.name.toLowerCase()} services
          </h2>
          <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {category.services
              .filter((s) => s.slug !== service.slug)
              .map((s) => {
                const SIcon = s.icon;
                return (
                  <Link
                    key={s.slug}
                    href={`/services/${category.slug}/${s.slug}`}
                    className="flex items-center gap-3 rounded-xl border border-gray-200 p-4 transition-colors hover:border-copper-200 hover:bg-copper-50"
                  >
                    <SIcon className="h-5 w-5 text-copper-600" />
                    <span className="text-sm font-medium text-gray-900">
                      {s.title}
                    </span>
                  </Link>
                );
              })}
          </div>
        </div>
      )}
    </div>
  );
}
