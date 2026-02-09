"use client";

import { Link } from "@/i18n/navigation";
import Image from "next/image";
import { ServiceGrid } from "@/components/ServiceGrid";
import { allServices, serviceCategories } from "@/lib/services-data";
import { ArrowRight } from "lucide-react";

export function ServicesContent() {
  return (
    <>
      {/* Overview illustration */}
      <div className="relative mx-auto mt-10 aspect-[16/7] max-w-4xl overflow-hidden rounded-2xl">
        <Image
          src="/images/generated/services-overview.png"
          alt="Overview of Stalela business services ecosystem"
          fill
          className="object-cover"
          sizes="(max-width: 768px) 100vw, 896px"
        />
      </div>

      {/* Category cards */}
      <div className="mt-12 grid gap-6 sm:grid-cols-2 lg:grid-cols-3">
        {serviceCategories.map((category) => {
          const FirstIcon = category.services[0]?.icon;
          return (
            <Link
              key={category.slug}
              href={`/services/${category.slug}`}
              className="group flex flex-col rounded-2xl border border-gray-200 bg-white p-6 shadow-sm transition-all hover:border-copper-200 hover:shadow-md"
            >
              {FirstIcon && (
                <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-copper-100">
                  <FirstIcon className="h-5 w-5 text-copper-600" />
                </div>
              )}
              <h3 className="mt-4 text-lg font-bold text-gray-900">
                {category.name}
              </h3>
              <p className="mt-2 flex-1 text-sm text-gray-600">
                {category.description}
              </p>
              <div className="mt-4 flex items-center gap-1 text-sm font-medium text-copper-600">
                {category.services.length} service{category.services.length !== 1 ? "s" : ""}
                <ArrowRight className="h-4 w-4 transition-transform group-hover:translate-x-1" />
              </div>
            </Link>
          );
        })}
      </div>

      {/* Browse all services */}
      <div className="mt-16">
        <h2 className="text-2xl font-bold tracking-tight text-gray-900 sm:text-3xl">
          Browse all services
        </h2>
        <p className="mt-2 text-gray-600">
          Click any service for a quick overview.
        </p>
        <div className="mt-6">
          <ServiceGrid services={allServices} />
        </div>
      </div>
    </>
  );
}
