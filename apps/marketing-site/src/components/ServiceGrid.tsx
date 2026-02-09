"use client";

import { useState } from "react";
import { allServices, type ServiceItem } from "@/lib/services-data";
import { ServiceModal } from "./ServiceModal";

interface ServiceGridProps {
  services?: ServiceItem[];
}

export function ServiceGrid({ services }: ServiceGridProps) {
  const items = services ?? allServices;
  const [activeService, setActiveService] = useState<ServiceItem | null>(null);

  return (
    <>
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {items.map((service) => (
          <button
            key={service.title}
            type="button"
            onClick={() => setActiveService(service)}
            className="flex items-center gap-4 rounded-xl bg-copper-600 p-5 text-left text-white transition-colors hover:bg-copper-700 focus:outline-none focus:ring-2 focus:ring-copper-600 focus:ring-offset-2"
          >
            <div className="flex h-12 w-12 flex-shrink-0 items-center justify-center rounded-lg bg-white/20">
              <service.icon className="h-6 w-6" />
            </div>
            <span className="text-sm font-semibold leading-tight sm:text-base">
              {service.title}
            </span>
          </button>
        ))}
      </div>

      <ServiceModal
        service={activeService}
        onClose={() => setActiveService(null)}
      />
    </>
  );
}
