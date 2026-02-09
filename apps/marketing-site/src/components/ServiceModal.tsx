"use client";

import { useEffect, useRef } from "react";
import { Link } from "@/i18n/navigation";
import { X, Check, ArrowRight } from "lucide-react";
import { Button } from "./Button";
import { getCategoryForService } from "@/lib/services-data";
import type { ServiceItem } from "@/lib/services-data";

interface ServiceModalProps {
  service: ServiceItem | null;
  onClose: () => void;
}

export function ServiceModal({ service, onClose }: ServiceModalProps) {
  const overlayRef = useRef<HTMLDivElement>(null);

  // Close on Escape key
  useEffect(() => {
    function handleKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    if (service) {
      document.addEventListener("keydown", handleKey);
      document.body.style.overflow = "hidden";
    }
    return () => {
      document.removeEventListener("keydown", handleKey);
      document.body.style.overflow = "";
    };
  }, [service, onClose]);

  if (!service) return null;

  const Icon = service.icon;
  const category = getCategoryForService(service.slug);

  return (
    <div
      ref={overlayRef}
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4 backdrop-blur-sm"
      onClick={(e) => {
        if (e.target === overlayRef.current) onClose();
      }}
      role="dialog"
      aria-modal="true"
      aria-label={service.title}
    >
      <div className="relative w-full max-w-lg rounded-2xl bg-white p-6 shadow-xl sm:p-8">
        {/* Close button */}
        <button
          onClick={onClose}
          className="absolute right-4 top-4 rounded-lg p-1 text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-600"
          aria-label="Close"
        >
          <X className="h-5 w-5" />
        </button>

        {/* Icon + Title */}
        <div className="flex items-center gap-3">
          <div className="flex h-12 w-12 flex-shrink-0 items-center justify-center rounded-lg bg-copper-100">
            <Icon className="h-6 w-6 text-copper-600" />
          </div>
          <h2 className="text-xl font-bold text-gray-900">{service.title}</h2>
        </div>

        {/* Description */}
        <p className="mt-4 leading-relaxed text-gray-600">
          {service.longDescription || service.description}
        </p>

        {/* Features */}
        {service.features && service.features.length > 0 && (
          <div className="mt-6">
            <h3 className="text-sm font-semibold text-gray-900">
              What&apos;s included
            </h3>
            <ul className="mt-3 space-y-2">
              {service.features.map((feature) => (
                <li
                  key={feature}
                  className="flex items-start gap-2 text-sm text-gray-600"
                >
                  <Check className="mt-0.5 h-4 w-4 flex-shrink-0 text-copper-600" />
                  {feature}
                </li>
              ))}
            </ul>
          </div>
        )}

        {/* CTA */}
        <div className="mt-8 flex gap-3">
          <Button href="/register" size="md">
            Get started
          </Button>
          <Button href="/contact" variant="outline" size="md">
            Contact us
          </Button>
        </div>
        {category && (
          <Link
            href={`/services/${category.slug}/${service.slug}`}
            className="mt-3 inline-flex items-center gap-1 text-sm text-copper-600 transition-colors hover:text-copper-700"
            onClick={onClose}
          >
            View full details <ArrowRight className="h-3.5 w-3.5" />
          </Link>
        )}
      </div>
    </div>
  );
}
