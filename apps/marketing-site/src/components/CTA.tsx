import Image from "next/image";
import { Container } from "./Container";
import { Button } from "./Button";

interface CTAProps {
  headline: string;
  description?: string;
  ctaText: string;
  ctaHref: string;
}

export function CTA({ headline, description, ctaText, ctaHref }: CTAProps) {
  return (
    <section className="relative overflow-hidden py-16 sm:py-24">
      {/* Background image */}
      <Image
        src="/images/generated/cta-background.png"
        alt="Warm textured background"
        fill
        className="object-cover"
        sizes="100vw"
      />
      {/* Warm overlay to keep it on-brand */}
      <div className="absolute inset-0 bg-copper-50/85" />

      <Container className="relative z-10">
        <div className="mx-auto max-w-2xl text-center">
          <h2 className="text-3xl font-bold tracking-tight text-gray-900 sm:text-4xl">
            {headline}
          </h2>
          {description && (
            <p className="mt-4 text-lg text-gray-600">{description}</p>
          )}
          <div className="mt-8">
            <Button href={ctaHref} size="lg">
              {ctaText}
            </Button>
          </div>
        </div>
      </Container>
    </section>
  );
}
