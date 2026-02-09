import Image from "next/image";
import { Button } from "./Button";
import { Container } from "./Container";

interface HeroProps {
  headline: string;
  subheadline: string;
  ctaText: string;
  ctaHref: string;
  secondaryCtaText?: string;
  secondaryCtaHref?: string;
}

export function Hero({
  headline,
  subheadline,
  ctaText,
  ctaHref,
  secondaryCtaText,
  secondaryCtaHref,
}: HeroProps) {
  return (
    <section className="relative min-h-[600px] overflow-hidden bg-gray-900 pt-20 sm:pt-32">
      {/* Background image */}
      <Image
        src="/images/generated/hero-banner.png"
        alt="South African business professionals collaborating"
        fill
        className="object-cover opacity-40"
        priority
        sizes="100vw"
      />
      {/* Gradient overlay */}
      <div className="absolute inset-0 bg-gradient-to-b from-gray-900/60 via-gray-900/40 to-gray-900/80" />

      <Container className="relative z-10 pb-16 sm:pb-24">
        <div className="mx-auto max-w-3xl text-center">
          <h1 className="text-4xl font-bold tracking-tight text-white sm:text-5xl lg:text-6xl">
            {headline}
          </h1>
          <p className="mt-6 text-lg leading-relaxed text-gray-200 sm:text-xl">
            {subheadline}
          </p>
          <div className="mt-10 flex flex-col items-center justify-center gap-4 sm:flex-row">
            <Button href={ctaHref} size="lg">
              {ctaText}
            </Button>
            {secondaryCtaText && secondaryCtaHref && (
              <Button href={secondaryCtaHref} variant="ghost" size="lg">
                {secondaryCtaText}
              </Button>
            )}
          </div>
        </div>
      </Container>
    </section>
  );
}
