"use client";

import { useState, useCallback, useEffect } from "react";
import Image from "next/image";
import { ChevronLeft, ChevronRight } from "lucide-react";

export interface CarouselSlide {
  src: string;
  alt: string;
  caption?: string;
}

interface ImageCarouselProps {
  slides: CarouselSlide[];
  /** Auto-advance interval in ms. 0 = disabled. Default 5000 */
  autoPlay?: number;
  /** Aspect ratio class. Default "aspect-[16/9]" */
  aspect?: string;
  className?: string;
}

export function ImageCarousel({
  slides,
  autoPlay = 5000,
  aspect = "aspect-[16/9]",
  className = "",
}: ImageCarouselProps) {
  const [current, setCurrent] = useState(0);
  const len = slides.length;

  const next = useCallback(() => setCurrent((i) => (i + 1) % len), [len]);
  const prev = useCallback(() => setCurrent((i) => (i - 1 + len) % len), [len]);

  useEffect(() => {
    if (!autoPlay || len <= 1) return;
    const id = setInterval(next, autoPlay);
    return () => clearInterval(id);
  }, [autoPlay, next, len]);

  if (len === 0) return null;

  return (
    <div className={`group relative overflow-hidden rounded-2xl ${className}`}>
      <div className={`relative w-full ${aspect}`}>
        {slides.map((slide, i) => (
          <div
            key={slide.src}
            className={`absolute inset-0 transition-opacity duration-500 ${
              i === current ? "opacity-100" : "pointer-events-none opacity-0"
            }`}
          >
            <Image
              src={slide.src}
              alt={slide.alt}
              fill
              className="object-cover"
              sizes="(max-width: 768px) 100vw, (max-width: 1200px) 50vw, 800px"
              priority={i === 0}
            />
          </div>
        ))}

        {/* Gradient overlay for caption */}
        {slides[current]?.caption && (
          <div className="absolute inset-x-0 bottom-0 bg-gradient-to-t from-black/60 to-transparent px-6 pb-5 pt-12">
            <p className="text-sm font-medium text-white sm:text-base">
              {slides[current].caption}
            </p>
          </div>
        )}
      </div>

      {/* Navigation arrows */}
      {len > 1 && (
        <>
          <button
            onClick={prev}
            aria-label="Previous slide"
            className="absolute left-3 top-1/2 -translate-y-1/2 rounded-full bg-white/80 p-2 shadow-lg backdrop-blur-sm transition-opacity hover:bg-white focus:outline-none focus:ring-2 focus:ring-copper-600 md:opacity-0 md:group-hover:opacity-100"
          >
            <ChevronLeft className="h-5 w-5 text-gray-800" />
          </button>
          <button
            onClick={next}
            aria-label="Next slide"
            className="absolute right-3 top-1/2 -translate-y-1/2 rounded-full bg-white/80 p-2 shadow-lg backdrop-blur-sm transition-opacity hover:bg-white focus:outline-none focus:ring-2 focus:ring-copper-600 md:opacity-0 md:group-hover:opacity-100"
          >
            <ChevronRight className="h-5 w-5 text-gray-800" />
          </button>
        </>
      )}

      {/* Dots */}
      {len > 1 && (
        <div className="absolute bottom-3 left-1/2 flex -translate-x-1/2 items-center gap-2">
          {slides.map((_, i) => (
            <button
              key={i}
              onClick={() => setCurrent(i)}
              aria-label={`Go to slide ${i + 1}`}
              className={`h-2 rounded-full transition-all ${
                i === current
                  ? "w-6 bg-white"
                  : "w-2 bg-white/50 hover:bg-white/80"
              }`}
            />
          ))}
        </div>
      )}
    </div>
  );
}
