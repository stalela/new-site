"use client";

import { useEffect, useState, useCallback } from "react";
import { usePathname, useSearchParams } from "next/navigation";

/**
 * Thin animated progress bar at the top of the viewport.
 * Shows immediately on route change to give instant visual feedback.
 */
export function NavigationProgress() {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [state, setState] = useState<"idle" | "loading" | "completing">("idle");
  const [progress, setProgress] = useState(0);

  const start = useCallback(() => {
    setState("loading");
    setProgress(30);
  }, []);

  // Crawl progress while loading
  useEffect(() => {
    if (state !== "loading") return;
    const interval = setInterval(() => {
      setProgress((prev) => {
        if (prev >= 80) {
          clearInterval(interval);
          return 80;
        }
        return prev + Math.random() * 6;
      });
    }, 400);
    return () => clearInterval(interval);
  }, [state]);

  // When pathname changes while loading, complete the bar
  useEffect(() => {
    if (state === "loading") {
      const raf = requestAnimationFrame(() => {
        setState("completing");
        setProgress(100);
      });
      const timeout = setTimeout(() => {
        setState("idle");
        setProgress(0);
      }, 500);
      return () => {
        cancelAnimationFrame(raf);
        clearTimeout(timeout);
      };
    }
    // For "idle" and "completing" states, no action needed on pathname change
  }, [pathname, searchParams]); // eslint-disable-line react-hooks/exhaustive-deps

  // Intercept click on internal links to trigger the bar immediately
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      const anchor = (e.target as HTMLElement).closest("a");
      if (!anchor) return;

      const href = anchor.getAttribute("href");
      if (!href) return;

      if (
        href.startsWith("http") ||
        href.startsWith("#") ||
        anchor.hasAttribute("download") ||
        anchor.target === "_blank"
      )
        return;

      if (href === pathname) return;

      start();
    };

    document.addEventListener("click", handleClick);
    return () => document.removeEventListener("click", handleClick);
  }, [pathname, start]);

  if (state === "idle") return null;

  return (
    <div
      className="fixed top-0 left-0 right-0 z-[9999] h-0.5 pointer-events-none"
      role="progressbar"
      aria-valuenow={Math.round(progress)}
    >
      <div
        className="h-full bg-gradient-to-r from-copper-600 via-copper-light to-copper-600 shadow-[0_0_10px_var(--copper-600),0_0_5px_var(--copper-light)] transition-all duration-300 ease-out"
        style={{
          width: `${progress}%`,
          opacity: state === "completing" ? 0 : 1,
          transition:
            state === "completing"
              ? "width 200ms ease-out, opacity 300ms ease-out 100ms"
              : "width 300ms ease-out",
        }}
      />
    </div>
  );
}
