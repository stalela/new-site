/**
 * Renders a <script type="application/ld+json"> tag for structured data.
 * Server component — no "use client" needed.
 */
export function JsonLd({ data }: { data: Record<string, unknown> }) {
  return (
    <script
      type="application/ld+json"
      dangerouslySetInnerHTML={{ __html: JSON.stringify(data) }}
    />
  );
}
