export default function DashboardLoading() {
  return (
    <div className="space-y-6 animate-pulse">
      {/* Header skeleton */}
      <div className="flex items-center gap-3">
        <div className="h-7 w-48 rounded-lg bg-surface-elevated" />
        <div className="h-5 w-20 rounded-full bg-surface-elevated" />
      </div>

      {/* Stat cards skeleton */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <div
            key={i}
            className="rounded-xl border border-border bg-surface p-6"
          >
            <div className="mb-3 h-4 w-24 rounded bg-surface-elevated" />
            <div className="h-8 w-16 rounded bg-surface-elevated" />
          </div>
        ))}
      </div>

      {/* Table skeleton */}
      <div className="rounded-xl border border-border bg-surface p-6">
        <div className="mb-4 h-5 w-32 rounded bg-surface-elevated" />
        <div className="space-y-3">
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i} className="flex items-center gap-4">
              <div className="h-4 w-1/4 rounded bg-surface-elevated" />
              <div className="h-4 w-1/5 rounded bg-surface-elevated" />
              <div className="h-4 w-1/6 rounded bg-surface-elevated" />
              <div className="h-4 w-1/6 rounded bg-surface-elevated" />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
