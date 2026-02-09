/**
 * Server-side Supabase admin client for data operations.
 * Uses service-role key for full access, bypassing RLS.
 * Lazy-initialised so imports don't crash during `next build`
 * when env vars may not yet be available.
 */
import { createAdminClient } from "@stalela/supabase/client";
import { createBlogApi } from "@stalela/supabase/blog";
import { createLeadsApi } from "@stalela/supabase/leads";
import { createCustomersApi } from "@stalela/supabase/customers";
import { createSeoApi } from "@stalela/supabase/seo";
import { createMetricsApi } from "@stalela/supabase/metrics";

function lazy<T extends object>(factory: () => T): T {
  let instance: T | undefined;
  return new Proxy({} as object, {
    get(_, prop) {
      if (!instance) instance = factory();
      return (instance as Record<string, unknown>)[prop as string];
    },
  }) as T;
}

const getClient = (() => {
  let client: ReturnType<typeof createAdminClient> | undefined;
  return () => {
    if (!client) client = createAdminClient();
    return client;
  };
})();

export const blogApi = lazy(() => createBlogApi(getClient()));
export const leadsApi = lazy(() => createLeadsApi(getClient()));
export const customersApi = lazy(() => createCustomersApi(getClient()));
export const seoApi = lazy(() => createSeoApi(getClient()));
export const metricsApi = lazy(() => createMetricsApi(getClient()));
