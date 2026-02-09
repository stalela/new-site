import { createClient, type SupabaseClient } from "@supabase/supabase-js";

/**
 * Create a public (anon-key) Supabase client.
 * Used by the marketing site for read-only access.
 */
export function createPublicClient(): SupabaseClient {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL!;
  const key = process.env.NEXT_PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY!;
  return createClient(url, key);
}

/**
 * Create an admin (service-role) Supabase client.
 * Used by the admin app for full CRUD access.
 * NEVER expose the service-role key to the browser.
 */
export function createAdminClient(): SupabaseClient {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL!;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY!;
  return createClient(url, key, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
}

export type { SupabaseClient };
