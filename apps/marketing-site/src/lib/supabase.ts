import { createPublicClient } from "@stalela/supabase/client";

/**
 * Public (anon-key) Supabase client shared across the marketing site.
 * Write operations are handled by the admin app.
 */
export const supabase = createPublicClient();
