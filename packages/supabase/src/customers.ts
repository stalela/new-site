import type { SupabaseClient } from "@supabase/supabase-js";
import type { Customer, CustomerInsert, CustomerUpdate } from "./types";

export function createCustomersApi(supabase: SupabaseClient) {
  return {
    /** List customers with optional filters. */
    async list(opts?: {
      status?: string;
      search?: string;
      limit?: number;
      offset?: number;
    }) {
      let query = supabase
        .from("customers")
        .select("*", { count: "exact" });

      if (opts?.status) query = query.eq("status", opts.status);
      if (opts?.search) {
        query = query.or(
          `email.ilike.%${opts.search}%,name.ilike.%${opts.search}%,company.ilike.%${opts.search}%`
        );
      }

      const final = query.order("created_at", { ascending: false });
      if (opts?.limit) final.limit(opts.limit);
      if (opts?.offset) final.range(opts.offset, opts.offset + (opts.limit ?? 20) - 1);

      const { data, error, count } = await final;
      if (error) throw error;
      return { customers: data as Customer[], total: count ?? 0 };
    },

    /** Get a single customer by ID. */
    async getById(id: string) {
      const { data, error } = await supabase
        .from("customers")
        .select("*")
        .eq("id", id)
        .single();
      if (error) throw error;
      return data as Customer;
    },

    /** Create a customer. */
    async create(customer: CustomerInsert) {
      const now = new Date().toISOString();
      const { data, error } = await supabase
        .from("customers")
        .insert({ ...customer, created_at: now, updated_at: now })
        .select()
        .single();
      if (error) throw error;
      return data as Customer;
    },

    /** Update a customer. */
    async update(id: string, updates: CustomerUpdate) {
      const { data, error } = await supabase
        .from("customers")
        .update({ ...updates, updated_at: new Date().toISOString() })
        .eq("id", id)
        .select()
        .single();
      if (error) throw error;
      return data as Customer;
    },

    /** Promote a lead to a customer. */
    async promoteFromLead(leadId: string, overrides?: Partial<CustomerInsert>) {
      // Fetch the lead
      const { data: lead, error: leadErr } = await supabase
        .from("leads")
        .select("*")
        .eq("id", leadId)
        .single();
      if (leadErr) throw leadErr;

      const now = new Date().toISOString();
      const { data, error } = await supabase
        .from("customers")
        .insert({
          lead_id: leadId,
          name: overrides?.name ?? lead.name ?? lead.email,
          email: overrides?.email ?? lead.email,
          phone: overrides?.phone ?? lead.phone ?? null,
          company: overrides?.company ?? null,
          status: "prospect",
          notes: overrides?.notes ?? null,
          created_at: now,
          updated_at: now,
        })
        .select()
        .single();
      if (error) throw error;
      return data as Customer;
    },

    /** Delete a customer. */
    async delete(id: string) {
      const { error } = await supabase
        .from("customers")
        .delete()
        .eq("id", id);
      if (error) throw error;
    },
  };
}
