"use client";

import { useState } from "react";
import { useTranslations } from "next-intl";
import { Button } from "./Button";

interface FormField {
  name: string;
  label: string;
  type: "text" | "email" | "tel" | "number" | "select" | "textarea";
  required?: boolean;
  placeholder?: string;
  options?: string[];
}

interface LeadFormProps {
  fields: FormField[];
  submitLabel?: string;
  source: string;
}

interface FormStatus {
  type: "idle" | "loading" | "success" | "error";
  message?: string;
}

export function LeadForm({ fields, submitLabel, source }: LeadFormProps) {
  const t = useTranslations("LeadForm");
  const resolvedSubmitLabel = submitLabel || t("submit");
  const [status, setStatus] = useState<FormStatus>({ type: "idle" });

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setStatus({ type: "loading" });

    const formData = new FormData(e.currentTarget);
    const data: Record<string, string> = { source };
    formData.forEach((value, key) => {
      data[key] = value.toString();
    });

    try {
      const res = await fetch("/api/lead", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });

      if (!res.ok) {
        const body = await res.json().catch(() => null);
        throw new Error(body?.error || t("error"));
      }

      setStatus({
        type: "success",
        message: t("success"),
      });
      (e.target as HTMLFormElement).reset();
    } catch (err) {
      setStatus({
        type: "error",
        message: err instanceof Error ? err.message : t("error"),
      });
    }
  }

  if (status.type === "success") {
    return (
      <div className="rounded-xl border border-copper-200 bg-copper-50 p-8 text-center">
        <p className="text-lg font-medium text-copper-800">{status.message}</p>
      </div>
    );
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-5">
      {fields.map((field) => (
        <div key={field.name}>
          <label
            htmlFor={field.name}
            className="mb-1.5 block text-sm font-medium text-gray-700"
          >
            {field.label}
            {field.required && <span className="text-red-500"> *</span>}
          </label>

          {field.type === "textarea" ? (
            <textarea
              id={field.name}
              name={field.name}
              required={field.required}
              placeholder={field.placeholder}
              rows={4}
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm text-gray-900 placeholder-gray-400 focus:border-copper-600 focus:outline-none focus:ring-1 focus:ring-copper-600"
            />
          ) : field.type === "select" ? (
            <select
              id={field.name}
              name={field.name}
              required={field.required}
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm text-gray-900 focus:border-copper-600 focus:outline-none focus:ring-1 focus:ring-copper-600"
            >
              <option value="">{t("select")}</option>
              {field.options?.map((opt) => (
                <option key={opt} value={opt}>
                  {opt}
                </option>
              ))}
            </select>
          ) : (
            <input
              id={field.name}
              name={field.name}
              type={field.type}
              required={field.required}
              placeholder={field.placeholder}
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm text-gray-900 placeholder-gray-400 focus:border-copper-600 focus:outline-none focus:ring-1 focus:ring-copper-600"
            />
          )}
        </div>
      ))}

      {status.type === "error" && (
        <p className="text-sm text-red-600">{status.message}</p>
      )}

      <Button type="submit" disabled={status.type === "loading"} className="w-full">
        {status.type === "loading" ? t("submitting") : resolvedSubmitLabel}
      </Button>
    </form>
  );
}
