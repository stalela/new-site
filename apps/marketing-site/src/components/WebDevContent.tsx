"use client";

import { Link } from "@/i18n/navigation";
import Image from "next/image";
import {
  Globe,
  Search,
  Palette,
  Code2,
  Rocket,
  Smartphone,
  Zap,
  ShieldCheck,
  BarChart3,
  ArrowRight,
  Check,
  MessageSquare,
  Layers,
  Monitor,
} from "lucide-react";
import { Button } from "./Button";
import { getCategoryForService } from "@/lib/services-data";

/* ── Process steps ── */
const process = [
  {
    step: "01",
    icon: Search,
    title: "Discovery",
    description:
      "We learn about your business, audience, and goals. What does your brand stand for? Who are your customers? What should visitors do on your site?",
    deliverables: ["Brand questionnaire", "Competitor analysis", "Sitemap & content plan"],
  },
  {
    step: "02",
    icon: Palette,
    title: "Design",
    description:
      "We create wireframes and a full visual design based on your brand. You review, give feedback, and approve before a single line of code is written.",
    deliverables: ["Wireframes", "Visual mockups", "Mobile designs", "Revision rounds"],
  },
  {
    step: "03",
    icon: Code2,
    title: "Develop",
    description:
      "We build your site with clean, modern code. Fast load times, responsive on every device, SEO-optimised from the start.",
    deliverables: ["Responsive development", "CMS integration", "Contact forms", "Analytics setup"],
  },
  {
    step: "04",
    icon: Rocket,
    title: "Launch & Support",
    description:
      "We deploy your site, set up your domain, and hand everything over. Need ongoing changes? We offer monthly support plans.",
    deliverables: ["Domain & hosting setup", "SSL certificate", "Launch checklist", "Post-launch support"],
  },
];

/* ── What you get features ── */
const features = [
  {
    icon: Smartphone,
    title: "Mobile-first design",
    description: "Every site is built responsive from the ground up — not bolted on as an afterthought.",
  },
  {
    icon: Zap,
    title: "Lightning fast",
    description: "Optimised images, modern frameworks, and clean code for sub-2-second load times.",
  },
  {
    icon: ShieldCheck,
    title: "Secure by default",
    description: "SSL certificates, secure forms, and best-practice hosting to protect your visitors.",
  },
  {
    icon: BarChart3,
    title: "SEO ready",
    description: "Semantic HTML, meta tags, structured data, and sitemap — ready for Google from day one.",
  },
  {
    icon: MessageSquare,
    title: "Lead capture built in",
    description: "Contact forms, WhatsApp links, and call-to-action buttons that actually convert.",
  },
  {
    icon: Layers,
    title: "Easy to update",
    description: "Content management system so you can edit text and images yourself — no developer needed.",
  },
];

/* ── Packages ── */
const packages = [
  {
    name: "Landing Page",
    price: "From R3,500",
    description: "A single, focused page to capture leads or announce your business.",
    includes: [
      "1-page responsive site",
      "Custom design",
      "Contact form",
      "Mobile optimised",
      "Basic SEO setup",
      "Hosting setup",
    ],
    highlighted: false,
  },
  {
    name: "Business Website",
    price: "From R8,500",
    description: "A complete multi-page site to showcase your services and build credibility.",
    includes: [
      "Up to 5 pages",
      "Custom design & branding",
      "Contact & lead forms",
      "CMS for easy edits",
      "Google Analytics",
      "SEO optimisation",
      "Domain & hosting setup",
      "30-day post-launch support",
    ],
    highlighted: true,
  },
  {
    name: "Custom Build",
    price: "Let's talk",
    description: "E-commerce, portals, integrations, or anything beyond a standard site.",
    includes: [
      "Unlimited pages",
      "Custom functionality",
      "Third-party integrations",
      "E-commerce capability",
      "Advanced SEO & performance",
      "Ongoing support plan",
    ],
    highlighted: false,
  },
];

/* ── Tech stack badges ── */
const techStack = [
  "Next.js",
  "React",
  "TypeScript",
  "Tailwind CSS",
  "WordPress",
  "Shopify",
  "Vercel",
  "Figma",
];

/* ── FAQ ── */
const faqs = [
  {
    q: "How long does a website take to build?",
    a: "A landing page typically takes 1–2 weeks. A full business site takes 3–5 weeks depending on complexity and content readiness.",
  },
  {
    q: "Do I need to provide content and images?",
    a: "We can guide you through content creation and source professional stock imagery. If you have existing copy and photos, even better.",
  },
  {
    q: "Can I update the site myself after launch?",
    a: "Yes. We set up a content management system (CMS) so you can edit text, images, and pages without any coding knowledge.",
  },
  {
    q: "Do you handle hosting and domains?",
    a: "Absolutely. We set up your domain, hosting, SSL, and email — everything you need to go live.",
  },
  {
    q: "What if I need changes after launch?",
    a: "We offer monthly support plans starting from R500/month, or you can request ad-hoc changes at our hourly rate.",
  },
  {
    q: "Do you build e-commerce sites?",
    a: "Yes. We build online stores using Shopify or custom solutions depending on your needs and budget.",
  },
];

export function WebDevContent() {
  const category = getCategoryForService("website-development");

  return (
    <div>
      {/* Breadcrumbs */}
      <nav className="flex items-center gap-2 text-sm text-gray-500">
        <Link href="/services" className="transition-colors hover:text-gray-700">
          Services
        </Link>
        <span>/</span>
        <Link
          href={`/services/${category?.slug}`}
          className="transition-colors hover:text-gray-700"
        >
          {category?.name}
        </Link>
        <span>/</span>
        <span className="font-medium text-gray-900">Website Development</span>
      </nav>

      {/* ─── Hero ─── */}
      <div className="mt-10 grid items-center gap-10 lg:grid-cols-2">
        <div>
          <div className="inline-flex items-center gap-2 rounded-full bg-copper-100 px-3 py-1 text-xs font-semibold text-copper-700">
            <Globe className="h-3.5 w-3.5" />
            Stalela Studio
          </div>
          <h1 className="mt-4 text-4xl font-bold tracking-tight text-gray-900 sm:text-5xl">
            Websites that work{" "}
            <span className="text-copper-600">as hard as you do</span>
          </h1>
          <p className="mt-4 text-lg leading-relaxed text-gray-600">
            We design and build fast, mobile-friendly websites for South African
            businesses. From a single landing page to a full online presence — we
            handle everything so you can focus on your business.
          </p>
          <div className="mt-8 flex flex-wrap gap-4">
            <Button href="/contact" size="lg">
              Get a free quote
            </Button>
            <Button href="#packages" variant="outline" size="lg">
              View packages
            </Button>
          </div>
        </div>
        <div className="relative aspect-[4/3] overflow-hidden rounded-2xl bg-gray-100">
          <Image
            src="/images/generated/category-digital-services.png"
            alt="Website development studio workspace"
            fill
            className="object-cover"
            sizes="(max-width: 1024px) 100vw, 50vw"
            priority
          />
        </div>
      </div>

      {/* ─── What you get ─── */}
      <div className="mt-24">
        <div className="mx-auto max-w-2xl text-center">
          <h2 className="text-3xl font-bold tracking-tight text-gray-900">
            What you get
          </h2>
          <p className="mt-3 text-lg text-gray-600">
            Every site we build comes with the essentials baked in.
          </p>
        </div>
        <div className="mt-12 grid gap-8 sm:grid-cols-2 lg:grid-cols-3">
          {features.map((feature) => (
            <div key={feature.title} className="group">
              <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-copper-100 transition-colors group-hover:bg-copper-200">
                <feature.icon className="h-5 w-5 text-copper-600" />
              </div>
              <h3 className="mt-4 font-semibold text-gray-900">{feature.title}</h3>
              <p className="mt-2 text-sm leading-relaxed text-gray-600">
                {feature.description}
              </p>
            </div>
          ))}
        </div>
      </div>

      {/* ─── Our process ─── */}
      <div className="mt-24">
        <div className="mx-auto max-w-2xl text-center">
          <h2 className="text-3xl font-bold tracking-tight text-gray-900">
            Our process
          </h2>
          <p className="mt-3 text-lg text-gray-600">
            From first conversation to launch day — here&apos;s how we work.
          </p>
        </div>
        <div className="mt-12 space-y-12">
          {process.map((item, i) => (
            <div
              key={item.step}
              className={`grid items-start gap-8 lg:grid-cols-2 ${i % 2 !== 0 ? "lg:direction-rtl" : ""}`}
            >
              <div className={i % 2 !== 0 ? "lg:order-2" : ""}>
                <div className="flex items-center gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-copper-600 text-white">
                    <item.icon className="h-5 w-5" />
                  </div>
                  <span className="text-sm font-bold text-copper-600">
                    Step {item.step}
                  </span>
                </div>
                <h3 className="mt-4 text-2xl font-bold text-gray-900">{item.title}</h3>
                <p className="mt-3 leading-relaxed text-gray-600">
                  {item.description}
                </p>
              </div>
              <div className={`rounded-xl border border-gray-200 bg-gray-50 p-6 ${i % 2 !== 0 ? "lg:order-1" : ""}`}>
                <p className="mb-3 text-xs font-semibold uppercase tracking-wider text-gray-400">
                  Deliverables
                </p>
                <ul className="space-y-2.5">
                  {item.deliverables.map((d) => (
                    <li key={d} className="flex items-center gap-2.5 text-sm text-gray-700">
                      <Check className="h-4 w-4 flex-shrink-0 text-copper-600" />
                      {d}
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* ─── Tech stack ─── */}
      <div className="mt-24 rounded-2xl bg-gray-900 px-6 py-12 sm:px-12">
        <div className="mx-auto max-w-2xl text-center">
          <h2 className="text-2xl font-bold text-white">Built with modern technology</h2>
          <p className="mt-2 text-gray-400">
            We use industry-leading tools to ensure your site is fast, secure, and maintainable.
          </p>
        </div>
        <div className="mx-auto mt-8 flex max-w-2xl flex-wrap items-center justify-center gap-3">
          {techStack.map((tech) => (
            <span
              key={tech}
              className="rounded-full border border-gray-700 bg-gray-800 px-4 py-2 text-sm font-medium text-gray-300"
            >
              {tech}
            </span>
          ))}
        </div>
      </div>

      {/* ─── Packages ─── */}
      <div id="packages" className="mt-24 scroll-mt-24">
        <div className="mx-auto max-w-2xl text-center">
          <h2 className="text-3xl font-bold tracking-tight text-gray-900">
            Packages
          </h2>
          <p className="mt-3 text-lg text-gray-600">
            Straightforward pricing. No hidden fees. Choose what fits your business.
          </p>
        </div>
        <div className="mt-12 grid gap-8 lg:grid-cols-3">
          {packages.map((pkg) => (
            <div
              key={pkg.name}
              className={`relative flex flex-col rounded-2xl border p-8 ${
                pkg.highlighted
                  ? "border-copper-600 ring-1 ring-copper-600"
                  : "border-gray-200"
              }`}
            >
              {pkg.highlighted && (
                <span className="absolute -top-3 left-6 rounded-full bg-copper-600 px-3 py-0.5 text-xs font-semibold text-white">
                  Most popular
                </span>
              )}
              <h3 className="text-lg font-bold text-gray-900">{pkg.name}</h3>
              <p className="mt-1 text-2xl font-bold text-copper-600">{pkg.price}</p>
              <p className="mt-3 text-sm text-gray-600">{pkg.description}</p>
              <ul className="mt-6 flex-1 space-y-3">
                {pkg.includes.map((item) => (
                  <li key={item} className="flex items-start gap-2.5 text-sm text-gray-700">
                    <Check className="mt-0.5 h-4 w-4 flex-shrink-0 text-copper-600" />
                    {item}
                  </li>
                ))}
              </ul>
              <div className="mt-8">
                <Button
                  href="/contact"
                  variant={pkg.highlighted ? "primary" : "outline"}
                  className="w-full"
                >
                  {pkg.price === "Let's talk" ? "Contact us" : "Get started"}
                </Button>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* ─── FAQ ─── */}
      <div className="mt-24">
        <div className="mx-auto max-w-2xl text-center">
          <h2 className="text-3xl font-bold tracking-tight text-gray-900">
            Frequently asked questions
          </h2>
        </div>
        <div className="mx-auto mt-12 max-w-3xl divide-y divide-gray-200">
          {faqs.map((faq) => (
            <details key={faq.q} className="group py-5">
              <summary className="flex cursor-pointer items-center justify-between text-left font-semibold text-gray-900 [&::-webkit-details-marker]:hidden">
                {faq.q}
                <span className="ml-4 flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-gray-100 text-gray-500 transition-transform group-open:rotate-45">
                  +
                </span>
              </summary>
              <p className="mt-3 leading-relaxed text-gray-600">{faq.a}</p>
            </details>
          ))}
        </div>
      </div>

      {/* ─── Final CTA ─── */}
      <div className="mt-24 rounded-2xl bg-copper-50 p-8 text-center sm:p-12">
        <Monitor className="mx-auto h-10 w-10 text-copper-600" />
        <h2 className="mt-4 text-2xl font-bold text-gray-900 sm:text-3xl">
          Ready to build your website?
        </h2>
        <p className="mx-auto mt-3 max-w-lg text-gray-600">
          Tell us about your business and we&apos;ll send you a free proposal
          within 48 hours. No commitment, no pressure.
        </p>
        <div className="mt-8 flex flex-wrap items-center justify-center gap-4">
          <Button href="/contact" size="lg">
            Get a free quote
          </Button>
          <Button href="/register" variant="outline" size="lg">
            Register your business first
          </Button>
        </div>
      </div>

      {/* ─── Other digital services ─── */}
      {category && category.services.length > 1 && (
        <div className="mt-16 border-t border-gray-200 pt-10">
          <h2 className="text-lg font-bold text-gray-900">
            Other {category.name.toLowerCase()} services
          </h2>
          <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {category.services
              .filter((s) => s.slug !== "website-development")
              .map((s) => {
                const SIcon = s.icon;
                return (
                  <Link
                    key={s.slug}
                    href={`/services/${category.slug}/${s.slug}`}
                    className="flex items-center gap-3 rounded-xl border border-gray-200 p-4 transition-colors hover:border-copper-200 hover:bg-copper-50"
                  >
                    <SIcon className="h-5 w-5 text-copper-600" />
                    <span className="text-sm font-medium text-gray-900">
                      {s.title}
                    </span>
                    <ArrowRight className="ml-auto h-4 w-4 text-gray-400" />
                  </Link>
                );
              })}
          </div>
        </div>
      )}
    </div>
  );
}
