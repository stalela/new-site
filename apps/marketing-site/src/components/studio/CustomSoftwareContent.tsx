"use client";

import { Code2, Database, Server, Shield, Sparkles, Cloud, Rocket, Settings, Eye, BarChart3, Cog } from "lucide-react";
import { StudioPage } from "../StudioPage";

export function CustomSoftwareContent() {
  return (
    <StudioPage
      serviceSlug="custom-software"
      badge="Stalela Studio"
      badgeIcon={Code2}
      headline="Software built for"
      headlineAccent="your business"
      description="Off-the-shelf software forces you to adapt to it. Custom software adapts to you. We build web applications, internal tools, automations, and integrations for South African businesses that have outgrown spreadsheets and generic SaaS."
      heroImage="/images/generated/studio/software-hero.png"
      heroImageAlt="Custom software architecture"
      gallery={[
        { src: "/images/generated/studio/software-hero.png", alt: "Software development workspace", caption: "Tailored solutions for complex business needs" },
        { src: "/images/generated/studio/software-architecture.png", alt: "System architecture diagram", caption: "Scalable, maintainable architecture from day one" },
      ]}
      galleryTitle="Built right, built to last"
      galleryDescription="Modern tech stack, clean architecture, and code you actually own."
      features={[
        { icon: Server, title: "Web applications", description: "Customer portals, admin dashboards, booking systems, inventory management — whatever your business needs." },
        { icon: Cog, title: "Internal tools", description: "Replace spreadsheets and manual processes with custom tools that save your team hours every week." },
        { icon: Database, title: "Integrations & APIs", description: "Connect your systems — accounting, CRM, ERP, payment gateways, government portals." },
        { icon: Cloud, title: "Cloud infrastructure", description: "Deployed on AWS, Azure, or Google Cloud with proper security, backups, and monitoring." },
        { icon: Shield, title: "Security first", description: "Authentication, authorization, encryption, and POPIA compliance baked in from the start." },
        { icon: BarChart3, title: "Reporting & analytics", description: "Built-in dashboards and reporting so you can make data-driven decisions." },
      ]}
      process={[
        { step: "01", icon: Eye, title: "Discovery", description: "We map your business processes, pain points, and requirements. We identify the simplest solution that solves the real problem.", deliverables: ["Process mapping", "Requirements document", "Solution architecture", "Project estimate"] },
        { step: "02", icon: Settings, title: "Design", description: "We design the user experience, data model, and system architecture. You see clickable prototypes before we write a line of code.", deliverables: ["UI/UX wireframes", "Clickable prototype", "Data model", "Technical specification"] },
        { step: "03", icon: Code2, title: "Build", description: "Agile development in 2-week sprints. You see working software every fortnight and can adjust priorities as we go.", deliverables: ["Working software (every 2 weeks)", "Sprint demos", "Test suite", "Documentation"] },
        { step: "04", icon: Rocket, title: "Launch & support", description: "We deploy to production, train your team, and provide ongoing support and maintenance.", deliverables: ["Production deployment", "User training", "Admin documentation", "SLA support agreement"] },
      ]}
      techStack={[
        "TypeScript", "React", "Next.js", "Node.js",
        "PostgreSQL", "Redis", "Docker", "Kubernetes",
        "AWS", "Vercel", "Prisma", "tRPC",
        "Tailwind CSS", "Playwright", "GitHub Actions",
      ]}
      packages={[
        { name: "MVP", price: "From R45,000", description: "Validate your idea with a minimum viable product.", includes: ["Requirements workshop", "UI/UX design", "Core feature development", "Cloud deployment", "3 months bug-fix support", "Source code ownership"], highlighted: false },
        { name: "Business App", price: "From R95,000", description: "A full-featured application for your business.", includes: ["Comprehensive discovery", "Full UI/UX design & prototype", "Multi-module development", "Integrations (up to 3)", "User authentication & roles", "Cloud deployment & monitoring", "6 months support", "Source code ownership"], highlighted: true },
        { name: "Enterprise", price: "Custom", description: "Complex systems for larger operations.", includes: ["Everything in Business App", "Unlimited integrations", "Advanced security & compliance", "Multi-tenant architecture", "CI/CD pipeline", "Load testing & optimisation", "Dedicated team", "12 months SLA support"], highlighted: false },
      ]}
      faqs={[
        { q: "How long does a custom software project take?", a: "MVPs: 6–10 weeks. Business applications: 3–6 months. Enterprise projects: 6–12+ months. We give you a realistic estimate after discovery." },
        { q: "Do I own the code?", a: "Yes. 100%. The source code, database, and all intellectual property transfer to you. No vendor lock-in." },
        { q: "What if my requirements change during development?", a: "That's expected! Our agile process is designed for change. We re-prioritise every sprint so you're always building the most valuable features." },
        { q: "Can you take over an existing codebase?", a: "Yes. We can audit, maintain, and extend existing applications. We'll give you an honest assessment of the code quality and a plan forward." },
        { q: "What about POPIA compliance?", a: "We build data protection into the architecture — consent management, data minimisation, encryption, right to erasure, and audit logging." },
      ]}
      ctaIcon={Code2}
      ctaHeadline="Got a business problem that needs a software solution?"
      ctaDescription="Book a free discovery call and let's map out the simplest path to solving it."
    />
  );
}
