"use client";

import { Search, BarChart3, Globe, Sparkles, TrendingUp, FileText, Eye, Rocket, Target, Settings } from "lucide-react";
import { StudioPage } from "../StudioPage";

export function SEOContent() {
  return (
    <StudioPage
      serviceSlug="seo"
      badge="Stalela Studio"
      badgeIcon={Search}
      headline="Get found on Google —"
      headlineAccent="by the right people"
      description="South African businesses lose thousands in revenue every month by being invisible on search. We build SEO strategies that put you in front of customers who are already looking for what you offer."
      heroImage="/images/generated/studio/seo-hero.png"
      heroImageAlt="SEO analytics dashboard"
      gallery={[
        { src: "/images/generated/studio/seo-hero.png", alt: "SEO analytics and strategy", caption: "Data-driven SEO that moves the needle" },
        { src: "/images/generated/studio/seo-dashboard.png", alt: "SEO dashboard with metrics", caption: "Track your rankings, traffic, and conversions" },
      ]}
      galleryTitle="Real results, transparent reporting"
      galleryDescription="See exactly where your traffic comes from and where it's going."
      features={[
        { icon: Search, title: "Keyword research", description: "Find the exact terms your South African customers search for — including local and industry-specific keywords." },
        { icon: FileText, title: "On-page SEO", description: "Title tags, meta descriptions, headings, image alt text, schema markup — every page optimised." },
        { icon: Globe, title: "Technical SEO", description: "Site speed, mobile-friendliness, crawlability, indexing, Core Web Vitals — the foundation of rankings." },
        { icon: TrendingUp, title: "Link building", description: "Quality South African backlinks from directories, publications, and industry sites." },
        { icon: BarChart3, title: "Analytics & reporting", description: "Monthly reports with rankings, traffic, conversions, and actionable next steps." },
        { icon: Target, title: "Local SEO", description: "Google Business Profile optimisation, local citations, and map pack visibility for your area." },
      ]}
      process={[
        { step: "01", icon: Search, title: "Audit", description: "We perform a comprehensive SEO audit of your website — technical health, content gaps, keyword opportunities, and competitor analysis.", deliverables: ["Technical SEO audit", "Keyword gap analysis", "Competitor benchmarking"] },
        { step: "02", icon: Settings, title: "Strategy", description: "Based on the audit, we build a prioritised roadmap. Quick wins first, then sustained growth strategies.", deliverables: ["SEO strategy document", "Keyword target list", "Content calendar"] },
        { step: "03", icon: Rocket, title: "Implement", description: "We execute — fixing technical issues, optimising pages, creating content, and building backlinks.", deliverables: ["On-page optimisations", "Technical fixes", "Content creation", "Link building"] },
        { step: "04", icon: BarChart3, title: "Measure", description: "Monthly reporting with clear metrics. We track rankings, organic traffic, and conversions — and adjust strategy accordingly.", deliverables: ["Monthly ranking report", "Traffic analytics", "Conversion tracking", "Strategy adjustments"] },
      ]}
      techStack={[
        "Google Search Console", "Google Analytics 4", "Ahrefs", "Semrush",
        "Screaming Frog", "Schema.org", "Core Web Vitals", "PageSpeed Insights",
      ]}
      packages={[
        { name: "SEO Audit", price: "From R3,500", description: "A one-time deep dive into your site's SEO health.", includes: ["Full technical audit", "Keyword opportunity analysis", "Competitor review", "Prioritised fix list", "30-minute strategy call"], highlighted: false },
        { name: "Growth", price: "From R5,500/mo", description: "Ongoing SEO to build sustainable organic traffic.", includes: ["Monthly on-page optimisation", "Technical SEO maintenance", "2 blog posts / month", "Local SEO management", "Monthly reporting & call", "Link building (5+ per month)"], highlighted: true },
        { name: "Accelerate", price: "From R9,500/mo", description: "Aggressive SEO for competitive markets.", includes: ["Everything in Growth", "4 blog posts / month", "Advanced link building (10+)", "Content hub strategy", "Conversion rate optimisation", "Bi-weekly reporting & calls", "Dedicated SEO manager"], highlighted: false },
      ]}
      faqs={[
        { q: "How long does SEO take to show results?", a: "Typically 3–6 months for meaningful results. Quick wins (technical fixes, local SEO) can show improvement in weeks. We set realistic expectations upfront." },
        { q: "Do you guarantee first-page rankings?", a: "No ethical SEO provider can guarantee rankings — Google's algorithm has hundreds of factors. We guarantee consistent effort, transparent reporting, and proven strategies." },
        { q: "Do you write the content too?", a: "Yes. Our packages include SEO-optimised blog posts and page content written by humans who understand your industry." },
        { q: "Can you help with Google Ads too?", a: "SEO and Google Ads work best together. Ask about our Digital Marketing package for a combined approach." },
        { q: "What if I change my website?", a: "We'll handle redirects, re-optimisation, and ensure you don't lose existing rankings during migrations." },
      ]}
      ctaIcon={Search}
      ctaHeadline="Ready to be found?"
      ctaDescription="Get a free SEO audit and see exactly where your opportunities are."
    />
  );
}
