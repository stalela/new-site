"use client";

import { Megaphone, BarChart3, Target, Mail, Share2, TrendingUp, Eye, Rocket, Sparkles, PenTool } from "lucide-react";
import { StudioPage } from "../StudioPage";

export function DigitalMarketingContent() {
  return (
    <StudioPage
      serviceSlug="digital-marketing"
      badge="Stalela Studio"
      badgeIcon={Megaphone}
      headline="Marketing that actually"
      headlineAccent="moves the needle"
      description="From Google Ads to social media to email — we run full-funnel digital marketing campaigns for South African businesses. Every rand tracked, every result measured, every strategy refined."
      heroImage="/images/generated/studio/marketing-hero.png"
      heroImageAlt="Digital marketing campaign dashboard"
      gallery={[
        { src: "/images/generated/studio/marketing-hero.png", alt: "Marketing strategy overview", caption: "Full-funnel marketing for SA businesses" },
        { src: "/images/generated/studio/marketing-channels.png", alt: "Multi-channel marketing", caption: "Reach your audience everywhere they are" },
      ]}
      galleryTitle="Multi-channel, data-driven marketing"
      galleryDescription="We meet your customers where they already spend their time."
      features={[
        { icon: Target, title: "Google Ads", description: "Search, Display, Shopping, and YouTube campaigns — targeting South African audiences with precision." },
        { icon: Share2, title: "Social media ads", description: "Facebook, Instagram, LinkedIn, and TikTok advertising — from awareness to conversion." },
        { icon: Mail, title: "Email marketing", description: "Automated sequences, newsletters, and promotional campaigns that nurture and convert." },
        { icon: PenTool, title: "Content marketing", description: "Blog posts, social content, and lead magnets that attract your ideal customers." },
        { icon: BarChart3, title: "Analytics & CRO", description: "Conversion tracking, A/B testing, and data-driven optimisation across all channels." },
        { icon: TrendingUp, title: "Strategy & reporting", description: "Monthly strategy reviews with clear ROI reporting. Know exactly what's working." },
      ]}
      process={[
        { step: "01", icon: Target, title: "Research", description: "We analyse your market, competitors, audience, and current marketing efforts. We identify where the biggest opportunities lie.", deliverables: ["Market analysis", "Audience personas", "Competitor ad review", "Channel recommendations"] },
        { step: "02", icon: PenTool, title: "Strategy", description: "We build a custom marketing plan — channels, budgets, creatives, messaging, and KPIs. Everything aligned with your business goals.", deliverables: ["Marketing strategy", "Channel plan", "Budget allocation", "KPI framework"] },
        { step: "03", icon: Rocket, title: "Launch", description: "Campaigns go live across chosen channels. We create all ads, landing pages, and email sequences.", deliverables: ["Ad creatives", "Landing pages", "Email sequences", "Tracking setup"] },
        { step: "04", icon: BarChart3, title: "Optimise", description: "Continuous A/B testing, budget optimisation, and creative refreshes. Monthly reports with clear next steps.", deliverables: ["Monthly performance report", "A/B test results", "Budget optimisation", "Strategy refinements"] },
      ]}
      techStack={[
        "Google Ads", "Meta Ads", "LinkedIn Ads", "TikTok Ads",
        "Google Analytics 4", "Mailchimp", "HubSpot", "Hotjar",
        "Google Tag Manager", "Looker Studio",
      ]}
      packages={[
        { name: "Starter", price: "From R4,500/mo", description: "Get started with paid advertising.", includes: ["1 ad platform (Google or Meta)", "Campaign setup & management", "Ad creative design", "Monthly reporting", "Up to R10k ad spend managed"], highlighted: false },
        { name: "Growth", price: "From R8,500/mo", description: "Multi-channel marketing to scale.", includes: ["2 ad platforms", "Email marketing setup & management", "Ad creative & copywriting", "Landing page optimisation", "Bi-weekly reporting & calls", "Up to R25k ad spend managed"], highlighted: true },
        { name: "Scale", price: "From R15,000/mo", description: "Full-service marketing for serious growth.", includes: ["All major ad platforms", "Full email automation suite", "Content marketing (4 posts/mo)", "CRO & A/B testing", "Custom dashboards", "Weekly reporting & strategy", "Dedicated marketing manager", "Up to R50k+ ad spend managed"], highlighted: false },
      ]}
      faqs={[
        { q: "What's the minimum ad budget you recommend?", a: "For Google Ads, we recommend at least R5,000/month. For social media ads, at least R3,000/month. These are the ad spend amounts (separate from our management fee)." },
        { q: "Do you handle the creative (images, videos)?", a: "Yes. All packages include ad creative design. For video ads, we can produce simple motion graphics or coordinate with video production partners." },
        { q: "How quickly will I see results?", a: "Paid ads can generate leads within days of launch. Full campaign optimisation typically takes 2–3 months to reach peak performance." },
        { q: "Do I keep my ad accounts if I leave?", a: "Absolutely. All ad accounts, pixels, and data belong to you. We set everything up in your name." },
        { q: "Can you work with my existing campaigns?", a: "Yes. We'll audit what you have, keep what's working, and fix or replace what isn't." },
      ]}
      ctaIcon={Megaphone}
      ctaHeadline="Ready to grow your business online?"
      ctaDescription="Get a free marketing audit and discover your biggest growth opportunities."
    />
  );
}
