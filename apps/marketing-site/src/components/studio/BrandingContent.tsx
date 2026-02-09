"use client";

import { Palette, Search, PenTool, Rocket, Eye, BookOpen, Sparkles } from "lucide-react";
import { StudioPage } from "../StudioPage";

export function BrandingContent() {
  return (
    <StudioPage
      serviceSlug="branding-and-design"
      badge="Stalela Studio"
      badgeIcon={Palette}
      headline="Build a brand that stands out"
      headlineAccent="stands out"
      description="We create cohesive brand identities for South African businesses — from logo concepts and colour palettes to full brand guidelines and marketing collateral. Your brand should tell your story at every touchpoint."
      heroImage="/images/generated/studio/branding-hero.png"
      heroImageAlt="Brand identity mood board on a designer's desk"
      gallery={[
        { src: "/images/generated/studio/branding-hero.png", alt: "Brand identity mood board", caption: "Every project starts with understanding your brand story" },
        { src: "/images/generated/studio/branding-process.png", alt: "Brand design process", caption: "From rough sketches to polished digital identity" },
        { src: "/images/generated/studio/branding-deliverables.png", alt: "Brand deliverables", caption: "Complete brand package ready for print and digital" },
      ]}
      galleryTitle="Our work"
      galleryDescription="See how we transform business ideas into professional brand identities."
      features={[
        { icon: Eye, title: "Visual identity", description: "Logo, colours, typography, and imagery that form a cohesive look and feel." },
        { icon: BookOpen, title: "Brand guidelines", description: "A comprehensive document so anyone can use your brand consistently." },
        { icon: PenTool, title: "Print-ready files", description: "Business cards, letterheads, envelopes — all designed and production-ready." },
        { icon: Sparkles, title: "Social media kit", description: "Profile images, cover photos, and post templates for all platforms." },
        { icon: Palette, title: "Colour psychology", description: "Strategic colour choices that resonate with your target audience." },
        { icon: Rocket, title: "Launch-ready", description: "Everything you need to launch your brand across all channels." },
      ]}
      process={[
        { step: "01", icon: Search, title: "Discovery", description: "We interview you about your business, competitors, audience, and aspirations. What feeling should your brand evoke?", deliverables: ["Brand questionnaire", "Competitor audit", "Mood board"] },
        { step: "02", icon: PenTool, title: "Concept development", description: "We explore multiple creative directions — logo concepts, colour palettes, and typography pairings for your review.", deliverables: ["3 logo concepts", "Colour palette options", "Typography recommendations"] },
        { step: "03", icon: Palette, title: "Refinement", description: "Based on your feedback, we refine the chosen direction through multiple rounds until it's perfect.", deliverables: ["Refined logo", "Final colour palette", "Typography system", "Brand patterns"] },
        { step: "04", icon: Rocket, title: "Delivery", description: "We package everything into a complete brand kit with guidelines, templates, and all the files you need.", deliverables: ["Brand guidelines PDF", "Logo files (all formats)", "Business card designs", "Social media templates", "Letterhead & email signature"] },
      ]}
      packages={[
        { name: "Logo Only", price: "From R2,500", description: "A professional logo with multiple format exports.", includes: ["3 initial concepts", "Up to 3 revisions", "Full colour & mono", "Vector + raster files", "Favicon variant"], highlighted: false },
        { name: "Brand Identity", price: "From R6,500", description: "Complete visual identity for your business.", includes: ["Logo design (3 concepts)", "Colour palette & typography", "Brand guidelines document", "Business card design", "Social media profile kit", "Letterhead template"], highlighted: true },
        { name: "Full Brand Package", price: "From R12,000", description: "Everything plus marketing collateral.", includes: ["Everything in Brand Identity", "Stationery suite", "Email signature design", "Presentation template", "Flyer / brochure design", "Pull-up banner design", "Social media post templates"], highlighted: false },
      ]}
      faqs={[
        { q: "How long does branding take?", a: "A logo-only project takes 1–2 weeks. A full brand identity package takes 3–4 weeks, depending on revision rounds." },
        { q: "How many revision rounds do I get?", a: "Logo Only includes 3 rounds. Brand Identity and Full Package include 5 rounds. Additional revisions available at hourly rate." },
        { q: "What files do I receive?", a: "Vector files (SVG, AI, EPS), raster files (PNG, JPG), and a PDF brand guidelines document. Everything you need for print and digital." },
        { q: "Can you match my existing brand?", a: "Absolutely. If you have existing brand elements, we can build on them or do a full refresh while keeping recognisable elements." },
        { q: "Do you design for specific industries?", a: "We've designed brands for construction, tech, retail, hospitality, and professional services across South Africa." },
      ]}
      ctaIcon={Palette}
      ctaHeadline="Ready to build your brand?"
      ctaDescription="Tell us about your business and we'll create a brand that represents who you are."
    />
  );
}
