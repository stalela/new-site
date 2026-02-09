"use client";

import { Layers, Palette, Eye, Sparkles, FileImage, PenTool, Rocket, Monitor, Image, BookOpen, Megaphone } from "lucide-react";
import { StudioPage } from "../StudioPage";

export function GraphicDesignContent() {
  return (
    <StudioPage
      serviceSlug="graphic-design"
      badge="Stalela Studio"
      badgeIcon={Layers}
      headline="Design that communicates,"
      headlineAccent="not just decorates"
      description="From social media posts to pitch decks to packaging — we create visual assets that grab attention and drive action. Consistent, professional design for every touchpoint of your South African business."
      heroImage="/images/generated/studio/graphic-hero.png"
      heroImageAlt="Vibrant graphic design portfolio"
      gallery={[
        { src: "/images/generated/studio/graphic-hero.png", alt: "Graphic design workspace", caption: "Creative design for every business need" },
        { src: "/images/generated/studio/graphic-portfolio.png", alt: "Portfolio of design work", caption: "Social media, print, packaging and more" },
      ]}
      galleryTitle="Design for every channel"
      galleryDescription="Consistent visual identity across digital and print."
      features={[
        { icon: Image, title: "Social media graphics", description: "Scroll-stopping posts, stories, and ads for Instagram, Facebook, LinkedIn, and TikTok." },
        { icon: BookOpen, title: "Presentations & decks", description: "Investor decks, sales presentations, and company profiles that win deals." },
        { icon: Megaphone, title: "Marketing collateral", description: "Flyers, brochures, banners, pull-ups, and posters for events and campaigns." },
        { icon: Monitor, title: "Digital assets", description: "Email headers, blog images, infographics, and web banners." },
        { icon: FileImage, title: "Packaging design", description: "Product labels, boxes, and packaging that stands out on the shelf." },
        { icon: Palette, title: "Brand consistency", description: "Every design follows your brand guidelines — colours, fonts, tone, style." },
      ]}
      process={[
        { step: "01", icon: PenTool, title: "Brief", description: "Tell us what you need — the type of design, where it will be used, and your brand assets. Share examples of styles you like.", deliverables: ["Design brief", "Brand asset kit review"] },
        { step: "02", icon: Eye, title: "Concept", description: "We create initial concepts based on your brief. You'll see realistic previews in context (on a phone, on a wall, etc.).", deliverables: ["Initial concepts", "In-context mockups"] },
        { step: "03", icon: Sparkles, title: "Refine", description: "Feedback rounds to perfect every detail — layout, colours, copy placement, sizing for all placements.", deliverables: ["Refined designs", "Multi-size adaptations"] },
        { step: "04", icon: Rocket, title: "Deliver", description: "Final files in all formats and sizes you need — ready for print, web, or social upload.", deliverables: ["Print-ready files (PDF, CMYK)", "Digital files (PNG, JPG)", "Source files (AI, PSD)"] },
      ]}
      packages={[
        { name: "Single Design", price: "From R600", description: "One-off designs when you need something specific.", includes: ["1 custom design", "2 revision rounds", "Print & digital formats", "Source file included"], highlighted: false },
        { name: "Design Pack", price: "From R2,500", description: "A bundle of designs for a campaign or launch.", includes: ["5 custom designs", "3 revision rounds each", "Multi-size adaptations", "Social media optimised", "All file formats", "Source files"], highlighted: true },
        { name: "Monthly Retainer", price: "From R4,500/mo", description: "Ongoing design support for your business.", includes: ["Up to 10 designs per month", "Unlimited revisions", "24-hour turnaround", "Priority support", "All file formats & source files", "Content calendar collaboration"], highlighted: false },
      ]}
      faqs={[
        { q: "What types of design do you handle?", a: "Social media graphics, flyers, brochures, banners, presentations, infographics, packaging, email templates, and more. If it's visual, we design it." },
        { q: "Do I get the source files?", a: "Yes. Every project includes editable source files (AI, PSD, or Figma depending on the project)." },
        { q: "Can you work with my existing brand guidelines?", a: "Absolutely. Send us your brand guide and we'll match everything perfectly. If you don't have one, we can create one." },
        { q: "What's the turnaround time?", a: "Single designs: 2–3 business days. Design packs: 5–7 business days. Retainer clients get 24-hour turnaround." },
        { q: "Do you handle printing?", a: "We focus on design but can coordinate with our trusted print partners for production." },
      ]}
      ctaIcon={Layers}
      ctaHeadline="Need professional design, fast?"
      ctaDescription="Whether it's a single flyer or a full campaign — we've got you covered."
    />
  );
}
