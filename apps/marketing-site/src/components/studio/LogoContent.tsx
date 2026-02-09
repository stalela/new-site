"use client";

import { PenTool, Search, Palette, Rocket, Sparkles, Eye, Layers, FileImage } from "lucide-react";
import { StudioPage } from "../StudioPage";

export function LogoContent() {
  return (
    <StudioPage
      serviceSlug="logo-design"
      badge="Stalela Studio"
      badgeIcon={PenTool}
      headline="A logo that means business"
      headlineAccent="means business"
      description="Your logo is the first thing people see. We design unique, versatile logos that work across every medium — from a tiny app icon to a massive billboard. Crafted for South African businesses that want to be taken seriously."
      heroImage="/images/generated/studio/logo-hero.png"
      heroImageAlt="Designer sketching logo concepts"
      gallery={[
        { src: "/images/generated/studio/logo-hero.png", alt: "Logo sketching process", caption: "Every logo starts with pencil and paper" },
        { src: "/images/generated/studio/logo-process.png", alt: "Logo design stages", caption: "From sketch to vector to final application" },
        { src: "/images/generated/studio/logo-showcase.png", alt: "Logo on various applications", caption: "Your logo, everywhere it needs to be" },
      ]}
      galleryTitle="From concept to creation"
      galleryDescription="See how we turn ideas into iconic logos."
      features={[
        { icon: Sparkles, title: "Unique concepts", description: "No templates. Every logo is designed from scratch, tailored to your business." },
        { icon: Eye, title: "Versatile design", description: "Works at any size — app icon, business card, signage, and everything in between." },
        { icon: FileImage, title: "All file formats", description: "SVG, AI, EPS, PNG, JPG — you get every format for print and digital." },
        { icon: Palette, title: "Colour variations", description: "Full colour, single colour, reversed, and monochrome versions included." },
        { icon: Layers, title: "Brand-ready", description: "Designed with your future brand identity in mind, not just as a standalone mark." },
        { icon: PenTool, title: "Revision rounds", description: "We refine until you love it. Up to 5 rounds of revisions on every project." },
      ]}
      process={[
        { step: "01", icon: Search, title: "Brief", description: "We learn about your business, values, audience, and competitors. You share examples of logos you love (and hate).", deliverables: ["Creative brief", "Competitor logo audit", "Style preferences"] },
        { step: "02", icon: PenTool, title: "Concepts", description: "We explore three distinct creative directions — each with a rationale. You pick your favourite direction.", deliverables: ["3 logo concepts", "Design rationale", "Presentation deck"] },
        { step: "03", icon: Palette, title: "Refine", description: "We refine the chosen concept through revision rounds. Colours, proportions, and details are perfected.", deliverables: ["Refined logo", "Colour versions", "Size test renders"] },
        { step: "04", icon: Rocket, title: "Deliver", description: "You receive production-ready files in every format, plus a simple usage guide.", deliverables: ["Vector files (SVG, AI, EPS)", "Raster files (PNG, JPG)", "Favicon & app icon", "Simple usage guide"] },
      ]}
      packages={[
        { name: "Essential", price: "From R1,800", description: "A clean, professional logo for your new business.", includes: ["2 initial concepts", "3 revision rounds", "Full colour version", "PNG & SVG files", "Favicon variant"], highlighted: false },
        { name: "Professional", price: "From R3,500", description: "Multiple concepts with full file package.", includes: ["3 initial concepts", "5 revision rounds", "Full colour & mono", "All vector formats (SVG, AI, EPS)", "All raster formats (PNG, JPG)", "Favicon & app icon", "Simple brand guide"], highlighted: true },
        { name: "Premium", price: "From R5,500", description: "Logo plus supporting brand elements.", includes: ["Everything in Professional", "Secondary logo mark", "Pattern / texture element", "Business card design", "Social media profile kit", "Comprehensive brand guide"], highlighted: false },
      ]}
      faqs={[
        { q: "How many concepts will I see?", a: "Depending on your package, you'll see 2–3 unique concepts. Each comes with a design rationale explaining the thinking behind it." },
        { q: "What if I don't like any of the concepts?", a: "We'll go back to the brief and explore new directions. We don't stop until you're happy — that's why we include revision rounds." },
        { q: "Can you redesign my existing logo?", a: "Yes! We can modernise, refine, or completely reimagine your current logo while keeping recognisable elements if desired." },
        { q: "Do I own the final logo?", a: "Yes. Full copyright transfers to you upon final payment. You own the logo outright." },
        { q: "How long does it take?", a: "Typically 1–2 weeks from brief to final delivery, depending on how quickly you provide feedback." },
      ]}
      ctaIcon={PenTool}
      ctaHeadline="Ready for a logo you'll love?"
      ctaDescription="Tell us about your business and we'll create something unique — delivered in under two weeks."
    />
  );
}
