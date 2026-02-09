"use client";

import { RectangleEllipsis, Palette, Printer, Sparkles, Eye, Layers, Rocket, PenTool, FileImage } from "lucide-react";
import { StudioPage } from "../StudioPage";

export function BusinessCardsContent() {
  return (
    <StudioPage
      serviceSlug="business-cards"
      badge="Stalela Studio"
      badgeIcon={RectangleEllipsis}
      headline="Business cards that leave an"
      headlineAccent="impression"
      description="In South Africa, the business card is still king. Whether you're networking at a conference or closing a deal, a well-designed card says you mean business. We design cards that people keep, not toss."
      heroImage="/images/generated/studio/cards-hero.png"
      heroImageAlt="Premium business card designs"
      gallery={[
        { src: "/images/generated/studio/cards-hero.png", alt: "Premium business cards on desk", caption: "Cards that feel as good as they look" },
        { src: "/images/generated/studio/cards-styles.png", alt: "Various card styles and finishes", caption: "From minimalist to bold — find your style" },
      ]}
      galleryTitle="Print makes permanent"
      galleryDescription="Premium finishes and bold designs that people actually keep."
      features={[
        { icon: Sparkles, title: "Premium finishes", description: "Spot UV, foil stamping, embossing, matte lamination — elevate your first impression." },
        { icon: Palette, title: "Custom design", description: "Designed to match your brand identity, not pulled from a template gallery." },
        { icon: Printer, title: "Print-ready files", description: "CMYK, 300dpi, bleed marks — files that any printer can use, locally or internationally." },
        { icon: Eye, title: "Double-sided", description: "Front and back designs that work together to maximise the space." },
        { icon: Layers, title: "Multiple variants", description: "Different cards for directors, sales, and support — all on-brand." },
        { icon: FileImage, title: "Digital version", description: "A digital card image for WhatsApp, email signatures, and LinkedIn." },
      ]}
      process={[
        { step: "01", icon: PenTool, title: "Design brief", description: "Share your logo, brand colours, and contact details. Tell us your style preferences — classic, modern, bold, or minimalist.", deliverables: ["Creative brief", "Brand asset review"] },
        { step: "02", icon: Palette, title: "Concepts", description: "We present 2–3 card designs in realistic mockups so you can see exactly how they'll look in hand.", deliverables: ["2–3 card designs", "Realistic mockups"] },
        { step: "03", icon: Sparkles, title: "Refine", description: "Pick your favourite and we'll refine every detail — typography, spacing, alignment, finish recommendations.", deliverables: ["Refined design", "Finish recommendations"] },
        { step: "04", icon: Rocket, title: "Deliver", description: "You receive print-ready files and digital versions. We can also coordinate with your printer if needed.", deliverables: ["Print-ready PDF (CMYK)", "PNG / JPG for digital", "Printer specifications sheet"] },
      ]}
      packages={[
        { name: "Standard", price: "From R800", description: "A professional card design for your business.", includes: ["1 design concept", "2 revision rounds", "Single-sided or double-sided", "Print-ready PDF", "Digital card (PNG)"], highlighted: false },
        { name: "Professional", price: "From R1,500", description: "Multiple concepts with premium finish options.", includes: ["2 design concepts", "3 revision rounds", "Double-sided design", "Premium finish recommendations", "Print-ready PDF (CMYK)", "Digital card (PNG & JPG)", "Printer spec sheet"], highlighted: true },
        { name: "Team Pack", price: "From R2,500", description: "Consistent cards for your whole team.", includes: ["Everything in Professional", "Up to 5 name variants", "Editable template (Canva or AI)", "QR code integration", "WhatsApp-ready digital cards"], highlighted: false },
      ]}
      faqs={[
        { q: "Can you print the cards too?", a: "We focus on design, but we work with trusted local printers and can coordinate the printing for you at competitive rates." },
        { q: "What size are the cards?", a: "Standard South African business cards are 90×50mm. We can also do US size (89×51mm) or custom sizes." },
        { q: "Can I add a QR code?", a: "Absolutely. We can add QR codes linking to your website, vCard, WhatsApp, or any URL." },
        { q: "Do you design letterheads and envelopes too?", a: "Yes! We offer a full stationery suite. Ask about our Branding & Design package for a complete set." },
        { q: "How quickly can I get my design?", a: "Typically 3–5 business days from brief to final delivery." },
      ]}
      ctaIcon={RectangleEllipsis}
      ctaHeadline="Need cards that make people look twice?"
      ctaDescription="Get a professional business card design delivered in under a week."
    />
  );
}
