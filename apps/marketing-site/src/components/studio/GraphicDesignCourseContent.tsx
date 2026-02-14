"use client";

import {
  BookOpen,
  Palette,
  PenTool,
  Layers,
  Eye,
  Sparkles,
  Rocket,
  GraduationCap,
  FileImage,
  Monitor,
  CheckCircle2,
} from "lucide-react";
import { StudioPage } from "../StudioPage";

export function GraphicDesignCourseContent() {
  return (
    <StudioPage
      serviceSlug="graphic-design-course"
      badge="Stalela Studio"
      badgeIcon={GraduationCap}
      headline="Learn graphic design with"
      headlineAccent="real projects"
      description="Our Graphic Design Course is built for South African entrepreneurs, students, and teams who want practical design skills fast. You’ll learn by doing — from social posts and flyers to branded marketing assets."
      heroImage="/images/generated/studio/graphic-hero.png"
      heroImageAlt="Graphic design course workspace and learning materials"
      gallery={[
        { src: "/images/generated/studio/graphic-hero.png", alt: "Graphic design studio setup", caption: "Hands-on learning with real business design briefs" },
        { src: "/images/generated/studio/graphic-portfolio.png", alt: "Student-style design portfolio examples", caption: "Build portfolio-ready work across digital and print" },
        { src: "/images/generated/studio/branding-process.png", alt: "Brand design process board", caption: "Understand the full design process from concept to delivery" },
      ]}
      galleryTitle="Train on industry-style design work"
      galleryDescription="We reuse the same visual standards used in our studio service delivery."
      features={[
        { icon: BookOpen, title: "Structured modules", description: "Step-by-step lessons covering design basics through to practical campaign work." },
        { icon: Palette, title: "Design foundations", description: "Master typography, colour, layout, hierarchy, and composition." },
        { icon: Monitor, title: "Tool workflows", description: "Learn practical workflows in Canva plus Adobe-friendly design thinking." },
        { icon: FileImage, title: "Business asset creation", description: "Create social media posts, flyers, banners, and presentation visuals." },
        { icon: Layers, title: "Portfolio projects", description: "Complete guided projects you can show to clients or employers." },
        { icon: CheckCircle2, title: "Completion certificate", description: "Receive a course completion certificate after final project review." },
      ]}
      process={[
        { step: "01", icon: Eye, title: "Orientation", description: "We assess your level, goals, and preferred learning outcomes so your course path is practical and relevant.", deliverables: ["Skill baseline", "Learning plan", "Project brief pack"] },
        { step: "02", icon: PenTool, title: "Core training", description: "You work through guided lessons with clear examples and mini assignments for each concept.", deliverables: ["Module worksheets", "Design exercises", "Instructor feedback"] },
        { step: "03", icon: Sparkles, title: "Project build", description: "Apply what you learned to full design projects used in real business marketing contexts.", deliverables: ["Social campaign set", "Flyer/poster design", "Brand visual set"] },
        { step: "04", icon: Rocket, title: "Final review", description: "We review your final pieces, polish your outputs, and issue completion confirmation.", deliverables: ["Portfolio-ready files", "Improvement notes", "Completion certificate"] },
      ]}
      packages={[
        { name: "Starter Course", price: "From R1,200", description: "Great for beginners learning design fundamentals.", includes: ["4 core modules", "Guided exercises", "2 project briefs", "Certificate of completion"], highlighted: false },
        { name: "Professional Course", price: "From R2,800", description: "Comprehensive practical training with portfolio focus.", includes: ["8 full modules", "Weekly feedback", "6 project briefs", "Portfolio review", "Certificate of completion"], highlighted: true },
        { name: "Team Training", price: "From R6,500", description: "Group training for internal teams needing consistent design capability.", includes: ["Up to 8 participants", "Custom business-focused modules", "Shared brand template setup", "Final team project", "Certificates for all participants"], highlighted: false },
      ]}
      faqs={[
        { q: "Is this course beginner friendly?", a: "Yes. The course starts from fundamentals and builds up to practical design projects." },
        { q: "Do I need Adobe software?", a: "No. You can start with Canva. We also teach design principles that transfer to Adobe tools." },
        { q: "How long does the course take?", a: "Most learners complete Starter in 2–3 weeks and Professional in 4–6 weeks." },
        { q: "Will I build a portfolio?", a: "Yes. Each module includes project work designed to become portfolio-ready pieces." },
        { q: "Can businesses enrol teams?", a: "Absolutely. The Team Training package is designed for internal marketing or admin teams." },
      ]}
      ctaIcon={GraduationCap}
      ctaHeadline="Ready to learn graphic design properly?"
      ctaDescription="Join the Graphic Design Course and start producing professional visuals for your business."
    />
  );
}
