"use client";

import { Smartphone, Palette, Zap, Shield, Cloud, BarChart3, Eye, Rocket, Code2, Settings, Download } from "lucide-react";
import { StudioPage } from "../StudioPage";

export function MobileAppsContent() {
  return (
    <StudioPage
      serviceSlug="mobile-apps"
      badge="Stalela Studio"
      badgeIcon={Smartphone}
      headline="Apps your customers will"
      headlineAccent="actually use"
      description="We build cross-platform mobile apps for iOS and Android from a single codebase. Whether you need a customer-facing app, internal field tool, or marketplace — we ship fast, polished, and performant."
      heroImage="/images/generated/studio/apps-hero.png"
      heroImageAlt="Mobile app development"
      gallery={[
        { src: "/images/generated/studio/apps-hero.png", alt: "Mobile app showcase", caption: "Beautiful, performant apps for iOS and Android" },
        { src: "/images/generated/studio/apps-process.png", alt: "App development process", caption: "From wireframe to app store in weeks" },
      ]}
      galleryTitle="From idea to app store"
      galleryDescription="Cross-platform development that doesn't compromise on quality."
      features={[
        { icon: Smartphone, title: "Cross-platform", description: "One codebase, two platforms. React Native or Flutter apps for iOS and Android simultaneously." },
        { icon: Palette, title: "Custom UI/UX", description: "Platform-native feel with custom animations, gestures, and interactions. No cookie-cutter templates." },
        { icon: Zap, title: "Offline support", description: "Apps that work without internet — critical for South African field workers and areas with poor connectivity." },
        { icon: Cloud, title: "Cloud backend", description: "Scalable APIs, real-time sync, push notifications, and cloud storage — all managed." },
        { icon: Shield, title: "Secure by default", description: "Biometric auth, encrypted storage, certificate pinning, and POPIA-compliant data handling." },
        { icon: Download, title: "App Store deployment", description: "We handle Apple App Store and Google Play submissions, screenshots, descriptions, and approvals." },
      ]}
      process={[
        { step: "01", icon: Eye, title: "Discovery", description: "We validate your app concept, define the user journey, and identify the minimum feature set that delivers value.", deliverables: ["User journey map", "Feature prioritisation", "Technical feasibility", "Project estimate"] },
        { step: "02", icon: Palette, title: "Design", description: "Interactive prototypes you can tap through on your phone. We test with real users before writing code.", deliverables: ["UI/UX wireframes", "Interactive prototype", "Design system", "User testing results"] },
        { step: "03", icon: Code2, title: "Develop", description: "Agile sprints with working builds every two weeks. You test on your own device throughout development.", deliverables: ["TestFlight / Beta builds", "Sprint demos", "API development", "Automated tests"] },
        { step: "04", icon: Rocket, title: "Launch", description: "App Store and Play Store submission, launch marketing assets, and post-launch monitoring.", deliverables: ["Store submissions", "App screenshots & description", "Launch monitoring", "User analytics setup"] },
      ]}
      techStack={[
        "React Native", "Flutter", "TypeScript", "Dart",
        "Expo", "Firebase", "Supabase", "Node.js",
        "PostgreSQL", "Push Notifications", "App Store Connect", "Google Play Console",
      ]}
      packages={[
        { name: "MVP App", price: "From R55,000", description: "Validate your idea with a polished MVP.", includes: ["Core feature development", "iOS + Android", "Custom UI design", "Basic backend / API", "Push notifications", "App Store submission", "3 months bug-fix support"], highlighted: false },
        { name: "Business App", price: "From R120,000", description: "A fully featured app for your business.", includes: ["Everything in MVP", "Advanced features & integrations", "Offline support", "User authentication & profiles", "Admin dashboard (web)", "Analytics integration", "6 months support"], highlighted: true },
        { name: "Enterprise", price: "Custom", description: "Complex apps for large user bases.", includes: ["Everything in Business", "Multi-tenant architecture", "Advanced security & compliance", "CI/CD pipeline", "Load testing", "Custom backend infrastructure", "Dedicated team", "12 months SLA"], highlighted: false },
      ]}
      faqs={[
        { q: "React Native or Flutter — which is better?", a: "Both are excellent. We recommend React Native if you have existing web tech in React/TypeScript, and Flutter for design-heavy apps. We'll advise based on your project." },
        { q: "Will my app work offline?", a: "Yes, if needed. We build offline-first apps using local storage and background sync — especially important for SA with intermittent connectivity." },
        { q: "How do app store submissions work?", a: "We handle everything — Apple App Store and Google Play accounts, screenshots, descriptions, privacy policies, and the review process." },
        { q: "Can you update my existing app?", a: "Yes. We can maintain, update, and add features to existing React Native, Flutter, or native iOS/Android apps." },
        { q: "What about ongoing maintenance?", a: "Apps need regular updates for OS compatibility, security patches, and new features. We offer monthly maintenance plans from R5,000/month." },
      ]}
      ctaIcon={Smartphone}
      ctaHeadline="Ready to build your app?"
      ctaDescription="Book a free discovery call and we'll help you define the fastest path from idea to app store."
    />
  );
}
