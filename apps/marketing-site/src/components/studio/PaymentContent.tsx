"use client";

import { CreditCard, Shield, Globe, Sparkles, BarChart3, Settings, Eye, Rocket, Lock, ArrowLeftRight, Zap } from "lucide-react";
import { StudioPage } from "../StudioPage";

export function PaymentContent() {
  return (
    <StudioPage
      serviceSlug="payment-integration"
      badge="Stalela Studio"
      badgeIcon={CreditCard}
      headline="Get paid online —"
      headlineAccent="seamlessly"
      description="Accept card payments, EFT, SnapScan, mobile money, and subscriptions. We integrate South African payment gateways into your website, app, or system — securely and with zero downtime."
      heroImage="/images/generated/studio/payment-hero.png"
      heroImageAlt="Payment gateway integration"
      gallery={[
        { src: "/images/generated/studio/payment-hero.png", alt: "Payment integration dashboard", caption: "Accept every payment method your customers use" },
        { src: "/images/generated/studio/payment-gateways.png", alt: "South African payment gateways", caption: "Integrated with every major SA payment provider" },
      ]}
      galleryTitle="Every way to pay, one integration"
      galleryDescription="From card payments to EFT to mobile wallets — we handle the complexity."
      features={[
        { icon: CreditCard, title: "Card payments", description: "Visa, Mastercard, and Amex via PayFast, Peach Payments, Paystack, or Stripe." },
        { icon: ArrowLeftRight, title: "EFT & instant EFT", description: "Bank transfers via Ozow, Stitch, or PayFast instant EFT — popular with SA customers." },
        { icon: Zap, title: "SnapScan & mobile", description: "QR code payments and mobile wallets for in-store and online purchases." },
        { icon: Lock, title: "Subscription billing", description: "Recurring payments with automated retries, dunning, and plan management." },
        { icon: Shield, title: "PCI compliance", description: "Tokenised card storage, 3D Secure, and PCI DSS compliance — your customers' data is safe." },
        { icon: BarChart3, title: "Reconciliation", description: "Automated payment matching, refund management, and financial reporting." },
      ]}
      process={[
        { step: "01", icon: Eye, title: "Assessment", description: "We review your business model, transaction volumes, and requirements to recommend the best payment stack.", deliverables: ["Payment needs assessment", "Gateway comparison", "Cost analysis"] },
        { step: "02", icon: Settings, title: "Setup", description: "We handle merchant applications, gateway account setup, and API configuration. No paperwork headaches.", deliverables: ["Merchant account setup", "Gateway configuration", "API keys & credentials"] },
        { step: "03", icon: Globe, title: "Integrate", description: "We build the integration into your website, app, or system — checkout flows, webhooks, and error handling.", deliverables: ["Payment checkout flow", "Webhook handlers", "Error handling & retries", "Test transactions"] },
        { step: "04", icon: Rocket, title: "Go live", description: "After thorough testing, we switch to live mode. We monitor the first transactions and ensure everything flows smoothly.", deliverables: ["Live deployment", "Transaction monitoring", "Documentation", "Support handover"] },
      ]}
      techStack={[
        "PayFast", "Peach Payments", "Paystack", "Stripe",
        "Ozow", "Stitch", "SnapScan", "Yoco",
        "Webhook handlers", "3D Secure", "PCI DSS",
      ]}
      packages={[
        { name: "Basic Integration", price: "From R5,500", description: "Single gateway integration for your website.", includes: ["1 payment gateway", "Card payment support", "Checkout page integration", "Success / failure handling", "Basic webhook setup", "Test & go-live support"], highlighted: false },
        { name: "Full Commerce", price: "From R12,000", description: "Multi-method payments for e-commerce.", includes: ["2+ payment methods", "Card, EFT, and SnapScan", "Subscription / recurring billing", "Refund management", "Payment notification emails", "Reconciliation dashboard", "3D Secure setup"], highlighted: true },
        { name: "Enterprise", price: "Custom", description: "Complex payment orchestration.", includes: ["Everything in Full Commerce", "Multi-gateway failover", "Split payments / marketplace", "Custom checkout flows", "Advanced fraud detection", "Financial reporting integration", "Dedicated support"], highlighted: false },
      ]}
      faqs={[
        { q: "Which payment gateway is best for SA?", a: "It depends on your business. PayFast is great for general e-commerce, Peach Payments for subscriptions, Ozow for instant EFT, and Yoco for in-person. We'll recommend the right fit." },
        { q: "How long does integration take?", a: "Basic integrations: 1–2 weeks. Full commerce setups: 2–4 weeks. Enterprise: 4–8 weeks depending on complexity." },
        { q: "Do I need a merchant account?", a: "Some gateways (like PayFast) handle this for you. Others require a separate merchant account. We'll guide you through the process." },
        { q: "What about transaction fees?", a: "Fees depend on the gateway and your volumes. Typical SA rates: 2.5–3.5% for cards, 1–2% for EFT. We help negotiate the best rates." },
        { q: "Can you integrate with my accounting software?", a: "Yes. We can sync payments with Xero, Sage, QuickBooks, and other accounting platforms." },
      ]}
      ctaIcon={CreditCard}
      ctaHeadline="Ready to accept payments?"
      ctaDescription="Let's find the right payment solution for your business and get it live in weeks."
    />
  );
}
