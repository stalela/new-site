"use client";

import { Bot, Brain, Cog, Zap, Shield, BarChart3, Eye, Rocket, Database, Sparkles, Workflow } from "lucide-react";
import { StudioPage } from "../StudioPage";

export function AIAgentsContent() {
  return (
    <StudioPage
      serviceSlug="ai-agents"
      badge="Stalela Studio"
      badgeIcon={Bot}
      headline="AI agents that work"
      headlineAccent="while you sleep"
      description="Autonomous AI agents that handle repetitive tasks, process documents, monitor systems, and make decisions based on your rules. Built for South African businesses ready to automate intelligently — not just add a chatbot."
      heroImage="/images/generated/studio/ai-hero.png"
      heroImageAlt="AI agent workflow system"
      gallery={[
        { src: "/images/generated/studio/ai-hero.png", alt: "AI agent dashboard", caption: "Intelligent automation for your business" },
        { src: "/images/generated/studio/ai-workflow.png", alt: "AI workflow orchestration", caption: "Multi-step workflows with human-in-the-loop controls" },
      ]}
      galleryTitle="Automation with intelligence"
      galleryDescription="Not just rules — agents that understand context and make smart decisions."
      features={[
        { icon: Brain, title: "Document processing", description: "Extract data from invoices, contracts, and forms. Classify, summarise, and route documents automatically." },
        { icon: Workflow, title: "Workflow automation", description: "Multi-step business processes that run autonomously with human approval gates for critical decisions." },
        { icon: Database, title: "Data enrichment", description: "Automatically research, validate, and enrich your business data from multiple sources." },
        { icon: Zap, title: "System monitoring", description: "AI-powered alerts that understand context — not just thresholds, but anomalies and trends." },
        { icon: Shield, title: "Human-in-the-loop", description: "AI handles the 80%, escalates the 20% that needs human judgement. Full audit trails." },
        { icon: Cog, title: "Custom integrations", description: "Agents that connect to your existing tools — email, CRM, accounting, government portals." },
      ]}
      process={[
        { step: "01", icon: Eye, title: "Identify", description: "We audit your business processes to find the highest-value automation opportunities. Not everything should be automated — we focus on what matters.", deliverables: ["Process audit", "Automation opportunity map", "ROI analysis", "Priority ranking"] },
        { step: "02", icon: Brain, title: "Design", description: "We architect the agent system — what it monitors, what it does, when it escalates, and how it learns.", deliverables: ["Agent architecture", "Decision rules", "Escalation flows", "Integration plan"] },
        { step: "03", icon: Cog, title: "Build", description: "We build, test, and train the agents. Real data, real scenarios, with human oversight until confidence is high.", deliverables: ["Working agents", "Training & testing", "Monitoring dashboard", "Documentation"] },
        { step: "04", icon: Rocket, title: "Deploy", description: "Agents go live with guardrails. We monitor, tune, and expand scope as trust builds.", deliverables: ["Production deployment", "Performance monitoring", "Tuning & optimisation", "Expansion roadmap"] },
      ]}
      techStack={[
        "OpenAI GPT-4", "Claude", "LangChain", "LangGraph",
        "Python", "TypeScript", "PostgreSQL", "Redis",
        "Vector databases", "Webhook APIs", "n8n", "Make",
      ]}
      packages={[
        { name: "Starter Agent", price: "From R15,000", description: "A single-purpose automation agent.", includes: ["1 automated workflow", "Document processing or monitoring", "Basic decision logic", "Email notifications", "Dashboard access", "3 months support"], highlighted: false },
        { name: "Business Automation", price: "From R35,000", description: "Multi-agent system for complex processes.", includes: ["Up to 3 automated workflows", "Multi-step decision chains", "Human approval gates", "CRM / email / accounting integration", "Custom dashboard", "6 months support & tuning"], highlighted: true },
        { name: "Enterprise AI", price: "Custom", description: "Organisation-wide intelligent automation.", includes: ["Everything in Business", "Unlimited workflows", "Custom AI model fine-tuning", "Advanced analytics & reporting", "Multi-department deployment", "Dedicated AI engineer", "12 months SLA"], highlighted: false },
      ]}
      faqs={[
        { q: "What's the difference between AI agents and chatbots?", a: "Chatbots respond to user messages. AI agents work autonomously — processing data, making decisions, and taking actions without waiting for someone to ask. Think of agents as employees, chatbots as receptionists." },
        { q: "Is this safe? What if the AI makes a mistake?", a: "We build human-in-the-loop controls into every agent. Critical decisions require human approval. All actions are logged and auditable. You set the guardrails." },
        { q: "What kind of tasks can AI agents handle?", a: "Document processing, data entry, email triage, lead qualification, compliance monitoring, report generation, system alerts, data enrichment — any repetitive knowledge work." },
        { q: "Do you use our data to train AI models?", a: "No. Your data stays private. We use commercial AI APIs with data processing agreements, or can deploy models on your own infrastructure." },
        { q: "What's the ROI?", a: "Most clients see 5–10x return within 6 months by eliminating manual processing hours. We build a business case with expected savings before starting." },
      ]}
      ctaIcon={Bot}
      ctaHeadline="Ready to put AI to work?"
      ctaDescription="Book a free automation audit and discover the tasks that AI agents can handle for your business."
    />
  );
}
