"use client";

import { MessageSquare, Bot, Zap, Globe, BarChart3, Shield, Eye, Rocket, Settings, Clock, Users } from "lucide-react";
import { StudioPage } from "../StudioPage";

export function ChatbotsContent() {
  return (
    <StudioPage
      serviceSlug="chatbots"
      badge="Stalela Studio"
      badgeIcon={MessageSquare}
      headline="Customer support that"
      headlineAccent="never sleeps"
      description="AI-powered chatbots that answer questions, qualify leads, book appointments, and handle support — 24/7, in English, Afrikaans, Zulu, and more. Integrated into your website, WhatsApp, or Facebook Messenger."
      heroImage="/images/generated/studio/chatbot-hero.png"
      heroImageAlt="AI chatbot interface"
      gallery={[
        { src: "/images/generated/studio/chatbot-hero.png", alt: "Chatbot conversation interface", caption: "Natural conversations that solve real problems" },
        { src: "/images/generated/studio/chatbot-features.png", alt: "Chatbot features overview", caption: "Multi-channel, multilingual, available 24/7" },
      ]}
      galleryTitle="Smart conversations, real results"
      galleryDescription="Not the annoying chatbots of 2018. These ones actually help."
      features={[
        { icon: Bot, title: "AI-powered", description: "Understands natural language, context, and intent. Handles complex questions, not just keyword matching." },
        { icon: Globe, title: "Multilingual", description: "English, Afrikaans, Zulu, Xhosa, Sotho — serve your customers in their language." },
        { icon: MessageSquare, title: "Multi-channel", description: "Website widget, WhatsApp Business, Facebook Messenger, Instagram DMs — all from one brain." },
        { icon: Clock, title: "24/7 availability", description: "Never miss a lead or leave a customer waiting. Instant responses, any time, any day." },
        { icon: Users, title: "Lead qualification", description: "Ask the right questions, score leads, and route hot prospects to your sales team immediately." },
        { icon: Shield, title: "Human handover", description: "Seamless escalation to a real person when the bot reaches its limits. No dead ends." },
      ]}
      process={[
        { step: "01", icon: Eye, title: "Map conversations", description: "We analyse your most common customer questions, support tickets, and sales conversations to design the chatbot's knowledge.", deliverables: ["FAQ analysis", "Conversation flow map", "Intent classification", "Handover triggers"] },
        { step: "02", icon: Settings, title: "Build & train", description: "We build the bot, train it on your content, and fine-tune responses until they sound like your team — not a robot.", deliverables: ["Chatbot development", "Knowledge base training", "Response tuning", "Personality & tone setup"] },
        { step: "03", icon: Zap, title: "Integrate", description: "Deploy to your chosen channels — website, WhatsApp, Messenger. Connect to your CRM and booking system.", deliverables: ["Channel integration", "CRM connection", "Booking system sync", "Widget customisation"] },
        { step: "04", icon: Rocket, title: "Launch & learn", description: "Go live with monitoring. The bot improves weekly based on real conversations and feedback.", deliverables: ["Live deployment", "Conversation analytics", "Weekly improvement reports", "Ongoing training"] },
      ]}
      techStack={[
        "OpenAI GPT-4", "Claude", "WhatsApp Business API", "Facebook Messenger",
        "Intercom", "Crisp", "Botpress", "Voiceflow",
        "Zapier", "Make", "Custom APIs",
      ]}
      packages={[
        { name: "FAQ Bot", price: "From R8,000", description: "Answer common questions automatically.", includes: ["Website chatbot widget", "Up to 50 FAQ answers", "Basic lead capture", "Business hours awareness", "Setup & training", "3 months support"], highlighted: false },
        { name: "Smart Bot", price: "From R18,000", description: "AI-powered conversations across channels.", includes: ["Website + 1 messaging channel", "AI-powered responses (GPT-4)", "Lead qualification & scoring", "Appointment booking", "CRM integration", "Human handover", "Multilingual (2 languages)", "6 months support & training"], highlighted: true },
        { name: "Enterprise Bot", price: "Custom", description: "Full-scale conversational AI.", includes: ["Everything in Smart Bot", "All messaging channels", "Unlimited languages", "Advanced analytics dashboard", "Custom API integrations", "A/B conversation testing", "Dedicated bot trainer", "12 months SLA"], highlighted: false },
      ]}
      faqs={[
        { q: "Will the chatbot sound robotic?", a: "No. We train it to match your brand voice — professional, friendly, casual, whatever fits. Modern AI chatbots sound natural and handle nuance well." },
        { q: "Can it handle WhatsApp?", a: "Yes. We integrate with the official WhatsApp Business API for reliable, scalable messaging. Your customers can reach you on the platform they already use daily." },
        { q: "What if the bot can't answer a question?", a: "It seamlessly hands over to a human agent with full context of the conversation. No frustrating dead ends for your customers." },
        { q: "How do you handle multiple languages?", a: "The bot auto-detects language and responds accordingly. We train it on South African English, Afrikaans, and any other languages you need." },
        { q: "Does it integrate with my existing systems?", a: "Yes. We connect to CRMs (HubSpot, Salesforce), booking systems (Calendly), help desks (Zendesk, Freshdesk), and custom APIs." },
      ]}
      ctaIcon={MessageSquare}
      ctaHeadline="Ready to automate your customer conversations?"
      ctaDescription="Let's build a chatbot that handles the repetitive questions so your team can focus on closing deals."
    />
  );
}
