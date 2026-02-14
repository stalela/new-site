import {
  Building2,
  ShieldCheck,
  Receipt,
  Landmark,
  Globe,
  FileText,
  RefreshCw,
  MapPin,
  Users,
  BadgeCheck,
  Award,
  Info,
  Mail,
  Palette,
  Briefcase,
  Code2,
  CreditCard,
  PenTool,
  Layers,
  Megaphone,
  Smartphone,
  Bot,
  MessageSquare,
  BookOpen,
  Search,
  RectangleEllipsis,
  type LucideIcon,
} from "lucide-react";

export interface ServiceItem {
  icon: LucideIcon;
  title: string;
  slug: string;
  description: string;
  longDescription: string;
  features: string[];
}

export interface ServiceCategory {
  name: string;
  slug: string;
  description: string;
  services: ServiceItem[];
}

/* ─── All services ─── */
export const allServices: ServiceItem[] = [
  {
    icon: Building2,
    slug: "company-registration",
    title: "Company Registration",
    description: "Register Pty Ltd, NPC, CC, and other entity types with CIPC.",
    longDescription: "We handle the full CIPC registration process — from name reservation to certificate delivery. Our team validates your director details, prepares your MOI, and submits everything on your behalf.",
    features: ["Name reservation (1–4 options)", "Pty Ltd, NPC, CC registration", "MOI preparation and filing", "Director and shareholder setup", "Registration certificate delivery", "Post-registration compliance triggers"],
  },
  {
    icon: Receipt,
    slug: "sars-services",
    title: "SARS Services",
    description: "Income tax, VAT registration, and ongoing SARS filing support.",
    longDescription: "From initial income tax registration to ongoing VAT and PAYE filing, we support your full SARS lifecycle through practitioner access to eFiling.",
    features: ["Income tax registration", "VAT registration and filing", "PAYE employer registration", "EMP201 and EMP501 submissions", "eFiling support via practitioner access", "Payment tracking and receipts"],
  },
  {
    icon: Briefcase,
    slug: "bizprofile",
    title: "BizProfile",
    description: "CIPC company profile management and updates.",
    longDescription: "Access and manage your CIPC company records through BizPortal. We help you keep your profile current — from director changes to registered office updates.",
    features: ["Company profile access", "Director appointment updates", "Registered office changes", "Company status queries", "Disclosure certificate requests"],
  },
  {
    icon: Globe,
    slug: "domain-name-registration",
    title: "Domain Name Registration",
    description: "Register .co.za and other domains for your business.",
    longDescription: "Secure your business identity online with a .co.za domain or other extensions. We handle the registration and DNS setup so your digital presence matches your brand.",
    features: [".co.za domain registration", ".com, .net, .org options", "DNS configuration", "Domain renewal management", "Email-ready domain setup"],
  },
  {
    icon: BadgeCheck,
    slug: "b-bbee-certificates",
    title: "B-BBEE Certificates",
    description: "B-BBEE affidavits and certificates for compliance and tenders.",
    longDescription: "Get your B-BBEE compliance documentation sorted — sworn affidavits for EMEs and QSEs, plus full verification for larger enterprises. Required for most government and corporate tenders.",
    features: ["EME sworn affidavits", "QSE certificates", "Full B-BBEE verification support", "Tender-ready documentation", "Annual renewal reminders"],
  },
  {
    icon: ShieldCheck,
    slug: "compensation-fund",
    title: "Compensation Fund",
    description: "COIDA registration and annual return of earnings.",
    longDescription: "Register with the Compensation Fund under COIDA, submit your annual return of earnings, and obtain your letter of good standing — required for tenders and compliance.",
    features: ["COIDA employer registration", "Annual return of earnings", "Letter of good standing", "Assessment payment support", "Compliance tracking"],
  },
  {
    icon: Users,
    slug: "uif-registration",
    title: "UIF Registration",
    description: "Unemployment Insurance Fund employer registration and declarations.",
    longDescription: "We register your business as a UIF employer and help you with monthly declarations. A legal requirement once you have employees.",
    features: ["Employer UIF registration", "Monthly declaration submissions", "Compliance status tracking", "Employee contribution calculations", "Department of Labour liaison"],
  },
  {
    icon: Landmark,
    slug: "business-bank-accounts",
    title: "Business Bank Accounts",
    description: "Document preparation and referral to banking partners.",
    longDescription: "We prepare your complete onboarding package — registration certificate, MOI, director IDs, and proof of address — then connect you with our banking partners for account opening.",
    features: ["Document bundle preparation", "Bank partner referrals (FNB, Standard Bank, Nedbank, Absa)", "Application status tracking", "Account linking support", "Secure document handling"],
  },
  {
    icon: RefreshCw,
    slug: "company-name-changes",
    title: "Company Name Changes",
    description: "CIPC name change applications and certificate updates.",
    longDescription: "Submit a name change application to CIPC, receive an updated registration certificate, and update all related records in one process.",
    features: ["Name availability check", "CIPC name change application", "Updated registration certificate", "MOI amendment support", "Downstream record updates"],
  },
  {
    icon: MapPin,
    slug: "company-address-changes",
    title: "Company / CC Address Changes",
    description: "Registered and business address amendments with CIPC.",
    longDescription: "Amend your registered, business, or postal address with CIPC for your company or close corporation. We submit the CM22 or CoR21.2 form on your behalf.",
    features: ["Registered address changes", "Business address amendments", "Postal address updates", "CM22 / CoR21.2 filing", "Confirmation of update"],
  },
  {
    icon: FileText,
    slug: "annual-return-filing",
    title: "Annual Return Filing",
    description: "CIPC annual return preparation, filing, and tracking.",
    longDescription: "Never miss your CIPC annual return deadline. We track your anniversary date, prepare the submission, handle payment, and file on your behalf.",
    features: ["Anniversary date tracking", "Annual return preparation", "CIPC payment processing", "Filing and confirmation", "Deregistration prevention"],
  },
  {
    icon: Award,
    slug: "nsbc-member-benefits",
    title: "NSBC Member Benefits",
    description: "Access National Small Business Chamber member resources.",
    longDescription: "Join the National Small Business Chamber for access to business resources, discounts on services, networking opportunities, and advocacy support.",
    features: ["Membership application", "Business resource access", "Service discounts", "Networking opportunities", "Advocacy and support"],
  },
  {
    icon: Info,
    slug: "ar-deregistration-letters",
    title: "AR Deregistration Letters",
    description: "Annual return deregistration objection letters and reinstatement.",
    longDescription: "If your company has been flagged for deregistration due to missed annual returns, we draft and submit objection letters and guide you through the reinstatement process.",
    features: ["Deregistration objection letters", "Outstanding annual return filing", "Reinstatement applications", "CIPC liaison", "Prevention of final deregistration"],
  },
  {
    icon: BadgeCheck,
    slug: "proudly-south-african",
    title: "Proudly South African",
    description: "Proudly SA membership applications and certification.",
    longDescription: "Apply for membership with the Proudly South African campaign. Certification demonstrates commitment to local content and gives access to the Proudly SA logo for marketing.",
    features: ["Membership application", "Certification process", "Logo usage rights", "Buy Local campaign access", "Annual renewal support"],
  },
  {
    icon: ShieldCheck,
    slug: "information-regulator-services",
    title: "Information Regulator Services",
    description: "POPIA compliance registration with the Information Regulator.",
    longDescription: "Register as a responsible party with the Information Regulator under POPIA. We help you understand your obligations and complete the registration process.",
    features: ["Information Officer registration", "Responsible party registration", "POPIA compliance guidance", "Registration confirmation", "Ongoing compliance support"],
  },
  {
    icon: Mail,
    slug: "business-email-setup",
    title: "Business Email Setup",
    description: "Professional email addresses on your own domain.",
    longDescription: "Set up professional email addresses using your business domain. We configure everything — DNS records, mailboxes, and forwarding — so you're ready to communicate.",
    features: ["Custom domain email (you@yourbusiness.co.za)", "Google Workspace or Microsoft 365 setup", "DNS and MX record configuration", "Multiple mailbox support", "Email forwarding and aliases"],
  },
  {
    icon: Globe,
    slug: "website-development",
    title: "Website Development",
    description: "Business websites and landing pages to establish your online presence.",
    longDescription: "Get a professional business website or landing page that represents your brand online. We design, build, and deploy fast, mobile-friendly sites.",
    features: ["Custom design and development", "Mobile-responsive layouts", "SEO-friendly structure", "Contact forms and lead capture", "Hosting and deployment"],
  },
  {
    icon: Palette,
    slug: "branding-and-design",
    title: "Branding & Design",
    description: "Complete brand identity — logo, colours, typography, and guidelines.",
    longDescription: "We craft a cohesive brand identity that tells your story. From logo concepts to a full brand guidelines document, your business will look professional across every touchpoint.",
    features: ["Logo design (3 concepts, revisions)", "Brand guidelines document", "Colour palette & typography", "Business card design", "Letterhead & templates", "Social media profile kit"],
  },
  {
    icon: PenTool,
    slug: "logo-design",
    title: "Logo Design",
    description: "Custom logo design with multiple concepts and revision rounds.",
    longDescription: "Your logo is the face of your business. We create unique, versatile logo designs that work across print, digital, signage, and merchandise — starting from a creative brief and ending with production-ready files.",
    features: ["Creative brief & mood board", "3 initial concepts", "Up to 5 revision rounds", "Full colour & mono versions", "Vector files (SVG, AI, EPS)", "PNG, JPG for web & social", "Favicon & app icon variants"],
  },
  {
    icon: RectangleEllipsis,
    slug: "business-cards",
    title: "Business Cards",
    description: "Professional business card design and print-ready files.",
    longDescription: "Make a lasting first impression with professionally designed business cards. We create designs that align with your brand identity and deliver print-ready files for any print shop.",
    features: ["Custom card design", "Front & back layout", "Brand-aligned typography", "Print-ready PDF & AI files", "Digital card version", "Multiple staff variants"],
  },
  {
    icon: Layers,
    slug: "graphic-design",
    title: "Graphic Design",
    description: "Flyers, banners, social media graphics, and marketing collateral.",
    longDescription: "From social media posts to exhibition banners, we design all the visual assets your business needs. Consistent, on-brand, and ready to publish or print.",
    features: ["Social media graphics", "Flyers & brochures", "Pull-up banners", "Presentation decks", "Email newsletter templates", "Infographics", "Print-ready & digital formats"],
  },
  {
    icon: Search,
    slug: "seo",
    title: "SEO",
    description: "Search engine optimisation to get your business found on Google.",
    longDescription: "We optimise your website so it ranks higher in Google search results. From technical SEO to content strategy and local listings, we help South African businesses get discovered online.",
    features: ["Technical SEO audit", "On-page optimisation", "Keyword research & strategy", "Google Business Profile setup", "Local SEO for SA businesses", "Monthly performance reports", "Competitor analysis"],
  },
  {
    icon: Megaphone,
    slug: "digital-marketing",
    title: "Digital Marketing",
    description: "Social media management, Google Ads, and online advertising.",
    longDescription: "Reach your customers where they spend their time — online. We manage social media campaigns, Google Ads, and content marketing to drive real leads and grow your business.",
    features: ["Social media management", "Google Ads campaigns", "Facebook & Instagram advertising", "Content calendar & scheduling", "Campaign analytics & reporting", "Lead generation funnels", "Email marketing setup"],
  },
  {
    icon: Code2,
    slug: "custom-software",
    title: "Custom Software",
    description: "Bespoke business systems, portals, and workflow automation.",
    longDescription: "When off-the-shelf doesn't cut it, we build custom software tailored to your business. Internal portals, client dashboards, booking systems, inventory management — built to scale with you.",
    features: ["Requirements analysis", "UI/UX design", "Full-stack development", "Database design", "API integrations", "Cloud deployment", "Ongoing support & maintenance", "Training & documentation"],
  },
  {
    icon: CreditCard,
    slug: "payment-integration",
    title: "Payment Integration",
    description: "Online payment gateways, invoicing, and e-commerce checkout.",
    longDescription: "Accept payments online with confidence. We integrate payment gateways like PayFast, Yoco, Stripe, and SnapScan into your website or app — secure, PCI-compliant, and ready for South African businesses.",
    features: ["PayFast integration", "Yoco setup", "Stripe / SnapScan support", "Recurring billing setup", "Invoice generation", "PCI compliance guidance", "Payment dashboard setup"],
  },
  {
    icon: Smartphone,
    slug: "mobile-apps",
    title: "Mobile Apps",
    description: "iOS and Android apps for your business or product.",
    longDescription: "Take your business mobile with a custom iOS and Android app. From customer-facing apps to internal tools, we design and develop native-quality mobile experiences.",
    features: ["iOS & Android development", "React Native / Flutter", "UI/UX design", "Push notifications", "App Store submission", "Backend & API development", "Post-launch updates"],
  },
  {
    icon: Bot,
    slug: "ai-agents",
    title: "AI Agents",
    description: "Intelligent automation agents that handle tasks for your business.",
    longDescription: "Automate repetitive work with AI agents. From data extraction and document processing to customer onboarding and lead qualification — we build intelligent agents that integrate with your existing tools.",
    features: ["Custom AI agent development", "Document processing & extraction", "Workflow automation", "CRM & tool integrations", "Natural language interfaces", "Monitoring & analytics dashboard", "Ongoing training & improvement"],
  },
  {
    icon: MessageSquare,
    slug: "chatbots",
    title: "Chatbots",
    description: "AI-powered chatbots for customer support and lead capture.",
    longDescription: "Deploy an AI chatbot on your website or WhatsApp to handle customer queries 24/7. Qualify leads, answer FAQs, book appointments, and route complex issues to your team — automatically.",
    features: ["Website chat widget", "WhatsApp Business integration", "AI-powered responses", "Lead qualification flows", "Appointment booking", "Handoff to human agents", "Conversation analytics", "Multi-language support"],
  },
  {
    icon: BookOpen,
    slug: "graphic-design-course",
    title: "Graphic Design Course",
    description: "Practical graphic design training for beginners and business teams.",
    longDescription: "Learn the fundamentals of professional graphic design through a guided, hands-on course. We cover visual hierarchy, typography, colour, social media layouts, and real-world design workflows so you can create polished business assets confidently.",
    features: ["Beginner-friendly modules", "Typography and colour fundamentals", "Canva and Adobe workflow basics", "Social media and marketing design projects", "Portfolio-ready assignments", "Certificate of completion"],
  },
];

/* ─── Helpers ─── */

export function getServiceBySlug(slug: string): ServiceItem | undefined {
  return allServices.find((s) => s.slug === slug);
}

export function getCategoryBySlug(slug: string): ServiceCategory | undefined {
  return serviceCategories.find((c) => c.slug === slug);
}

export function getCategoryForService(serviceSlug: string): ServiceCategory | undefined {
  return serviceCategories.find((c) => c.services.some((s) => s.slug === serviceSlug));
}

function buildCategory(
  name: string,
  slug: string,
  description: string,
  titles: string[],
): ServiceCategory {
  return {
    name,
    slug,
    description,
    services: titles.map(
      (title) => allServices.find((s) => s.title === title)!,
    ),
  };
}

/* ─── Categories ─── */

export const serviceCategories: ServiceCategory[] = [
  buildCategory(
    "Company Registration & Changes",
    "company-registration-and-changes",
    "Register new companies, update profiles, change names and addresses, and handle deregistration issues with CIPC.",
    ["Company Registration", "BizProfile", "Company Name Changes", "Company / CC Address Changes", "AR Deregistration Letters"],
  ),
  buildCategory(
    "Tax & Compliance",
    "tax-and-compliance",
    "SARS registrations, annual returns, UIF, Compensation Fund, and POPIA compliance with the Information Regulator.",
    ["SARS Services", "Annual Return Filing", "UIF Registration", "Compensation Fund", "Information Regulator Services"],
  ),
  buildCategory(
    "Banking & Finance",
    "banking-and-finance",
    "Business bank account onboarding — document preparation and referral to banking partners.",
    ["Business Bank Accounts"],
  ),
  buildCategory(
    "Certifications & Memberships",
    "certifications-and-memberships",
    "B-BBEE certificates, NSBC membership, and Proudly South African certification for tenders and credibility.",
    ["B-BBEE Certificates", "NSBC Member Benefits", "Proudly South African"],
  ),
  buildCategory(
    "Tech & Digital Services",
    "tech-and-digital-services",
    "Full-service digital studio — websites, branding, apps, software, marketing, AI, and everything in between.",
    [
      "Website Development",
      "Branding & Design",
      "Logo Design",
      "Business Cards",
      "Graphic Design",
      "Graphic Design Course",
      "SEO",
      "Digital Marketing",
      "Custom Software",
      "Payment Integration",
      "Mobile Apps",
      "AI Agents",
      "Chatbots",
      "Domain Name Registration",
      "Business Email Setup",
    ],
  ),
];
