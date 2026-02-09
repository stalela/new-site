# Copilot Agent Prompt: Build Stalela Marketing Site (Next.js)

## Objective

Build the **Stalela marketing website** using Next.js.

This is NOT the core application.  
This is a marketing and lead‑capture site only.

The goal is to:

- Explain what Stalela does
- Build trust with SMEs and accountants
- Capture leads for company registration and compliance services
- Provide a clean, credible public presence

Do not build internal dashboards or automation systems yet.

---

## Tech Stack

- Next.js (App Router)
- TypeScript
- Tailwind CSS
- shadcn/ui
- Vercel deployment target
- Simple form storage (API route → database or email)

No heavy backend logic.

---

## Repo Structure

```
stalela/
  apps/
    marketing-site/
  packages/
    ui/
  docs/
```

All work happens inside:

```
apps/marketing-site
```

---

## Hard Constraints

You must follow these rules:

- Do NOT build authentication
- Do NOT build dashboards
- Do NOT build compliance engines
- Do NOT build banking integrations
- Do NOT build APIs beyond simple lead capture
- Do NOT invent features outside marketing scope
- Do NOT generate placeholder copy unless instructed

Only build marketing UI and lead forms.

---

## Brand Positioning

Stalela is:

A business operating layer for SMEs in South Africa.

We help businesses:

- Register companies
- Stay compliant
- Manage admin
- Set up banking
- Avoid penalties

Tone:

- Clear
- Practical
- Trustworthy
- No hype
- No buzzwords

---

## Pages to Build

### Home (/)

Sections:

1. Hero
2. Services overview
3. How it works
4. Pricing preview
5. Trust indicators
6. CTA
7. Footer

Primary CTA:
“Register your business”

---

### Services (/services)

Sections:

- Company registration
- Compliance management
- Tax + payroll support
- Banking setup

Each section must include CTA.

---

### How It Works (/how-it-works)

Steps:

1. Submit details
2. We register your company
3. We keep you compliant
4. You focus on business

---

### Pricing (/pricing)

Simple tiers:

Starter  
Growth  
Managed  

Static content for now.

---

### Contact (/contact)

Form fields:

- Name
- Email
- Phone
- Service needed
- Message

Submit to API route.

---

### Register (/register)

Lead form for business registration.

Fields:

- Full name
- Email
- Phone
- Business name
- Directors count
- Notes

---

## Components to Create

Navbar  
Footer  
Hero  
Section  
CTA  
PricingTable  
LeadForm  
Container  
Button  
Card  

All reusable.

---

## Design Rules

- Clean layout
- Large typography
- White background
- Subtle color accents
- No heavy animation
- Mobile-first
- Accessible

---

## Copy Rules

Do NOT invent marketing claims.

Use simple copy:

Headline:
"Run your business. We handle the admin."

Subheadline:
"Company registration, compliance, and banking support for South African businesses."

CTA:
"Get started"

---

## Forms

Forms should:

- Validate input
- Send POST request to `/api/lead`
- Store data (temporary storage OK)
- Show success message

No auth required.

---

## Folder Structure

```
app/
  layout.tsx
  page.tsx
  services/page.tsx
  pricing/page.tsx
  contact/page.tsx
  register/page.tsx

components/
  Navbar.tsx
  Footer.tsx
  Hero.tsx
  Section.tsx
  CTA.tsx
  LeadForm.tsx
```

---

## Agent Behavior Rules

You must:

- Follow docs exactly
- Only build requested pages
- Use reusable components
- Keep code clean
- Avoid unnecessary dependencies
- Avoid speculative features

If unclear, default to simplicity.

---

## Deliverables

1. Next.js marketing site
2. All pages implemented
3. Reusable components
4. Lead capture form working
5. Ready for deployment

Stop after marketing site is complete.

Do NOT start building the internal app.
