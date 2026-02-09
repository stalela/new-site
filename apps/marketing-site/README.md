# Stalela Marketing Site

A modern, multilingual marketing website for Stalela, a South African SME compliance platform. Built with Next.js 16, featuring internationalization support for English, isiZulu, Afrikaans, and isiXhosa.

## Overview

This site serves as the public face of Stalela, providing information about our compliance services, capturing leads, and building trust with potential clients. The site is optimized for performance and SEO, with static generation for all pages.

## Tech Stack

- **Framework**: Next.js 16 (App Router)
- **Language**: TypeScript
- **Styling**: Tailwind CSS v4
- **Internationalization**: next-intl
- **Icons**: Lucide React
- **Fonts**: Geist Sans & Geist Mono
- **Deployment**: Vercel

## Features

- Multilingual support (EN, ZU, AF, XH)
- Responsive design
- Lead capture forms
- Service showcase with rich content pages
- SEO optimized with static generation
- AI-generated images for services
- Contact and registration pages

## Getting Started

### Prerequisites

- Node.js 18+
- npm or yarn

### Installation

1. Clone the repository:
```bash
git clone https://github.com/stalela/new-site.git
cd new-site
```

2. Install dependencies:
```bash
npm install
```

3. Run the development server:
```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) to view the site.

### Build Commands

- `npm run dev` - Start development server
- `npm run build` - Create production build
- `npm run lint` - Run ESLint
- `npm run generate-images` - Generate AI images (requires DashScope API)
- `npm run generate-images:only` - Generate specific images only

## Project Structure

```
src/
├── app/                 # Next.js app directory
│   ├── [locale]/        # Internationalized routes
│   ├── api/             # API routes (lead capture)
│   └── globals.css      # Global styles
├── components/          # Reusable components
├── lib/                 # Utilities and data
│   ├── services-data.ts # Service definitions
│   └── utils.ts         # Helper functions
└── messages/            # Translation files
```

## Internationalization

The site supports 4 languages:
- English (`en`)
- isiZulu (`zu`)
- Afrikaans (`af`)
- isiXhosa (`xh`)

Translations are stored in `src/messages/` as JSON files.

## Deployment

The site is configured for deployment on Vercel. The `vercel.json` file sets the build configuration.

### Automatic Deployment

Pushes to the `main` branch trigger automatic deployments on Vercel.

## Contributing

1. Create a feature branch
2. Make your changes
3. Test locally
4. Submit a pull request

## License

This project is proprietary to Stalela.
