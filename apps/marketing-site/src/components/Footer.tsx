import Image from "next/image";
import { Link } from "@/i18n/navigation";
import { Container } from "./Container";
import { getTranslations } from "next-intl/server";

export async function Footer() {
  const t = await getTranslations("Footer");

  const footerLinks = {
    [t("product")]: [
      { label: t("services"), href: "/services" },
      { label: t("howItWorks"), href: "/how-it-works" },
      { label: t("pricing"), href: "/pricing" },
      { label: t("blog"), href: "/blog" },
    ],
    [t("company")]: [
      { label: t("contact"), href: "/contact" },
    ],
    [t("legal")]: [
      { label: t("privacyPolicy"), href: "#" },
      { label: t("termsOfService"), href: "#" },
    ],
  };

  return (
    <footer className="border-t border-gray-200 bg-gray-50">
      <Container className="py-12">
        <div className="grid grid-cols-2 gap-8 md:grid-cols-4">
          <div className="col-span-2 md:col-span-1">
            <Link href="/" className="inline-block">
              <Image
                src="/images/logo.svg"
                alt="Stalela"
                width={240}
                height={66}
                className="h-14 w-auto"
              />
            </Link>
            <p className="mt-3 text-sm leading-relaxed text-gray-500">
              {t("tagline")}
            </p>
          </div>

          {Object.entries(footerLinks).map(([category, links]) => (
            <div key={category}>
              <h3 className="text-sm font-semibold text-gray-900">{category}</h3>
              <ul className="mt-3 space-y-2">
                {links.map((link) => (
                  <li key={link.label}>
                    <Link
                      href={link.href}
                      className="text-sm text-gray-500 transition-colors hover:text-gray-900"
                    >
                      {link.label}
                    </Link>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>

        <div className="mt-10 border-t border-gray-200 pt-6 text-center text-sm text-gray-400">
          {t("copyright", { year: new Date().getFullYear().toString() })}
        </div>
      </Container>
    </footer>
  );
}
