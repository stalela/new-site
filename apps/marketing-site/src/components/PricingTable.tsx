import { Card } from "./Card";
import { Button } from "./Button";
import { Check } from "lucide-react";
import { cn } from "@/lib/utils";

interface PricingTier {
  name: string;
  price: string;
  description: string;
  features: string[];
  ctaText: string;
  ctaHref: string;
  highlighted?: boolean;
}

interface PricingTableProps {
  tiers: PricingTier[];
}

export function PricingTable({ tiers }: PricingTableProps) {
  return (
    <div className="grid gap-8 md:grid-cols-3">
      {tiers.map((tier) => (
        <Card
          key={tier.name}
          className={cn(
            "flex flex-col",
            tier.highlighted && "border-copper-600 ring-1 ring-copper-600"
          )}
        >
          {tier.highlighted && (
            <span className="-mt-3 mb-4 inline-block self-start rounded-full bg-copper-100 px-3 py-1 text-xs font-semibold text-copper-700">
              Most popular
            </span>
          )}
          <h3 className="text-lg font-semibold text-gray-900">{tier.name}</h3>
          <p className="mt-1 text-sm text-gray-500">{tier.description}</p>
          <p className="mt-6 text-4xl font-bold text-gray-900">{tier.price}</p>
          <ul className="mt-8 flex-1 space-y-3">
            {tier.features.map((feature) => (
              <li key={feature} className="flex items-start gap-2 text-sm text-gray-600">
                <Check className="mt-0.5 h-4 w-4 flex-shrink-0 text-copper-600" />
                {feature}
              </li>
            ))}
          </ul>
          <div className="mt-8">
            <Button
              href={tier.ctaHref}
              variant={tier.highlighted ? "primary" : "outline"}
              className="w-full"
            >
              {tier.ctaText}
            </Button>
          </div>
        </Card>
      ))}
    </div>
  );
}
