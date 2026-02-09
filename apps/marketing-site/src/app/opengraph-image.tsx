import { ImageResponse } from "next/og";

export const runtime = "edge";
export const alt = "Stalela — Run your business. We handle the admin.";
export const size = { width: 1200, height: 630 };
export const contentType = "image/png";

export default function OGImage() {
  return new ImageResponse(
    (
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          width: "100%",
          height: "100%",
          background: "linear-gradient(135deg, #faf5f0 0%, #f0e4d6 50%, #e0c9ad 100%)",
          fontFamily: "system-ui, sans-serif",
        }}
      >
        {/* Copper accent bar */}
        <div
          style={{
            position: "absolute",
            top: 0,
            left: 0,
            right: 0,
            height: "8px",
            background: "linear-gradient(90deg, #a4785a 0%, #8a6349 100%)",
          }}
        />

        {/* Logo text */}
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            gap: "24px",
          }}
        >
          <div
            style={{
              fontSize: "72px",
              fontWeight: 700,
              color: "#1c1c1c",
              letterSpacing: "-2px",
            }}
          >
            Stalela
          </div>
          <div
            style={{
              fontSize: "28px",
              fontWeight: 400,
              color: "#8a6349",
              maxWidth: "600px",
              textAlign: "center",
              lineHeight: 1.4,
            }}
          >
            Run your business. We handle the admin.
          </div>
        </div>

        {/* Bottom tagline */}
        <div
          style={{
            position: "absolute",
            bottom: "40px",
            fontSize: "18px",
            color: "#6e4f3a",
          }}
        >
          Company registration • Compliance • Digital services — South Africa
        </div>
      </div>
    ),
    { ...size },
  );
}
