import { NextRequest, NextResponse } from "next/server";
import { promises as fs } from "fs";
import path from "path";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();

    // Basic validation
    if (!body.email || typeof body.email !== "string") {
      return NextResponse.json(
        { error: "Email is required." },
        { status: 400 }
      );
    }

    if (!body.source || typeof body.source !== "string") {
      return NextResponse.json(
        { error: "Source is required." },
        { status: 400 }
      );
    }

    const lead = {
      id: crypto.randomUUID(),
      ...body,
      createdAt: new Date().toISOString(),
    };

    // Simple file-based storage for now
    // Replace with a database in production
    const dataDir = path.join(process.cwd(), ".data");
    const filePath = path.join(dataDir, "leads.json");

    await fs.mkdir(dataDir, { recursive: true });

    let leads: unknown[] = [];
    try {
      const existing = await fs.readFile(filePath, "utf-8");
      leads = JSON.parse(existing);
    } catch {
      // File doesn't exist yet
    }

    leads.push(lead);
    await fs.writeFile(filePath, JSON.stringify(leads, null, 2));

    console.log(`[lead] New lead captured: source=${lead.source} id=${lead.id}`);

    return NextResponse.json({ success: true, id: lead.id }, { status: 201 });
  } catch {
    return NextResponse.json(
      { error: "Invalid request." },
      { status: 400 }
    );
  }
}
