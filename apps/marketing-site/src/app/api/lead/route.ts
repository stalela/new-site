import { NextRequest, NextResponse } from "next/server";
import { supabase } from "@/lib/supabase";

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

    const { email, source, name, phone, ...rest } = body;

    const { data, error } = await supabase
      .from("leads")
      .insert({
        email,
        source,
        name: name || null,
        phone: phone || null,
        data: Object.keys(rest).length > 0 ? rest : null,
      })
      .select("id")
      .single();

    if (error) {
      console.error("[lead] Supabase insert error:", error.message);
      return NextResponse.json(
        { error: "Failed to save lead." },
        { status: 500 }
      );
    }

    console.log(`[lead] New lead captured: source=${source} id=${data.id}`);

    return NextResponse.json({ success: true, id: data.id }, { status: 201 });
  } catch {
    return NextResponse.json(
      { error: "Invalid request." },
      { status: 400 }
    );
  }
}
