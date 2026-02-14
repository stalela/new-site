#!/usr/bin/env python3
"""
Import merged company data into Neo4j graph database.
=====================================================

Creates:
  - Company nodes (with supabaseId, name, lat, lng, category, source)
  - Province, City, Suburb, Category nodes
  - Relationships: IN_PROVINCE, IN_CITY, IN_SUBURB, IN_CATEGORY,
    NEAR (within 500m), COMPETES_WITH (same category + within 2km)

Usage:
  docker run -d -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password neo4j:5
  python scripts/import_companies_to_neo4j.py

Env vars:
  NEO4J_URI       — default: bolt://localhost:7687
  NEO4J_USER      — default: neo4j
  NEO4J_PASSWORD  — default: password
"""

import json
import os
import sys
import time
import math

try:
    from neo4j import GraphDatabase
except ImportError:
    print("Installing neo4j driver...")
    os.system(f"{sys.executable} -m pip install neo4j")
    from neo4j import GraphDatabase

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")

MERGED_FILE = os.path.join("output", "merged_companies.json")
BATCH_SIZE = 1000


def create_constraints(session):
    """Create uniqueness constraints and indexes."""
    print("Creating constraints and indexes...")
    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Company) REQUIRE c.supabaseId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Province) REQUIRE p.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:City) REQUIRE c.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Suburb) REQUIRE s.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (cat:Category) REQUIRE cat.name IS UNIQUE",
        "CREATE INDEX IF NOT EXISTS FOR (c:Company) ON (c.latitude, c.longitude)",
        "CREATE INDEX IF NOT EXISTS FOR (c:Company) ON (c.name)",
        "CREATE INDEX IF NOT EXISTS FOR (c:Company) ON (c.category)",
        "CREATE INDEX IF NOT EXISTS FOR (c:Company) ON (c.source)",
    ]
    for cypher in constraints:
        try:
            session.run(cypher)
        except Exception as e:
            print(f"  ⚠ {cypher[:60]}... → {e}")


def load_companies(session, records):
    """Batch-load Company nodes."""
    print(f"Loading {len(records):,} Company nodes...")
    t0 = time.time()

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        nodes = []
        for r in batch:
            source_id = str(r.get("_source_id", ""))
            if not source_id:
                continue
            lat = r.get("latitude")
            lng = r.get("longitude")
            nodes.append({
                "supabaseId": f"{r.get('_source', '')}:{source_id}",
                "name": r.get("name", ""),
                "category": r.get("category", "") or None,
                "source": r.get("_source", ""),
                "latitude": float(lat) if lat else None,
                "longitude": float(lng) if lng else None,
                "province": r.get("province", "") or None,
                "city": r.get("city", "") or None,
                "suburb": r.get("suburb", "") or None,
                "phone": r.get("phone", "") or None,
                "email": r.get("email", "") or None,
            })

        session.run(
            """
            UNWIND $nodes AS n
            MERGE (c:Company {supabaseId: n.supabaseId})
            SET c.name = n.name,
                c.category = n.category,
                c.source = n.source,
                c.latitude = n.latitude,
                c.longitude = n.longitude,
                c.phone = n.phone,
                c.email = n.email
            """,
            nodes=nodes
        )

        if (i + BATCH_SIZE) % 5000 < BATCH_SIZE:
            elapsed = time.time() - t0
            rate = (i + len(batch)) / elapsed
            print(f"  [{i + len(batch):,}/{len(records):,}] {rate:.0f} nodes/s")

    elapsed = time.time() - t0
    print(f"  Done: {len(records):,} nodes in {elapsed:.1f}s")


def load_location_hierarchy(session, records):
    """Create Province, City, Suburb nodes and relationships."""
    print("Building location hierarchy...")

    # Collect unique values
    provinces = set()
    cities = set()
    suburbs = set()
    for r in records:
        if r.get("province"):
            provinces.add(r["province"])
        if r.get("city"):
            cities.add(r["city"])
        if r.get("suburb"):
            suburbs.add(r["suburb"])

    # Create Province nodes
    session.run(
        "UNWIND $names AS name MERGE (:Province {name: name})",
        names=list(provinces)
    )
    print(f"  {len(provinces)} provinces")

    # Create City nodes
    session.run(
        "UNWIND $names AS name MERGE (:City {name: name})",
        names=list(cities)
    )
    print(f"  {len(cities)} cities")

    # Create Suburb nodes
    if suburbs:
        for i in range(0, len(suburbs), BATCH_SIZE):
            batch = list(suburbs)[i:i + BATCH_SIZE]
            session.run(
                "UNWIND $names AS name MERGE (:Suburb {name: name})",
                names=batch
            )
    print(f"  {len(suburbs)} suburbs")

    # Create IN_PROVINCE relationships
    print("  Linking companies → provinces...")
    for i in range(0, len(records), BATCH_SIZE):
        batch = [
            {
                "supabaseId": f"{r.get('_source', '')}:{r.get('_source_id', '')}",
                "province": r["province"],
            }
            for r in records[i:i + BATCH_SIZE]
            if r.get("province") and r.get("_source_id")
        ]
        if batch:
            session.run(
                """
                UNWIND $batch AS b
                MATCH (c:Company {supabaseId: b.supabaseId})
                MATCH (p:Province {name: b.province})
                MERGE (c)-[:IN_PROVINCE]->(p)
                """,
                batch=batch
            )

    # Create IN_CITY relationships
    print("  Linking companies → cities...")
    for i in range(0, len(records), BATCH_SIZE):
        batch = [
            {
                "supabaseId": f"{r.get('_source', '')}:{r.get('_source_id', '')}",
                "city": r["city"],
            }
            for r in records[i:i + BATCH_SIZE]
            if r.get("city") and r.get("_source_id")
        ]
        if batch:
            session.run(
                """
                UNWIND $batch AS b
                MATCH (c:Company {supabaseId: b.supabaseId})
                MATCH (ct:City {name: b.city})
                MERGE (c)-[:IN_CITY]->(ct)
                """,
                batch=batch
            )

    # Create IN_SUBURB relationships
    print("  Linking companies → suburbs...")
    for i in range(0, len(records), BATCH_SIZE):
        batch = [
            {
                "supabaseId": f"{r.get('_source', '')}:{r.get('_source_id', '')}",
                "suburb": r["suburb"],
            }
            for r in records[i:i + BATCH_SIZE]
            if r.get("suburb") and r.get("_source_id")
        ]
        if batch:
            session.run(
                """
                UNWIND $batch AS b
                MATCH (c:Company {supabaseId: b.supabaseId})
                MATCH (s:Suburb {name: b.suburb})
                MERGE (c)-[:IN_SUBURB]->(s)
                """,
                batch=batch
            )


def load_categories(session, records):
    """Create Category nodes and IN_CATEGORY relationships."""
    print("Building category graph...")

    categories = set()
    for r in records:
        if r.get("category"):
            categories.add(r["category"])
        for cat in r.get("categories", []):
            if cat:
                categories.add(cat)

    session.run(
        "UNWIND $names AS name MERGE (:Category {name: name})",
        names=list(categories)
    )
    print(f"  {len(categories)} categories")

    # Link companies to categories
    for i in range(0, len(records), BATCH_SIZE):
        batch = []
        for r in records[i:i + BATCH_SIZE]:
            sid = r.get("_source_id")
            if not sid:
                continue
            supabase_id = f"{r.get('_source', '')}:{sid}"
            cats = set()
            if r.get("category"):
                cats.add(r["category"])
            for cat in r.get("categories", []):
                if cat:
                    cats.add(cat)
            for cat in cats:
                batch.append({"supabaseId": supabase_id, "category": cat})

        if batch:
            session.run(
                """
                UNWIND $batch AS b
                MATCH (c:Company {supabaseId: b.supabaseId})
                MATCH (cat:Category {name: b.category})
                MERGE (c)-[:IN_CATEGORY]->(cat)
                """,
                batch=batch
            )


def build_proximity_relationships(session):
    """Build NEAR (500m) and COMPETES_WITH (same category + 2km) relationships.
    This runs as Neo4j Cypher queries over indexed data — may take a while for 174K nodes.
    """
    print("Building NEAR relationships (companies within 500m)...")
    print("  This may take several minutes for large datasets...")

    t0 = time.time()

    # NEAR: companies within ~500m (0.005 degrees ≈ 556m at equator)
    # Process city by city to bound the search space
    result = session.run("MATCH (c:City) RETURN c.name AS city ORDER BY city")
    cities = [r["city"] for r in result]

    for city in cities:
        try:
            session.run(
                """
                MATCH (a:Company)-[:IN_CITY]->(:City {name: $city})
                WHERE a.latitude IS NOT NULL AND a.longitude IS NOT NULL
                WITH a
                MATCH (b:Company)-[:IN_CITY]->(:City {name: $city})
                WHERE b.latitude IS NOT NULL AND b.longitude IS NOT NULL
                  AND id(a) < id(b)
                  AND abs(a.latitude - b.latitude) < 0.005
                  AND abs(a.longitude - b.longitude) < 0.005
                WITH a, b,
                  point.distance(
                    point({latitude: a.latitude, longitude: a.longitude}),
                    point({latitude: b.latitude, longitude: b.longitude})
                  ) AS dist
                WHERE dist < 500
                MERGE (a)-[:NEAR {distance: dist}]->(b)
                """,
                city=city
            )
        except Exception as e:
            # point.distance might not be available in all editions
            print(f"  ⚠ NEAR failed for {city}: {e}")
            break

    elapsed = time.time() - t0
    print(f"  NEAR relationships done in {elapsed:.1f}s")

    # COMPETES_WITH: same category + within 2km
    print("Building COMPETES_WITH relationships (same category + within 2km)...")
    t0 = time.time()

    result = session.run("MATCH (cat:Category) RETURN cat.name AS cat ORDER BY cat")
    categories = [r["cat"] for r in result]

    for cat in categories[:50]:  # Limit to top 50 categories to avoid explosion
        try:
            session.run(
                """
                MATCH (a:Company)-[:IN_CATEGORY]->(:Category {name: $cat})
                WHERE a.latitude IS NOT NULL
                WITH a
                MATCH (b:Company)-[:IN_CATEGORY]->(:Category {name: $cat})
                WHERE b.latitude IS NOT NULL
                  AND id(a) < id(b)
                  AND abs(a.latitude - b.latitude) < 0.02
                  AND abs(a.longitude - b.longitude) < 0.02
                WITH a, b,
                  point.distance(
                    point({latitude: a.latitude, longitude: a.longitude}),
                    point({latitude: b.latitude, longitude: b.longitude})
                  ) AS dist
                WHERE dist < 2000
                MERGE (a)-[:COMPETES_WITH {distance: dist, category: $cat}]->(b)
                """,
                cat=cat
            )
        except Exception as e:
            print(f"  ⚠ COMPETES_WITH failed for {cat}: {e}")
            break

    elapsed = time.time() - t0
    print(f"  COMPETES_WITH relationships done in {elapsed:.1f}s")


def main():
    print(f"Loading {MERGED_FILE}...")
    with open(MERGED_FILE, "r", encoding="utf-8") as f:
        records = json.load(f)
    print(f"Loaded {len(records):,} records")

    print(f"\nConnecting to Neo4j at {NEO4J_URI}...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        driver.verify_connectivity()
        print("Connected!")
    except Exception as e:
        print(f"Failed to connect: {e}")
        sys.exit(1)

    t0 = time.time()

    with driver.session(database="neo4j") as session:
        create_constraints(session)
        load_companies(session, records)
        load_location_hierarchy(session, records)
        load_categories(session, records)
        build_proximity_relationships(session)

    elapsed = time.time() - t0

    # Print summary
    with driver.session(database="neo4j") as session:
        counts = session.run(
            """
            MATCH (n)
            WITH labels(n)[0] AS label, count(n) AS cnt
            RETURN label, cnt ORDER BY cnt DESC
            """
        )
        print(f"\n{'='*60}")
        print(f"NEO4J IMPORT COMPLETE ({elapsed/60:.1f} minutes)")
        print(f"  Node counts:")
        for r in counts:
            print(f"    {r['label']}: {r['cnt']:,}")

        rels = session.run(
            """
            MATCH ()-[r]->()
            WITH type(r) AS rtype, count(r) AS cnt
            RETURN rtype, cnt ORDER BY cnt DESC
            """
        )
        print(f"  Relationship counts:")
        for r in rels:
            print(f"    {r['rtype']}: {r['cnt']:,}")
        print(f"{'='*60}")

    driver.close()


if __name__ == "__main__":
    main()
