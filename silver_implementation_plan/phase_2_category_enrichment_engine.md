# Phase 2: Category Enrichment & URL Normalization Engine
**Priority**: **P1 · High**  
**Target Files**: [`notebooks/02_silver_transformation.py`](file:///d:/Semester%20Projects/Panopticon/notebooks/02_silver_transformation.py), [`data/schemas/ref_citizenlab_categories_schema.json`](file:///d:/Semester%20Projects/Panopticon/data/schemas/ref_citizenlab_categories_schema.json)  
**Downstream Impact**: Enriches [`silver_network_measurements`](file:///d:/Semester%20Projects/Panopticon/data/schemas/silver_conformed_schema.json) with standardized Citizen Lab taxonomy, match provenance, and human rights risk tiers.  
**Associated Audit Findings**: `SILVER-42`, `SILVER-44`, `SILVER-46`, `SILVER-52`

```text
==============================================================================
PIPELINE DEPENDENCY & IMPACT TRACKING
==============================================================================
Upstream Dependencies:
  - ref_citizenlab_categories (Reference taxonomy populated in Phase 0)
  - Silver conformed DataFrame with normalized input URLs
Downstream Impact:
  - Populates: content_category, category_description, category_match_type,
    taxonomy_version, human_rights_risk_tier on silver_network_measurements
Risk & Side Effects:
  - Unbounded broadcast joins can crash Community Edition; reference table
    must remain < 10 MB and deduplicated prior to broadcasting.
==============================================================================
```

---

## 1. Problem Diagnosis & Architectural Vulnerabilities

### Problem 1.1: Bare-Domain Join Stripping Path Context (SILVER-42)
* **Root Cause**: Earlier designs stripped URL paths and joined strictly on `domain = parse_url(input, 'HOST')`.
* **Impact**: Completely destroys path-specific contextual categorization:
  - `bbc.com/news/persian` (Critical Political/News: `POLR`/`NEWS`) gets misclassified as generic portal news.
  - `reddit.com/r/lgbt` (`LGBT`) gets merged into general social media (`COMM`).
  - Broad over-generalization obscures localized censorship campaigns targeting specific article paths.

### Problem 1.2: Floating GitHub Taxonomy Versioning (SILVER-44, SILVER-46)
* **Root Cause**: Loading from an unversioned `master` branch URL dynamically alters historical categorizations when pipelines are backfilled or re-run months later.
* **Impact**: Violates longitudinal reproducibility. An evaluator re-running a 2026 dataset in 2027 gets conflicting category labels.
* **Remediation**: Pin `ref_citizenlab_categories` to an immutable Git commit SHA, and propagate `taxonomy_version` into every enriched row of `silver_network_measurements`.

### Problem 1.3: Non-Web Measurements with Null/Empty Input (SILVER-52)
* **Root Cause**: Instant messaging tests (WhatsApp, Telegram, Signal) and circumvention tests (Tor, Psiphon) test IP endpoints or bridge descriptors without a web URL (`input is null`).
* **Impact**: An unconditional inner join drops all app tests. An outer join with poorly-formed regex produces `NULL` matches or unnecessary string parsing overhead.

---

## 2. Target Implementation Blueprint

### Step 2.1: Robust URL Normalization Function
```python
def normalize_url_and_domain(df: DataFrame) -> DataFrame:
    """
    Normalizes input URLs:
      1. Strips trailing slashes, fragments, and queries for exact matching.
      2. Extracts lower-case hostname (stripping leading 'www.').
    """
    return df.withColumn(
        "normalized_url",
        when(
            col("input").isNotNull() & (col("input") != "") & (col("input") != "NO_INPUT"),
            lower(regexp_replace(col("input"), r"/+$", ""))
        ).otherwise(lit(None).cast("string"))
    ).withColumn(
        "extracted_domain",
        when(
            col("normalized_url").isNotNull(),
            lower(regexp_replace(
                coalesce(
                    parse_url(col("normalized_url"), "HOST"),
                    regexp_extract(col("normalized_url"), r"(?:https?://)?([^/:]+)", 1)
                ),
                r"^www\.",
                ""
            ))
        ).otherwise(lit(None).cast("string"))
    )
```

### Step 2.2: Two-Tier Hierarchical Citizen Lab Join Engine
Implement a two-pass join: Tier 1 (Exact Normalized URL) $\rightarrow$ Tier 2 (Deduplicated Domain Fallback) $\rightarrow$ Tier 3 (Unmatched Default):
```python
def enrich_citizenlab_taxonomy(
    df_measurements: DataFrame,
    df_ref_categories: DataFrame
) -> DataFrame:
    """
    Enriches measurements using Citizen Lab taxonomy via Two-Tier matching:
      Tier 1: Exact normalized URL match (category_match_type = 'EXACT_URL')
      Tier 2: Domain fallback on deduplicated reference index (category_match_type = 'DOMAIN_FALLBACK')
      Tier 3: Default fallback (category_match_type = 'UNMATCHED')
    """
    from pyspark.sql.functions import broadcast

    # 1. Prepare Tier 1 Reference Table: Exact Normalized URL
    ref_tier1 = df_ref_categories.select(
        lower(regexp_replace(col("url"), r"/+$", "")).alias("ref_url"),
        col("category_code").alias("cat_tier1"),
        col("category_description").alias("desc_tier1"),
        col("taxonomy_version").alias("tax_ver_tier1")
    ).distinct()

    # 2. Prepare Tier 2 Reference Table: Deduplicated Domain Index
    # When multiple URLs exist for one domain with differing categories,
    # prioritize political/human rights categories over generic ones.
    w_domain = Window.partitionBy("ref_domain").orderBy(
        when(col("category_code").isin("POLR", "HUMR", "NEWS", "LGBT", "REL"), lit(1))
        .when(col("category_code").isin("COMM", "FILE", "ANON"), lit(2))
        .otherwise(lit(3)),
        col("url")
    )
    ref_tier2 = df_ref_categories.withColumn(
        "ref_domain",
        lower(regexp_replace(
            coalesce(parse_url(col("url"), "HOST"), regexp_extract(col("url"), r"(?:https?://)?([^/:]+)", 1)),
            r"^www\.",
            ""
        ))
    ).withColumn("rank", row_number().over(w_domain)) \
     .filter(col("rank") == 1) \
     .select(
        "ref_domain",
        col("category_code").alias("cat_tier2"),
        col("category_description").alias("desc_tier2"),
        col("taxonomy_version").alias("tax_ver_tier2")
    )

    # 3. Execute Broadcast Joins (Safe: Citizen Lab taxonomy is ~15,000 rows / < 2 MB)
    joined = df_measurements \
        .join(broadcast(ref_tier1), col("normalized_url") == col("ref_url"), "left") \
        .join(broadcast(ref_tier2), col("extracted_domain") == col("ref_domain"), "left")

    # 4. Synthesize Two-Tier Results
    enriched = joined.withColumn(
        "category_match_type",
        when(col("cat_tier1").isNotNull(), lit("EXACT_URL"))
        .when(col("cat_tier2").isNotNull(), lit("DOMAIN_FALLBACK"))
        .otherwise(lit("UNMATCHED"))
    ).withColumn(
        "content_category",
        coalesce(col("cat_tier1"), col("cat_tier2"), lit("MISC"))
    ).withColumn(
        "category_description",
        coalesce(col("desc_tier1"), col("desc_tier2"), lit("Uncategorized Content"))
    ).withColumn(
        "taxonomy_version",
        coalesce(col("tax_ver_tier1"), col("tax_ver_tier2"), lit("UNKNOWN"))
    ).drop("ref_url", "cat_tier1", "desc_tier1", "tax_ver_tier1",
           "ref_domain", "cat_tier2", "desc_tier2", "tax_ver_tier2")

    return enriched
```

### Step 2.3: Human Rights Risk Tier Derivation
Derive the risk tier for censorship impact analysis:
```python
def assign_human_rights_risk_tier(df: DataFrame) -> DataFrame:
    """
    Maps Citizen Lab category codes into academic Human Rights Risk Tiers:
      - HIGH: Political discourse, human rights advocacy, independent news, minority rights
      - MEDIUM: Communication tools, anonymity/circumvention, religious debate
      - LOW: Entertainment, gaming, commercial shopping, miscellaneous
    """
    return df.withColumn(
        "human_rights_risk_tier",
        when(col("content_category").isin("POLR", "HUMR", "NEWS", "LGBT", "MMED"), lit("HIGH"))
        .when(col("content_category").isin("ANON", "COMM", "REL", "FILE", "PUBH"), lit("MEDIUM"))
        .otherwise(lit("LOW"))
    )
```

---

## 3. Verification Protocol & Acceptance Criteria

1. **Exact URL Precedence**: Test with `https://bbc.com/news/persian`; confirm `category_match_type == 'EXACT_URL'` and category is `NEWS` (not overridden by domain defaults).
2. **Domain Fallback**: Test with `https://subdomain.twitter.com/random_page`; confirm `category_match_type == 'DOMAIN_FALLBACK'` and category is `COMM`.
3. **Unmatched Handling**: Test with `https://internal-untracked-site.local`; confirm `category_match_type == 'UNMATCHED'` and category is `MISC`.
4. **App Tests**: Confirm rows with null `input` evaluate cleanly to `category_match_type == 'UNMATCHED'` with zero runtime errors.
5. **Lineage Preservation**: Confirm `taxonomy_version` is populated on 100% of rows.
