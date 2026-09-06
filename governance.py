"""[Resume point 6] Data governance & PHI controls — RBAC + column masking +
lineage tags, keeping HIPAA/PHI protected. Mirrors Unity Catalog / Lake
Formation grants and column masks (here as an enforced access layer).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Which roles may see raw PHI. Everyone else gets masked columns.
PHI_COLUMNS = ["name", "date_of_birth", "ssn"]
PRIVILEGED_ROLES = {"admin", "clinician"}


def apply_column_masking(df: DataFrame, role: str) -> DataFrame:
    """Return a role-appropriate view. Non-privileged roles see masked PHI."""
    if role in PRIVILEGED_ROLES:
        return df
    out = df
    for col in PHI_COLUMNS:
        if col in df.columns:
            if col == "ssn":
                out = out.withColumn(col, F.concat(F.lit("***-**-"), F.substring(F.col("ssn"), -4, 4)))
            elif col == "date_of_birth":
                out = out.withColumn(col, F.concat(F.substring(F.col("date_of_birth"), 1, 4), F.lit("-**-**")))
            else:
                out = out.withColumn(col, F.lit("*** MASKED ***"))
    return out


def lineage_tag(df: DataFrame, source: str) -> DataFrame:
    """Attach a lineage tag column (who/what produced this dataset)."""
    return df.withColumn("_lineage_source", F.lit(source))
