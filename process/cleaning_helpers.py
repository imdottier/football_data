import logging, re
import pandas as pd
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StringType

def camel_to_snake(name: str) -> str:
    """Converts a single string from camelCase to snake_case."""
    if name is None:
        return None
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()


def flatten_df(df: DataFrame, cols_to_snake_case_values: set[str] | None = None) -> DataFrame:
    """
    Recursively flattens a DataFrame's nested structs and converts all column
    names from camelCase to snake_case.

    Optionally, it can also convert the *values* within specified string columns
    to snake_case.

    Args:
        df: The input DataFrame with potentially nested structs.
        cols_to_snake_case_values: A set of final snake_case column names whose
                                   string values should also be converted to snake_case.

    Returns:
        A new DataFrame with a flat schema and snake_case column names.
    """
    if cols_to_snake_case_values is None:
        cols_to_snake_case_values = set()

    # --- Vectorized UDF for Value Conversion ---
    @pandas_udf(StringType())
    def camel_to_snake_vectorized(series: pd.Series) -> pd.Series:
        # apply the regex replacement to each string in the series
        return series.str.replace(r'(?<!^)(?=[A-Z])', '_', regex=True).str.lower()

    # handling recursion and column collection
    flat_cols = []
    
    def get_flat_cols(schema: StructType, prefix: str = ""):
        for field in schema.fields:
            # full column name with prefix for nested fields
            full_col_name = f"{prefix}{field.name}" if prefix else field.name
            
            if isinstance(field.dataType, StructType):
                # recurse into the nested struct
                get_flat_cols(field.dataType, prefix=f"{full_col_name}.")
            else:
                alias_name = camel_to_snake(full_col_name.replace(".", "_"))
                
                # convert values to snake_case if specified and if the field is a string
                if alias_name in cols_to_snake_case_values and isinstance(field.dataType, StringType):
                    flat_cols.append(
                        camel_to_snake_vectorized(col(full_col_name)).alias(alias_name)
                    )
                else:
                    flat_cols.append(
                        col(full_col_name).alias(alias_name)
                    )

    try:
        get_flat_cols(df.schema)
        
        if not flat_cols:
            logging.warning("flatten_df resulted in no columns. Returning an empty DataFrame.")
            return df.sparkSession.createDataFrame([], schema=StructType([]))
            
        return df.select(flat_cols)

    except Exception as e:
        logging.error("An error occurred during DataFrame flattening.", exc_info=True)
        raise