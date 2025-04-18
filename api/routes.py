from fastapi import FastAPI, APIRouter, Depends, Query, Path, HTTPException
from api.database_api import engine, table_dim_station, table_dim_time
from api.database_api import  table_silver
from typing import List, Dict, Any, Annotated
from typing import List, Optional,Union,Dict, Any, Annotated
from api.models import NumericColumns
from sqlalchemy import select, func 
from datetime import datetime


app = FastAPI()
router = APIRouter()

@router.get("/")
async def get_items():
    return {"message": "Staging API for Ocean ETL Data Analysis 1.0.0"}


@router.get("/silver/{station_id: Optional[int] = None}/{time_id: Optional[int] = None}")
async def get_silver_data(
    list_columns: List[str] = Query(...), 
    station_id: Optional[int] = None, 
                          time_id: Optional[int] = None):
    query = f"SELECT * FROM {table_silver}"
    if not list_columns and not station_id and not time_id:
        raise ValueError("At least one column or station_id and time_id must be provided.")
    if station_id:
        query += f" WHERE Station ID = {station_id}"
    if time_id:
        query += f" AND Datetime = {time_id}"
    if list_columns:
        query += f" AND ({','.join(list_columns)})"
    
    result = await engine.execute(query).fetchall()
    return result

################# DATA MANIPULATION METHODS ##########################################################################################

# GET X COLUMN MEDIAN ON Datetime Range between  two dates. api propose a dropdown containing the numeric values
@router.get("/silver/median/{column}/date_range/{start_date}/{end_date}")
async def get_median_meteo_data(
    column: NumericColumns,
    start_date: str = Path(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Path(..., description="End date in YYYY-MM-DD format")
):
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    async with engine.connect() as conn:
        stmt = select(func.percentile_cont(0.5).within_group(getattr(table_silver.c, column.value))).where(
            table_silver.c.Datetime.between(start, end)
        )
        result = await conn.execute(stmt)
        median = result.scalar()

    if median is None:
        raise HTTPException(status_code=404, detail="No data found for the given range and column.")

    return {
        "column": column.value,
        "start_date": start_date,
        "end_date": end_date,
        "median": median
    }

#### Get min and max of a numeric column

@router.get("/silver/min_max/{column}")
async def get_min_max_meteo_data(
    column: NumericColumns
):
    async with engine.connect() as conn:
        stmt = select(func.min(table_silver.c[column.value]), func.max(table_silver.c[column.value])).where(
            table_silver.c[column.value].isnot(None)
        )
        result = await conn.execute(stmt)
        min_value, max_value = result.fetchone()
        if min_value is None or max_value is None:
            raise HTTPException(status_code=404, detail="No data found for the given column.")
        return {
            "column": column.value,
            "min": min_value,
            "max": max_value
        }  

# GET MODE of one or several numeric columns

@router.get("/silver/mode")
async def get_mode_meteo_data(
    columns: Annotated[List[NumericColumns], Query(...)]
):
    async with engine.connect() as conn:
        stmt = select(*[
            func.mode().within_group(getattr(table_silver.c, col.value)).label(col.value)
            for col in columns
        ])
        result = await conn.execute(stmt)
        modes = result.fetchone()
        if not modes or any(v is None for v in modes):
            raise HTTPException(status_code=404, detail="No data found for the given columns.")
        return {
            "columns": [col.value for col in columns],
            "modes": dict(zip([col.value for col in columns], modes))
        }

# GET STANDARD DEVIATION of one or several numeric columns
@router.get("/silver/std_dev")
async def get_std_dev_meteo_data(
    columns: Annotated[List[NumericColumns], Query(...)]
):
    async with engine.connect() as conn:
        stmt = select(*[
            func.stddev(getattr(table_silver.c, col.value)).label(col.value)
            for col in columns
        ])
        result = await conn.execute(stmt)
        stds = result.fetchone()
        if not stds or any(v is None for v in stds):
            raise HTTPException(status_code=404, detail="No data found for the given columns.")
        return {
            "columns": [col.value for col in columns],
            "std_devs": dict(zip([col.value for col in columns], stds))
        }


# GET CORRELATION coefficient between two numeric columns

@router.get("/silver/correlation/{column1: NumericColumns}/{column2: NumericColumns}")
async def get_correlation_meteo_data(
    column1: NumericColumns,
    column2: NumericColumns
):
    async with engine.connect() as conn:
        stmt = select(func.corr(getattr(table_silver.c, column1.value), getattr(table_silver.c, column2.value)))
        result = await conn.execute(stmt)
        correlation = result.scalar()
        if correlation is None:
            raise HTTPException(status_code=404, detail="No data found for the given columns.")
        return {
            "column1": column1.value,
            "column2": column2.value,
            "correlation": correlation
        }

# GET PERCENTILES of one or several numeric columns

@router.get("/silver/percentiles")
async def get_percentiles_meteo_data(
    columns: Annotated[List[NumericColumns], Query()],
    percentiles: Annotated[List[int], Query()] = [50, 75, 90]
):
    if any(p < 0 or p > 100 for p in percentiles):
        raise HTTPException(status_code=400, detail="Percentiles must be between 0 and 100.")
    
    async with engine.connect() as conn:
        result_data = {}
        for col in columns:
            col_results = {}
            for p in percentiles:
                stmt = select(
                    func.percentile_cont(p / 100.0)
                    .within_group(getattr(table_silver.c, col.value))
                )
                res = await conn.execute(stmt)
                value = res.scalar()
                col_results[str(p)] = value
            result_data[col.value] = col_results

        return {
            "percentiles": percentiles,
            "results": result_data
        }


#  GET correlation matrix between all numeric columns

@router.get("/silver/correlation_matrix")
async def get_correlation_matrix_meteo_data():
    numeric_columns = [col.value for col in NumericColumns]
    async with engine.connect() as conn:
        stmt = select(*[
            func.corr(getattr(table_silver.c, col1), getattr(table_silver.c, col2)).label(f"{col1}_{col2}")
            for col1 in numeric_columns
            for col2 in numeric_columns
            if col1!= col2
        ])
        result = await conn.execute(stmt)
        correlation_matrix = result.fetchall()
        return {
            "columns": numeric_columns,
            "correlation_matrix": [[corr[0] for corr in row] for row in correlation_matrix]
        }

# GET HISTOGRAM of one numeric column

@router.get("/silver/histogram/{column: NumericColumns}")
async def get_histogram_meteo_data(
    column: NumericColumns
):
    async with engine.connect() as conn:
        stmt = select(func.histogram(getattr(table_silver.c, column.value), 100)).label(column.value)
        result = await conn.execute(stmt)
        histogram = result.fetchone()
        if histogram is None or histogram[column.value] is None:
            raise HTTPException(status_code=404, detail="No data found for the given column.")
        return {
            "column": column.value,
            "histogram": histogram[column.value]
        }

# GET LINEAR REGRESSION model for one numeric column
@router.get("/silver/linear_regression/{column: NumericColumns}")
async def get_linear_regression_meteo_data(
    column: NumericColumns
):
    async with engine.connect() as conn:
        stmt = select(
            func.corr(getattr(table_silver.c, column.value), table_silver.c.Temperature).label("correlation"),
            func.linear_regression_slope(getattr(table_silver.c, column.value), table_silver.c.Temperature).label("slope"),
            func.linear_regression_intercept(getattr(table_silver.c, column.value), table_silver.c.Temperature).label("intercept")
        )
        result = await conn.execute(stmt)
        regression_data = result.fetchone()
        if regression_data is None or any(v is None for v in regression_data):
            raise HTTPException(status_code=404, detail="No data found for the given column.")
        return {
            "column": column.value,
            "correlation": regression_data["correlation"],
            "slope": regression_data["slope"],
            "intercept": regression_data["intercept"]
        }

# GET K-MEANS clustering for one numeric column
@router.get("/silver/kmeans/{column: NumericColumns}")
async def get_kmeans_meteo_data(
    column: NumericColumns
):
    async with engine.connect() as conn:
        stmt = select(func.kmeans(getattr(table_silver.c, column.value), 3).label("clusters"))
        result = await conn.execute(stmt)
        clusters = result.fetchone()
        if clusters is None or clusters["clusters"] is None:
            raise HTTPException(status_code=404, detail="No data found for the given column.")
        return {
            "column": column.value,
            "clusters": clusters["clusters"]
        }
    












