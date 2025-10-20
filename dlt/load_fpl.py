import dlt
import httpx


@dlt.resource(
    name="fpl_data",
    write_disposition="append"
)
def get_data():
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    response = httpx.get(url)
    response.raise_for_status()
    
    data = response.json()
    
    # Endpoint returns nested data structures, each becomes a table
    for key, value in data.items():
        if isinstance(value, list):
            for item in value:
                yield dlt.mark.with_table_name(item, key)
        else:
            yield dlt.mark.with_table_name({key: value}, "metadata")


@dlt.source
def fpl_source():
    return get_data()

pipeline = dlt.pipeline(
    pipeline_name="fpl_analytics__fpl_pipeline",
    destination="motherduck",
    dataset_name="fpl",
)

if __name__ == "__main__":

    load_info = pipeline.run(fpl_source())
    
    print(load_info)