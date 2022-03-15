import os
import yaml
from string import Template
from yaml.loader import SafeLoader


def yaml_to_dict(file_path: str) -> dict:
    with open(file_path, "r") as file: 
        config = yaml.load(file, Loader=SafeLoader)
        return config["metrics"]


class ModelMaker:
    def __init__(self, config: dict, metric_desitination: str):
        self.name = config["name"]
        self.time_grains = config["time_grains"]
        self.metric_desitination = metric_desitination
    
    def __make_query(self, grain: str) -> str:
        query_template = Template('''{{ config(materialized="table") }}
        select * 
        from {{ metrics.metric(
            metric_name="$name",
            grain="$grain",
            secondary_calculations=[
                metrics.period_over_period(comparison_strategy="ratio", interval=1),
                metrics.period_over_period(comparison_strategy="difference", interval=1),
                metrics.period_to_date(aggregate="average", period="month"),
                metrics.period_to_date(aggregate="sum", period="year"),
                metrics.rolling(aggregate="average", interval=4),
                metrics.rolling(aggregate="min", interval=4)
            ]
        ) }}
        ''')
        query = query_template.substitute(name=self.name, grain=grain)
        return query

    def __get_model_prefix(self, grain: str) -> str:
        grains = {
            "day": "daily",
            "week": "weekly",
            "month": "monthly"
        }
        return f"metric__{grains[grain]}"

    def __make_model(self, grain) -> None:
        query = self.__make_query(grain)
        prefix = self.__get_model_prefix(grain)
        model_name = f"{prefix}_{self.name}"
        with open(f"{self.metric_desitination}/{model_name}.sql", "w") as file:
            file.write(query)
    
    def make_models(self):
        for grain in self.time_grains:
            self.__make_model(grain)


def ensure_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


if __name__ == "__main__":
    root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = f"{root_path}/models/mart/schema.yml"
    metric_desitination = f"{root_path}/models/metric"
    ensure_dir(metric_desitination)

    configs = yaml_to_dict(file_path)

    for config in configs: 
        model_maker = ModelMaker(config, metric_desitination)
        model_maker.make_models()
