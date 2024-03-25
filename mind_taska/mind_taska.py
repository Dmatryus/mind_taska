from typing import Union, Dict
from datetime import datetime
import json
from copy import deepcopy
import warnings

from pathlib import Path
import pandas as pd
import xmltodict


class MTConfig:
    def read_json(self, path: Path):
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)

        self.__init__(**config)

    def __init__(
        self,
        config_path: Union[Path, None] = None,
        excel_path: Union[Path, None] = None,
        color_status_mapping: Union[Dict[str, str], None] = None,
        shape_type_mapping: Union[Dict[str, str], None] = None,
    ):
        """
        Initialize the configuration with given parameters,
        or load from a json file at path provided in constructor call (default to current directory).
        """
        self.excel_path = excel_path
        self.color_status_mapping = color_status_mapping
        self.shape_type_mapping = shape_type_mapping

        if config_path is not None:
            self.read_json(config_path)

    def to_json(self, path: Union[Path, None] = None, return_str: bool = True):
        output = deepcopy(self.__dict__)
        output = {k: str(v) if isinstance(v, Path) else v for k, v in output.items()}
        if path:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=4)
        if return_str:
            return json.dumps(output, indent=4)

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self)


class MindTaska:
    def __init__(self, config: Union[MTConfig, Path] = None):
        self.config = config if isinstance(config, MTConfig) else MTConfig(config)
        if self.config.excel_path is not None:
            self.tasks = pd.read_excel(self.config.excel_path, sheet_name="tasks")
            self.stats = pd.read_excel(self.config.excel_path, sheet_name="stats")
        else:
            self.tasks = None
            self.stats = None

    def _parse_node(self, tasks: list, node: dict, project: str, parent: dict = None):
        parent = parent or {}
        result = None
        try:
            result = {
                "task": node["@text"],
                "project": project,
                "type": self.config.shape_type_mapping.get(node.get("@shape"))
                or parent.get("type"),
                "status": self.config.color_status_mapping.get(node.get("@bgColor"))
                or parent.get("status"),
                "worker": node.get("eicon", {}).get("@id"),
            }
            tasks.append(result)
        except KeyError as e:
            print(e)
            print(f"In node :\n{node}")

        topic = node.get("topic", [])
        topic = topic if isinstance(topic, list) else [topic]
        for t in topic:
            self._parse_node(tasks, t, project, result)

    def extract_tasks(self, maindmap_path: Path) -> pd.DataFrame:
        with open(maindmap_path, "r", encoding="utf-8") as f:
            xml_string = f.read()
        xml_data = xmltodict.parse(xml_string)
        tasks = []
        main_node = xml_data["map"]["topic"]["topic"]

        for project in main_node:
            self._parse_node(tasks, project, project["@text"])
        return pd.DataFrame(tasks)

    def get_stat(self, tasks: pd.DataFrame):
        stat = {
            "date": datetime.now(),
            "opened tasks": tasks[tasks["status"] != "complete"].shape[0],
            "new tasks": 0,
            "closed tasks": 0,
        }

        for w in tasks["worker"].unique():
            if w is not None:
                stat[w] = tasks[
                    (tasks["worker"] == w) & (tasks["status"] != "complete")
                ].shape[0]

        return pd.DataFrame([stat])

    def diff_stat(self, new_tasks: pd.DataFrame, new_stats: pd.DataFrame):
        complete_mark = "complete"

        if self.config.excel_path is None:
            return new_stats

        new_tasks_count = 0
        closed_tasks_count = 0
        for i, t in new_tasks.iterrows():
            same_task = self.tasks[
                (self.tasks["task"] == t["task"])
                & (self.tasks["project"] == t["project"])
            ]
            t_new_count = len(same_task)
            if t_new_count == 0:
                if t["status"] != complete_mark:
                    new_tasks_count += 1
            elif (
                t_new_count
                and same_task.iloc[0]["status"] != complete_mark
                and t["status"] == complete_mark
            ):
                closed_tasks_count += 1
            elif (
                t["status"] == "complite"
                and same_task.iloc[0]["status"] != complete_mark
            ):
                new_tasks_count += 1

        new_stats["new tasks"] = new_tasks_count
        new_stats["closed tasks"] = closed_tasks_count

        return new_stats

    def update_data(self, new_tasks: pd.DataFrame, new_stats: pd.DataFrame):
        self.tasks = new_tasks
        self.stats = pd.concat([self.stats, new_stats], axis=0, ignore_index=True)

    def save_excel(self, path: Path = None):
        out_path = path or self.config.excel_path
        if out_path is not None:
            with pd.ExcelWriter(out_path) as writer:
                self.tasks.to_excel(writer, sheet_name="tasks", index=False)
                self.stats.to_excel(writer, sheet_name="stats", index=False)
        else:
            warnings.warn("No excel path provided")

    def full_parse(self, maindmap_path: Path, excel_path: Path = None):
        tasks = self.extract_tasks(maindmap_path)
        stats = self.get_stat(tasks)
        stats = self.diff_stat(tasks, stats)
        self.update_data(tasks, stats)
        self.save_excel(excel_path)
