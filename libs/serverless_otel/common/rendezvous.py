import mmh3
import math
from dataclasses import dataclass
from typing import List

def hash_to_unit_interval(s: str) -> float:
    return (mmh3.hash128(s) + 1) / 2**128

@dataclass
class Node:
    name: str
    weight: float

    def compute_weighted_score(self, key: str):
        score = hash_to_unit_interval(f"{self.name}: {key}")
        log_score = 1.0 / -math.log(score)
        return self.weight * log_score

def determine_responsible_node(nodes: list[Node], key: str):
    return max(nodes, key=lambda node: node.compute_weighted_score(key), default=None)