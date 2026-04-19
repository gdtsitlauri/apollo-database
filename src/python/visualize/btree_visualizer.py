"""B+ Tree ASCII visualizer — saves a text-based tree diagram to results/visualizations/."""
from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class BNode:
    keys: list[int] = field(default_factory=list)
    children: list["BNode"] = field(default_factory=list)
    is_leaf: bool = True
    next: Optional["BNode"] = None


class BPlusTree:
    def __init__(self, order: int = 4):
        self.order = order
        self.root = BNode(is_leaf=True)

    def insert(self, key: int) -> None:
        result = self._insert(self.root, key)
        if result is not None:
            mid_key, new_node = result
            new_root = BNode(is_leaf=False)
            new_root.keys = [mid_key]
            new_root.children = [self.root, new_node]
            self.root = new_root

    def _insert(self, node: BNode, key: int):
        if node.is_leaf:
            node.keys.append(key)
            node.keys.sort()
            if len(node.keys) >= self.order:
                return self._split_leaf(node)
            return None
        idx = sum(1 for k in node.keys if k <= key)
        result = self._insert(node.children[idx], key)
        if result is not None:
            mid_key, new_child = result
            node.keys.insert(idx, mid_key)
            node.children.insert(idx + 1, new_child)
            if len(node.keys) >= self.order:
                return self._split_internal(node)
        return None

    def _split_leaf(self, node: BNode):
        mid = len(node.keys) // 2
        new_node = BNode(is_leaf=True)
        new_node.keys = node.keys[mid:]
        new_node.next = node.next
        node.next = new_node
        node.keys = node.keys[:mid]
        return new_node.keys[0], new_node

    def _split_internal(self, node: BNode):
        mid = len(node.keys) // 2
        mid_key = node.keys[mid]
        new_node = BNode(is_leaf=False)
        new_node.keys = node.keys[mid + 1:]
        new_node.children = node.children[mid + 1:]
        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]
        return mid_key, new_node

    def height(self) -> int:
        h, n = 0, self.root
        while not n.is_leaf:
            h += 1
            n = n.children[0]
        return h + 1

    def to_ascii(self) -> list[str]:
        lines = []
        self._render(self.root, "", True, lines)
        return lines

    def _render(self, node: BNode, prefix: str, is_last: bool, lines: list[str]) -> None:
        connector = "└── " if is_last else "├── "
        leaf_marker = " [L]" if node.is_leaf else ""
        lines.append(prefix + connector + str(node.keys) + leaf_marker)
        child_prefix = prefix + ("    " if is_last else "│   ")
        for i, child in enumerate(node.children):
            self._render(child, child_prefix, i == len(node.children) - 1, lines)


def save_placeholder_visualization() -> Path:
    output = Path("results/visualizations/btree_animation.gif")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b"GIF89a")
    return output


def main() -> None:
    out_dir = Path("results/visualizations")
    out_dir.mkdir(parents=True, exist_ok=True)

    scenarios = [
        ("uniform", list(range(1, 17))),
        ("skewed", [1, 2, 3, 4, 5, 6, 7, 8, 100, 200, 300, 400, 500, 600, 700, 800]),
        ("reverse", list(range(16, 0, -1))),
    ]

    report_lines = ["APOLLO B+ Tree Visualizer", "=" * 50, ""]

    for label, keys in scenarios:
        tree = BPlusTree(order=4)
        for k in keys:
            tree.insert(k)
        report_lines.append(f"Scenario: {label}  (order=4, keys={len(keys)})")
        report_lines.append(f"Tree height: {tree.height()}")
        report_lines.append("Structure:")
        report_lines.extend(tree.to_ascii())
        report_lines.append("")

    report_path = out_dir / "btree_diagram.txt"
    report_path.write_text("\n".join(report_lines), encoding="utf-8")

    save_placeholder_visualization()

    print(f"Saved btree diagram → {report_path}")
    print(f"Saved placeholder GIF → results/visualizations/btree_animation.gif")
    for line in report_lines[:30]:
        print(line)


if __name__ == "__main__":
    main()
