"""实际多模块缺陷定位任务与驻留内存 AST 工作集。"""
from __future__ import annotations
import ast
import hashlib
import json
from pathlib import Path

FILES = {
    'shop/__init__.py': '',
    'shop/pricing.py': '''from decimal import Decimal, ROUND_HALF_UP

def discounted_price(price, percent):
    value = Decimal(str(price)) * (Decimal("1") + Decimal(str(percent)) / 100)
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
''',
    'shop/cart.py': '''from decimal import Decimal
from shop.pricing import discounted_price

def cart_total(items, percent):
    return sum((discounted_price(price, percent) * quantity for price, quantity in items), Decimal("0.00"))
''',
    'tests/test_shop.py': '''import unittest
from decimal import Decimal
from shop.cart import cart_total
from shop.pricing import discounted_price

class PricingTests(unittest.TestCase):
    def test_discount(self):
        self.assertEqual(discounted_price("100", 20), Decimal("80.00"))
    def test_half_up_rounding(self):
        self.assertEqual(discounted_price("0.05", 10), Decimal("0.05"))
    def test_cart_multiple_files(self):
        self.assertEqual(cart_total([("10", 2), ("25", 1)], 20), Decimal("36.00"))
    def test_zero_discount(self):
        self.assertEqual(discounted_price("12.34", 0), Decimal("12.34"))
''',
}
MODEL_FILES = dict(FILES)
MODEL_FILES['shop/cart.py'] = FILES['shop/cart.py'].replace('discounted_price(price, percent) * quantity', 'discounted_price(price, percent)')
TEST_COMMAND = 'python3 -B -m unittest discover -s tests -v'


def fingerprint(trees):
    return hashlib.sha256(json.dumps({name: ast.dump(tree, include_attributes=True) for name, tree in sorted(trees.items())}, sort_keys=True).encode()).hexdigest()


class RepositoryContext:
    def __init__(self, root):
        self.trees = {name: ast.parse((Path(root) / name).read_text(), filename=name) for name in FILES if name.endswith('.py')}
        self.nonce = __import__('uuid').uuid4().hex
        self.history = ['读取4份源码与测试', '运行失败测试', '定位折扣公式运算符']
        self.before_sha256 = fingerprint(self.trees)
        tree = self.trees['shop/pricing.py']
        self.defects = [node for node in ast.walk(tree) if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add)
                        and isinstance(node.left, ast.Call) and ast.unparse(node.left) == "Decimal('1')"]
        if len(self.defects) != 1:
            raise ValueError('折扣公式AST定位需要唯一缺陷')

    def state(self):
        return dict(ast_sha256=fingerprint(self.trees), context_nonce=self.nonce,
                    ast_object_ids={name: id(tree) for name, tree in self.trees.items()},
                    history=list(self.history), source_files=sorted(self.trees),
                    defect_locations=[dict(path='shop/pricing.py', line=node.lineno, operator=type(node.op).__name__) for node in self.defects])

    def repair(self):
        self.defects[0].op = ast.Sub()
        self.history.append('AST替换Add为Sub并验证跨模块单测')
        return ast.unparse(self.trees['shop/pricing.py']) + '\n'
