"""Safe expression evaluator and template renderer.

Evaluates a restricted subset of Python expressions — no imports, no
attribute access on arbitrary objects, no builtins, no exec/eval.

Every operation is guarded against resource exhaustion:
- String results capped at ``MAX_RESULT_LEN``
- Numeric results capped at ``MAX_NUM``
- Power exponents capped at ``MAX_POW_EXP``
- String/list repeat capped at ``MAX_REPEAT``
- Expression source length capped at ``MAX_EXPR_LEN``

Usage::

    >>> safe_eval("index + 1", {"index": 3})
    4
    >>> render_template("Hello #{index + 1}!", {"index": 3})
    'Hello #4!'
"""

from __future__ import annotations

import ast
import operator
import re
from typing import Any

# ------------------------------------------------------------------
# Safety limits
# ------------------------------------------------------------------

MAX_EXPR_LEN: int = 1_000
"""Max characters in a single expression."""

MAX_RESULT_LEN: int = 10_000
"""Max string length for any intermediate or final result."""

MAX_NUM: int | float = 10**15
"""Absolute ceiling for numeric results."""

MAX_POW_EXP: int = 100
"""Largest allowed exponent in ``a ** b``."""

MAX_REPEAT: int = 1_000
"""Max repetitions for ``str * n`` or ``list * n``."""

MAX_TEMPLATE_LEN: int = 50_000
"""Max length of a rendered template string."""

MAX_TEMPLATE_EXPRESSIONS: int = 100
"""Max ``{…}`` expressions in a single template."""


class SafeEvalError(ValueError):
    """Raised when an expression is rejected or exceeds limits."""


# ------------------------------------------------------------------
# Operator tables
# ------------------------------------------------------------------

_BINOPS: dict[type, Any] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}

_UNARYOPS: dict[type, Any] = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
    ast.Not: operator.not_,
}

_CMPOPS: dict[type, Any] = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
}

_SAFE_CALLS: dict[str, Any] = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "len": len,
    "abs": abs,
    "min": min,
    "max": max,
    "round": round,
}


# ------------------------------------------------------------------
# AST evaluator
# ------------------------------------------------------------------


class _Evaluator(ast.NodeVisitor):
    """Walk an AST and evaluate only safe nodes."""

    def __init__(self, variables: dict[str, Any]) -> None:
        self._vars = variables

    def visit(self, node: ast.AST) -> Any:
        method = "visit_" + type(node).__name__
        visitor = getattr(self, method, None)
        if visitor is None:
            msg = f"Unsupported expression: {type(node).__name__}"
            raise SafeEvalError(msg)
        return visitor(node)

    # --- literals ---

    def visit_Constant(self, node: ast.Constant) -> Any:
        if isinstance(node.value, (int, float)):
            _check_num(node.value)
        if isinstance(node.value, str):
            _check_str(node.value)
        if isinstance(node.value, (int, float, str, bool, type(None))):
            return node.value
        msg = f"Unsupported literal type: {type(node.value).__name__}"
        raise SafeEvalError(msg)

    def visit_List(self, node: ast.List) -> list[Any]:
        result = [self.visit(el) for el in node.elts]
        _check_len(result)
        return result

    def visit_Tuple(self, node: ast.Tuple) -> tuple[Any, ...]:
        result = tuple(self.visit(el) for el in node.elts)
        _check_len(result)
        return result

    def visit_Dict(self, node: ast.Dict) -> dict[Any, Any]:
        keys = [self.visit(k) for k in node.keys]
        vals = [self.visit(v) for v in node.values]
        return dict(zip(keys, vals))

    # --- variables ---

    def visit_Name(self, node: ast.Name) -> Any:
        if node.id in self._vars:
            return self._vars[node.id]
        msg = f"Unknown variable: {node.id}"
        raise SafeEvalError(msg)

    # --- operators ---

    def visit_BinOp(self, node: ast.BinOp) -> Any:
        op_type = type(node.op)
        op_fn = _BINOPS.get(op_type)
        if op_fn is None:
            msg = f"Unsupported operator: {op_type.__name__}"
            raise SafeEvalError(msg)

        left = self.visit(node.left)
        right = self.visit(node.right)

        # Guard: power
        if op_type is ast.Pow:
            if isinstance(right, (int, float)) and abs(right) > MAX_POW_EXP:
                msg = (
                    f"Exponent {right} exceeds limit of "
                    f"{MAX_POW_EXP}"
                )
                raise SafeEvalError(msg)

        # Guard: string/list repeat
        if op_type is ast.Mult:
            if isinstance(left, str) and isinstance(right, (int, float)):
                if right > MAX_REPEAT:
                    msg = f"String repeat {right} exceeds limit of {MAX_REPEAT}"
                    raise SafeEvalError(msg)
            elif isinstance(right, str) and isinstance(left, (int, float)):
                if left > MAX_REPEAT:
                    msg = f"String repeat {left} exceeds limit of {MAX_REPEAT}"
                    raise SafeEvalError(msg)
            elif isinstance(left, list) and isinstance(right, (int, float)):
                if right > MAX_REPEAT:
                    msg = f"List repeat {right} exceeds limit of {MAX_REPEAT}"
                    raise SafeEvalError(msg)

        result = op_fn(left, right)

        if isinstance(result, (int, float)):
            _check_num(result)
        if isinstance(result, str):
            _check_str(result)
        return result

    def visit_UnaryOp(self, node: ast.UnaryOp) -> Any:
        op_fn = _UNARYOPS.get(type(node.op))
        if op_fn is None:
            msg = f"Unsupported unary operator: {type(node.op).__name__}"
            raise SafeEvalError(msg)
        result = op_fn(self.visit(node.operand))
        if isinstance(result, (int, float)):
            _check_num(result)
        return result

    def visit_Compare(self, node: ast.Compare) -> bool:
        left = self.visit(node.left)
        for op, comparator_node in zip(node.ops, node.comparators):
            op_fn = _CMPOPS.get(type(op))
            if op_fn is None:
                msg = f"Unsupported comparison: {type(op).__name__}"
                raise SafeEvalError(msg)
            right = self.visit(comparator_node)
            if not op_fn(left, right):
                return False
            left = right
        return True

    def visit_BoolOp(self, node: ast.BoolOp) -> Any:
        if isinstance(node.op, ast.And):
            result: Any = True
            for val_node in node.values:
                result = self.visit(val_node)
                if not result:
                    return result
            return result
        if isinstance(node.op, ast.Or):
            result = False
            for val_node in node.values:
                result = self.visit(val_node)
                if result:
                    return result
            return result
        msg = f"Unsupported bool op: {type(node.op).__name__}"
        raise SafeEvalError(msg)

    def visit_IfExp(self, node: ast.IfExp) -> Any:
        if self.visit(node.test):
            return self.visit(node.body)
        return self.visit(node.orelse)

    # --- subscript (dict/list access) ---

    def visit_Subscript(self, node: ast.Subscript) -> Any:
        obj = self.visit(node.value)
        key = self.visit(node.slice)
        if not isinstance(obj, (dict, list, tuple, str)):
            msg = f"Cannot subscript type: {type(obj).__name__}"
            raise SafeEvalError(msg)
        try:
            return obj[key]
        except (KeyError, IndexError, TypeError) as exc:
            msg = f"Subscript error: {exc}"
            raise SafeEvalError(msg) from exc

    # --- safe function calls ---

    def visit_Call(self, node: ast.Call) -> Any:
        if not isinstance(node.func, ast.Name):
            msg = "Only simple function calls allowed (no methods)"
            raise SafeEvalError(msg)
        fn_name = node.func.id
        fn = _SAFE_CALLS.get(fn_name)
        if fn is None:
            msg = f"Function not allowed: {fn_name}"
            raise SafeEvalError(msg)
        args = [self.visit(a) for a in node.args]
        kwargs = {kw.arg: self.visit(kw.value) for kw in node.keywords}
        result = fn(*args, **kwargs)
        if isinstance(result, str):
            _check_str(result)
        return result

    # --- f-strings ---

    def visit_JoinedStr(self, node: ast.JoinedStr) -> str:
        parts: list[str] = []
        for val in node.values:
            parts.append(str(self.visit(val)))
        result = "".join(parts)
        _check_str(result)
        return result

    def visit_FormattedValue(self, node: ast.FormattedValue) -> str:
        val = self.visit(node.value)
        if node.format_spec:
            spec = self.visit(node.format_spec)
            return format(val, spec)
        return str(val)

    # --- expression wrapper ---

    def visit_Expression(self, node: ast.Expression) -> Any:
        return self.visit(node.body)


# ------------------------------------------------------------------
# Guard helpers
# ------------------------------------------------------------------


def _check_num(value: int | float) -> None:
    if isinstance(value, float) and (
        value != value or value == float("inf") or value == float("-inf")
    ):
        msg = "Result is NaN or Infinity"
        raise SafeEvalError(msg)
    if abs(value) > MAX_NUM:
        msg = f"Numeric result exceeds limit of {MAX_NUM}"
        raise SafeEvalError(msg)


def _check_str(value: str) -> None:
    if len(value) > MAX_RESULT_LEN:
        msg = f"String length {len(value)} exceeds limit of {MAX_RESULT_LEN}"
        raise SafeEvalError(msg)


def _check_len(seq: list | tuple) -> None:  # type: ignore[type-arg]
    if len(seq) > MAX_REPEAT:
        msg = f"Sequence length {len(seq)} exceeds limit of {MAX_REPEAT}"
        raise SafeEvalError(msg)


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------


def safe_eval(expr: str, variables: dict[str, Any] | None = None) -> Any:
    """Evaluate a single expression safely.

    Only a restricted subset of Python is allowed:
    arithmetic, comparisons, boolean logic, ternary, subscript,
    and whitelisted functions (str, int, float, len, abs, min, max,
    round, bool).

    Raises ``SafeEvalError`` on disallowed constructs or resource
    limit violations.
    """
    if len(expr) > MAX_EXPR_LEN:
        msg = f"Expression too long ({len(expr)} > {MAX_EXPR_LEN})"
        raise SafeEvalError(msg)

    try:
        tree = ast.parse(expr.strip(), mode="eval")
    except SyntaxError as exc:
        msg = f"Invalid expression: {exc.msg}"
        raise SafeEvalError(msg) from exc

    return _Evaluator(variables or {}).visit(tree)


# Template rendering — {expr} placeholders
_TEMPLATE_RE = re.compile(
    r"\{\{|"       # escaped brace → literal {
    r"\}\}|"       # escaped brace → literal }
    r"\{([^}]+)\}"  # expression
)


def render_template(
    template: str,
    variables: dict[str, Any] | None = None,
) -> str:
    """Render a template string with ``{expression}`` placeholders.

    Use ``{{`` and ``}}`` for literal braces.

    Each ``{…}`` placeholder is evaluated via ``safe_eval`` with the
    given variables.

    Raises ``SafeEvalError`` if any expression fails or the result
    is too large.
    """
    if len(template) > MAX_TEMPLATE_LEN:
        msg = f"Template too long ({len(template)} > {MAX_TEMPLATE_LEN})"
        raise SafeEvalError(msg)

    vars_ = variables or {}
    expr_count = 0

    def _replace(match: re.Match) -> str:  # type: ignore[type-arg]
        nonlocal expr_count
        full = match.group(0)
        if full == "{{":
            return "{"
        if full == "}}":
            return "}"
        expr_count += 1
        if expr_count > MAX_TEMPLATE_EXPRESSIONS:
            msg = (
                f"Too many expressions in template "
                f"(> {MAX_TEMPLATE_EXPRESSIONS})"
            )
            raise SafeEvalError(msg)
        expr = match.group(1)
        return str(safe_eval(expr, vars_))

    result = _TEMPLATE_RE.sub(_replace, template)
    _check_str(result)
    return result
